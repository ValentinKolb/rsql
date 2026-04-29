package namespace

import (
	"database/sql"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	_ "modernc.org/sqlite"
)

// Config configures namespace manager lifecycle behavior.
type Config struct {
	IdleTimeout time.Duration
}

type handle struct {
	db       *sql.DB
	rw       sync.RWMutex
	lastUsed int64
	closed   atomic.Bool
}

// Manager manages namespace DB handles and per-namespace locks.
type Manager struct {
	mu      sync.Mutex
	handles map[string]*handle
	cfg     Config
	closed  chan struct{}
}

// NewManager creates a namespace manager.
func NewManager(cfg Config) *Manager {
	m := &Manager{
		handles: make(map[string]*handle),
		cfg:     cfg,
		closed:  make(chan struct{}),
	}
	go m.reaper()
	return m
}

func (m *Manager) reaper() {
	if m.cfg.IdleTimeout <= 0 {
		return
	}
	ticker := time.NewTicker(minDuration(m.cfg.IdleTimeout/2, 30*time.Second))
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			m.collectIdle()
		case <-m.closed:
			return
		}
	}
}

func minDuration(a, b time.Duration) time.Duration {
	if a <= 0 {
		return b
	}
	if a < b {
		return a
	}
	return b
}

func (m *Manager) collectIdle() {
	if m.cfg.IdleTimeout <= 0 {
		return
	}
	cutoff := time.Now().Add(-m.cfg.IdleTimeout)

	// Claim phase: under m.mu, identify idle handles and remove them from
	// the map. We hold each victim's write lock past this phase so any
	// concurrent reader/writer that already obtained the *handle pointer
	// will block until the close phase finishes — and then observe the
	// `closed` flag and refuse to use the stale DB.
	type victim struct {
		name string
		h    *handle
	}
	var victims []victim

	m.mu.Lock()
	for name, h := range m.handles {
		// lastUsed is atomic; no per-handle lock needed for the heuristic.
		if !h.lastUsedAt().Before(cutoff) {
			continue
		}
		// Try to claim exclusively. If a request is currently using this
		// handle, skip it and revisit on the next tick.
		if !h.rw.TryLock() {
			continue
		}
		// Re-check after locking — a request may have touched the handle
		// between the atomic read and the TryLock.
		if !h.lastUsedAt().Before(cutoff) {
			h.rw.Unlock()
			continue
		}
		h.closed.Store(true)
		delete(m.handles, name)
		victims = append(victims, victim{name, h})
		// Keep h.rw locked; the close phase below releases it.
	}
	m.mu.Unlock()

	// Close phase: outside m.mu so getOrOpen calls for unrelated namespaces
	// stay responsive even if Close() takes time draining connections.
	for _, v := range victims {
		_ = v.h.db.Close()
		v.h.rw.Unlock()
	}
}

// Close closes all open namespace handles.
func (m *Manager) Close() error {
	close(m.closed)

	m.mu.Lock()
	defer m.mu.Unlock()

	for name, h := range m.handles {
		h.rw.Lock()
		h.closed.Store(true)
		_ = h.db.Close()
		h.rw.Unlock()
		delete(m.handles, name)
	}
	return nil
}

func (m *Manager) getOrOpen(name, path string) (*handle, error) {
	m.mu.Lock()
	if h, ok := m.handles[name]; ok {
		h.touch()
		m.mu.Unlock()
		return h, nil
	}
	m.mu.Unlock()

	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, fmt.Errorf("open namespace db: %w", err)
	}

	h := &handle{db: db}
	h.touch()

	m.mu.Lock()
	if existing, ok := m.handles[name]; ok {
		m.mu.Unlock()
		_ = db.Close()
		existing.touch()
		return existing, nil
	}
	m.handles[name] = h
	m.mu.Unlock()

	return h, nil
}

// WithRead executes fn under namespace read lock. If the handle was evicted
// by the reaper between getOrOpen and lock acquisition, it retries with a
// freshly opened handle.
func (m *Manager) WithRead(name, path string, fn func(*sql.DB) error) error {
	for attempt := 0; attempt < 3; attempt++ {
		h, err := m.getOrOpen(name, path)
		if err != nil {
			return err
		}
		h.rw.RLock()
		if h.closed.Load() {
			h.rw.RUnlock()
			continue
		}
		h.touch()
		err = fn(h.db)
		h.rw.RUnlock()
		return err
	}
	return fmt.Errorf("namespace handle repeatedly evicted")
}

// WithWrite executes fn under namespace write lock. If the handle was evicted
// by the reaper between getOrOpen and lock acquisition, it retries with a
// freshly opened handle.
func (m *Manager) WithWrite(name, path string, fn func(*sql.DB) error) error {
	for attempt := 0; attempt < 3; attempt++ {
		h, err := m.getOrOpen(name, path)
		if err != nil {
			return err
		}
		h.rw.Lock()
		if h.closed.Load() {
			h.rw.Unlock()
			continue
		}
		h.touch()
		err = fn(h.db)
		h.rw.Unlock()
		return err
	}
	return fmt.Errorf("namespace handle repeatedly evicted")
}

// CloseHandle closes and removes a namespace handle from the manager.
func (m *Manager) CloseHandle(name string) error {
	m.mu.Lock()
	h, ok := m.handles[name]
	if ok {
		delete(m.handles, name)
	}
	m.mu.Unlock()
	if !ok {
		return nil
	}
	h.rw.Lock()
	defer h.rw.Unlock()
	h.closed.Store(true)
	return h.db.Close()
}

func (h *handle) touch() {
	atomic.StoreInt64(&h.lastUsed, time.Now().UnixNano())
}

func (h *handle) lastUsedAt() time.Time {
	n := atomic.LoadInt64(&h.lastUsed)
	if n == 0 {
		return time.Time{}
	}
	return time.Unix(0, n)
}
