package namespace

import (
	"database/sql"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestMinDuration(t *testing.T) {
	if got := minDuration(0, 5*time.Second); got != 5*time.Second {
		t.Fatalf("unexpected minDuration zero: %v", got)
	}
	if got := minDuration(3*time.Second, 5*time.Second); got != 3*time.Second {
		t.Fatalf("unexpected minDuration lower: %v", got)
	}
	if got := minDuration(7*time.Second, 5*time.Second); got != 5*time.Second {
		t.Fatalf("unexpected minDuration upper: %v", got)
	}
}

func TestManagerReadWriteAndIdleCollect(t *testing.T) {
	m := NewManager(Config{IdleTimeout: 20 * time.Millisecond})
	defer m.Close()

	dbPath := filepath.Join(t.TempDir(), "ns.db")
	if err := m.WithWrite("n1", dbPath, func(db *sql.DB) error {
		_, err := db.Exec(`CREATE TABLE IF NOT EXISTS t (id INTEGER PRIMARY KEY, v TEXT)`)
		if err != nil {
			return err
		}
		_, err = db.Exec(`INSERT INTO t(v) VALUES ('x')`)
		return err
	}); err != nil {
		t.Fatalf("with write: %v", err)
	}

	if err := m.WithRead("n1", dbPath, func(db *sql.DB) error {
		var c int
		return db.QueryRow(`SELECT COUNT(*) FROM t`).Scan(&c)
	}); err != nil {
		t.Fatalf("with read: %v", err)
	}

	// force idle collection
	time.Sleep(60 * time.Millisecond)
	m.collectIdle()

	// handle should reopen cleanly
	if err := m.WithRead("n1", dbPath, func(db *sql.DB) error {
		var c int
		return db.QueryRow(`SELECT COUNT(*) FROM t`).Scan(&c)
	}); err != nil {
		t.Fatalf("with read after collect: %v", err)
	}

	if err := m.CloseHandle("n1"); err != nil {
		t.Fatalf("close handle: %v", err)
	}
	if err := m.CloseHandle("n1"); err != nil {
		t.Fatalf("close missing handle: %v", err)
	}
}

func TestCollectIdleSkipsBusyNamespaceWithoutBlocking(t *testing.T) {
	m := NewManager(Config{IdleTimeout: 10 * time.Millisecond})
	defer m.Close()

	dbPath := filepath.Join(t.TempDir(), "busy.db")
	if err := m.WithWrite("busy", dbPath, func(db *sql.DB) error {
		_, err := db.Exec(`CREATE TABLE IF NOT EXISTS t (id INTEGER PRIMARY KEY, v TEXT)`)
		return err
	}); err != nil {
		t.Fatalf("seed namespace: %v", err)
	}

	started := make(chan struct{})
	done := make(chan error, 1)
	go func() {
		done <- m.WithWrite("busy", dbPath, func(_ *sql.DB) error {
			close(started)
			time.Sleep(80 * time.Millisecond)
			return nil
		})
	}()

	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("writer did not start")
	}

	begin := time.Now()
	m.collectIdle()
	if took := time.Since(begin); took > 30*time.Millisecond {
		t.Fatalf("collectIdle blocked on busy namespace: %v", took)
	}

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("busy writer returned error: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("busy writer did not finish")
	}
}

// TestCollectIdleDoesNotBlockOtherNamespaces guards against the head-of-line
// blocking pattern where collectIdle holds the manager mutex across a slow
// db.Close() and stalls getOrOpen for unrelated namespaces.
func TestCollectIdleDoesNotBlockOtherNamespaces(t *testing.T) {
	m := NewManager(Config{IdleTimeout: 10 * time.Millisecond})
	defer m.Close()

	idlePath := filepath.Join(t.TempDir(), "idle.db")
	if err := m.WithWrite("idle", idlePath, func(db *sql.DB) error {
		_, err := db.Exec(`CREATE TABLE IF NOT EXISTS t (id INTEGER PRIMARY KEY)`)
		return err
	}); err != nil {
		t.Fatalf("seed idle namespace: %v", err)
	}

	// Mark "idle" as past the cutoff so collectIdle will evict it.
	time.Sleep(30 * time.Millisecond)

	reaperDone := make(chan struct{})
	go func() {
		m.collectIdle()
		close(reaperDone)
	}()

	// While the reaper runs, opening a *different* namespace must succeed
	// quickly. Even if the reaper's db.Close() is slow, m.mu must be free.
	otherPath := filepath.Join(t.TempDir(), "other.db")
	begin := time.Now()
	err := m.WithWrite("other", otherPath, func(db *sql.DB) error {
		_, err := db.Exec(`CREATE TABLE IF NOT EXISTS t (id INTEGER PRIMARY KEY)`)
		return err
	})
	took := time.Since(begin)
	if err != nil {
		t.Fatalf("other namespace open: %v", err)
	}
	if took > 100*time.Millisecond {
		t.Fatalf("other namespace open blocked by reaper: %v", took)
	}
	<-reaperDone
}

// TestEvictionDuringConcurrentReadIsSafe stresses the eviction path against
// concurrent readers/writers. Without the `closed` flag and the WithRead
// retry, an in-flight goroutine could end up using a closed *sql.DB.
func TestEvictionDuringConcurrentReadIsSafe(t *testing.T) {
	m := NewManager(Config{IdleTimeout: 5 * time.Millisecond})
	defer m.Close()

	dbPath := filepath.Join(t.TempDir(), "stress.db")
	if err := m.WithWrite("stress", dbPath, func(db *sql.DB) error {
		_, err := db.Exec(`CREATE TABLE IF NOT EXISTS t (id INTEGER PRIMARY KEY, v INTEGER)`)
		if err != nil {
			return err
		}
		_, err = db.Exec(`INSERT INTO t(v) VALUES (1)`)
		return err
	}); err != nil {
		t.Fatalf("seed: %v", err)
	}

	stop := make(chan struct{})
	var failures atomic.Int64
	var ops atomic.Int64
	var wg sync.WaitGroup

	// Several readers/writers continuously hammer the namespace.
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				err := m.WithRead("stress", dbPath, func(db *sql.DB) error {
					var n int
					return db.QueryRow(`SELECT COUNT(*) FROM t`).Scan(&n)
				})
				ops.Add(1)
				if err != nil {
					failures.Add(1)
				}
			}
		}()
	}

	// Run reaper aggressively in parallel.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
			}
			m.collectIdle()
			time.Sleep(2 * time.Millisecond)
		}
	}()

	time.Sleep(200 * time.Millisecond)
	close(stop)
	wg.Wait()

	if ops.Load() == 0 {
		t.Fatal("no operations executed")
	}
	if failures.Load() > 0 {
		t.Fatalf("got %d failed reads under concurrent eviction (out of %d ops)", failures.Load(), ops.Load())
	}
}
