package sse

import (
	"encoding/json"
	"sync"

	"github.com/ValentinKolb/rsql/internal/domain"
)

type subscription struct {
	id     uint64
	tables map[string]struct{}
	ch     chan domain.SSEEvent
}

// Broker multiplexes namespace-scoped SSE events to subscribers.
type Broker struct {
	mu      sync.RWMutex
	nextID  uint64
	clients map[string]map[uint64]*subscription
}

// NewBroker creates an event broker.
func NewBroker() *Broker {
	return &Broker{clients: make(map[string]map[uint64]*subscription)}
}

// Subscribe registers a namespace subscription and returns its id and channel.
func (b *Broker) Subscribe(namespace string, tables []string) (uint64, <-chan domain.SSEEvent) {
	tableSet := make(map[string]struct{}, len(tables))
	for _, t := range tables {
		if t != "" {
			tableSet[t] = struct{}{}
		}
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	b.nextID++
	id := b.nextID
	if _, ok := b.clients[namespace]; !ok {
		b.clients[namespace] = make(map[uint64]*subscription)
	}
	b.clients[namespace][id] = &subscription{id: id, tables: tableSet, ch: make(chan domain.SSEEvent, 128)}
	return id, b.clients[namespace][id].ch
}

// Unsubscribe removes a subscription.
func (b *Broker) Unsubscribe(namespace string, id uint64) {
	b.mu.Lock()
	defer b.mu.Unlock()

	ns, ok := b.clients[namespace]
	if !ok {
		return
	}
	sub, ok := ns[id]
	if !ok {
		return
	}
	delete(ns, id)
	close(sub.ch)
	if len(ns) == 0 {
		delete(b.clients, namespace)
	}
}

// Publish sends an event to matching subscribers.
func (b *Broker) Publish(namespace string, event domain.SSEEvent) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	ns, ok := b.clients[namespace]
	if !ok {
		return
	}

	for _, sub := range ns {
		if len(sub.tables) > 0 {
			if _, ok := sub.tables[event.Table]; !ok {
				continue
			}
		}
		select {
		case sub.ch <- event:
		default:
		}
	}
}

// Encode formats a SSE event payload as JSON bytes.
func Encode(event domain.SSEEvent) ([]byte, error) {
	return json.Marshal(event)
}
