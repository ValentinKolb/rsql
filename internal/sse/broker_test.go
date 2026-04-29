package sse

import (
	"testing"
	"time"

	"github.com/ValentinKolb/rsql/internal/domain"
)

func TestBrokerSubscribePublishUnsubscribeAndEncode(t *testing.T) {
	b := NewBroker()
	id, ch := b.Subscribe("ns", []string{"kunden"})

	b.Publish("ns", domain.SSEEvent{Table: "orders", Action: "update"})
	select {
	case <-ch:
		t.Fatal("did not expect event for non-subscribed table")
	case <-time.After(50 * time.Millisecond):
	}

	expected := domain.SSEEvent{Table: "kunden", Action: "update"}
	b.Publish("ns", expected)
	select {
	case got := <-ch:
		if got.Table != expected.Table || got.Action != expected.Action {
			t.Fatalf("unexpected event: %#v", got)
		}
	case <-time.After(time.Second):
		t.Fatal("timeout waiting event")
	}

	if _, err := Encode(expected); err != nil {
		t.Fatalf("encode event: %v", err)
	}

	b.Unsubscribe("ns", id)
	b.Unsubscribe("ns", id)
}
