package meowcaller

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestRelayConsentTransactions(t *testing.T) {
	var s relayConsentTransactions
	now := time.Unix(100, 0)
	first := [12]byte{1}
	if s.consume(first, now) {
		t.Fatal("unsolicited pong")
	}
	s.remember(first, now)
	if s.consume([12]byte{2}, now) || s.consume(first, now.Add(-time.Second)) {
		t.Fatal("mismatched pong")
	}
	if !s.consume(first, now.Add(time.Second)) || s.consume(first, now.Add(time.Second)) {
		t.Fatal("missing or duplicate pong")
	}
	s.remember(first, now)
	if s.consume(first, now.Add(relayConsentWindow)) {
		t.Fatal("expired pong")
	}
	for i := 0; i < relayConsentCapacity; i++ {
		s.remember([12]byte{byte(i + 2)}, now)
	}
	if s.consume(first, now) || !s.consume([12]byte{2}, now) {
		t.Fatal("bounded storage failed")
	}
}

func TestRelayConsentConcurrentConsumption(t *testing.T) {
	var s relayConsentTransactions
	now := time.Unix(100, 0)
	id := [12]byte{7}
	s.remember(id, now)
	var accepted atomic.Int32
	var wg sync.WaitGroup
	for i := 0; i < 32; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if s.consume(id, now) {
				accepted.Add(1)
			}
		}()
	}
	wg.Wait()
	if accepted.Load() != 1 {
		t.Fatalf("accepted %d replies", accepted.Load())
	}
}
