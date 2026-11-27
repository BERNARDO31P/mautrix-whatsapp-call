package meowcaller

import (
	"sync"
	"time"
)

// A call-local bounded set of outstanding consent pings. Record before writing
// so an immediate response on the receive goroutine cannot beat registration.
const relayConsentWindow = 8 * time.Second
const relayConsentCapacity = 8

type relayConsentTransactions struct {
	mu      sync.Mutex
	pending [relayConsentCapacity]struct {
		id [12]byte
		at time.Time
	}
	next int
}

func (s *relayConsentTransactions) remember(id [12]byte, now time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.pending[s.next].id = id
	s.pending[s.next].at = now
	s.next = (s.next + 1) % len(s.pending)
}

func (s *relayConsentTransactions) consume(id [12]byte, now time.Time) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	for i := range s.pending {
		p := &s.pending[i]
		if !p.at.IsZero() && !now.Before(p.at) && now.Sub(p.at) < relayConsentWindow && p.id == id {
			p.at = time.Time{}
			return true
		}
	}
	return false
}
