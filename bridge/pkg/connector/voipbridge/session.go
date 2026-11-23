// Package voipbridge tracks call lifecycles without depending on either media stack.
// It is intentionally transport-neutral: no media is connected by this package.
package voipbridge

import (
	"errors"
	"sync"
	"time"
)

var (
	ErrInvalid    = errors.New("voipbridge: invalid call")
	ErrConflict   = errors.New("voipbridge: call already exists with different metadata")
	ErrNotFound   = errors.New("voipbridge: call not found")
	ErrTransition = errors.New("voipbridge: invalid call state transition")
)

type Direction string

const (
	Incoming Direction = "incoming"
	Outgoing Direction = "outgoing"
)

type State string

const (
	Ringing    State = "ringing"
	Connecting State = "connecting"
	Active     State = "active"
	Ended      State = "ended"
)

type ID struct {
	LoginID string
	CallID  string
}

type Session struct {
	ID        ID
	ChatJID   string
	Direction Direction
	State     State
	CreatedAt time.Time
	UpdatedAt time.Time
	EndReason string
}

type Manager struct {
	mu       sync.RWMutex
	sessions map[ID]Session
	now      func() time.Time
}

func NewManager() *Manager {
	return &Manager{sessions: make(map[ID]Session), now: time.Now}
}

// Start is idempotent for exact duplicates but never revives an ended call.
// LoginID is part of the key so calls from multiple WhatsApp logins cannot collide.
func (m *Manager) Start(id ID, chatJID string, direction Direction) (Session, error) {
	if id.LoginID == "" || id.CallID == "" || chatJID == "" || (direction != Incoming && direction != Outgoing) {
		return Session{}, ErrInvalid
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if existing, ok := m.sessions[id]; ok {
		if existing.ChatJID != chatJID || existing.Direction != direction {
			return Session{}, ErrConflict
		}
		return existing, nil
	}
	now := m.now()
	s := Session{ID: id, ChatJID: chatJID, Direction: direction, State: Ringing, CreatedAt: now, UpdatedAt: now}
	m.sessions[id] = s
	return s, nil
}

func canMove(from, to State) bool {
	if from == to {
		return true
	}
	switch from {
	case Ringing:
		return to == Connecting || to == Ended
	case Connecting:
		return to == Active || to == Ended
	case Active:
		return to == Ended
	default:
		return false
	}
}

// Transition is safe to call from concurrent WhatsApp and Matrix event handlers.
func (m *Manager) Transition(id ID, next State) (Session, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	s, ok := m.sessions[id]
	if !ok {
		return Session{}, ErrNotFound
	}
	if !canMove(s.State, next) || next == "" {
		return s, ErrTransition
	}
	if s.State != next {
		s.State = next
		s.UpdatedAt = m.now()
		m.sessions[id] = s
	}
	return s, nil
}

func (m *Manager) End(id ID, reason string) (Session, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	s, ok := m.sessions[id]
	if !ok {
		return Session{}, ErrNotFound
	}
	if s.State == Ended {
		// The phase transition may mark the session ended just before meowcaller
		// reports the precise reason (for example server:406). Preserve that
		// later, more useful reason instead of leaving a generic placeholder.
		if reason != "" && reason != s.EndReason {
			s.EndReason = reason
			s.UpdatedAt = m.now()
			m.sessions[id] = s
		}
		return s, nil
	}
	s.State = Ended
	s.EndReason = reason
	s.UpdatedAt = m.now()
	m.sessions[id] = s
	return s, nil
}

func (m *Manager) Get(id ID) (Session, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	s, ok := m.sessions[id]
	return s, ok
}

// EndLogin terminates all active calls when a WhatsApp login disconnects.
func (m *Manager) EndLogin(loginID, reason string) []Session {
	m.mu.Lock()
	defer m.mu.Unlock()
	ended := []Session{}
	for id, s := range m.sessions {
		if id.LoginID == loginID && s.State != Ended {
			s.State = Ended
			s.EndReason = reason
			s.UpdatedAt = m.now()
			m.sessions[id] = s
			ended = append(ended, s)
		}
	}
	return ended
}

// PruneEnded bounds retention while leaving active calls untouched.
func (m *Manager) PruneEnded(maxAge time.Duration) int {
	if maxAge < 0 {
		return 0
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	cutoff := m.now().Add(-maxAge)
	removed := 0
	for id, s := range m.sessions {
		if s.State == Ended && !s.UpdatedAt.After(cutoff) {
			delete(m.sessions, id)
			removed++
		}
	}
	return removed
}
