package voipbridge

import (
	"context"
	"errors"
	"io"

	"go.mau.fi/mautrix-whatsapp/pkg/connector/voipbridge/rtcwire"
)

// ParticipantTransport is for a negotiated independent group-media IPC session.
// The owner must run Watch and cancel/close the session on a transport error.
// It must not be constructed for the legacy aggregate worker session.
type ParticipantTransport struct {
	bus             *MediaBus
	peer            *rtcwire.Peer
	callID          string
	gate            chan struct{}
	active          uint64
	highest         uint64
	revision        uint64
	failed          error
	scopeGeneration uint64
	scopeRevision   uint64
	scopeRetired    bool
}

func NewParticipantTransport(bus *MediaBus, peer *rtcwire.Peer, callID string) (*ParticipantTransport, error) {
	if bus == nil || peer == nil || callID == "" || len(callID) > 256 {
		return nil, ErrBadFrame
	}
	return &ParticipantTransport{bus: bus, peer: peer, callID: callID, gate: make(chan struct{}, 1)}, nil
}

type participantEpochSnapshot struct {
	generation uint64
	revision   uint64
	changed    <-chan struct{}
	closed     bool
}

func (m *MediaBus) participantEpochSnapshot() participantEpochSnapshot {
	m.admissionMu.RLock()
	defer m.admissionMu.RUnlock()
	s := participantEpochSnapshot{revision: m.participantEpochRevision, changed: m.participantEpochChanged}
	select {
	case <-m.closed:
		s.closed = true
		return s
	default:
	}
	if m.participantEpochValid && m.participantEpochRoster == m.participantRosterTransaction && m.participantRoster != nil {
		s.generation = uint64(m.participantEpochRoster)<<32 | uint64(m.participantKeyTransaction)
	}
	return s
}

// NewScopedParticipantTransport cannot open a later roster/key epoch. A
// new independent admission is required even when the same devices return.
func NewScopedParticipantTransport(bus *MediaBus, peer *rtcwire.Peer, callID string, generation, revision uint64) (*ParticipantTransport, error) {
	t, err := NewParticipantTransport(bus, peer, callID)
	if err != nil {
		return nil, err
	}
	if !bus.ParticipantRosterCurrent(generation, revision) {
		return nil, ErrParticipantEpochRetired
	}
	t.scopeGeneration, t.scopeRevision = generation, revision
	return t, nil
}

func (t *ParticipantTransport) lock(ctx context.Context) error {
	select {
	case t.gate <- struct{}{}:
	case <-ctx.Done():
		return ctx.Err()
	}
	if err := ctx.Err(); err != nil {
		<-t.gate
		return err
	}
	return nil
}
func (t *ParticipantTransport) unlock() { <-t.gate }

// syncLocked serializes epoch barriers and media writes without holding the
// provider/bus locks during I/O. A retired generation cannot be reopened on
// this connection, even if an invalidation and reactivation were coalesced.
func (t *ParticipantTransport) syncLocked(ctx context.Context, s participantEpochSnapshot) error {
	if t.failed != nil {
		return t.failed
	}
	if t.active != 0 && (t.active != s.generation || t.revision != s.revision) {
		if err := t.sendEpoch(ctx, t.active, false); err != nil {
			return err
		}
		if t.active == t.scopeGeneration {
			t.scopeRetired = true
		}
		t.active = 0
	}
	if t.scopeGeneration != 0 && (t.scopeRetired || s.closed || s.generation != t.scopeGeneration || s.revision != t.scopeRevision) {
		return ErrParticipantEpochRetired
	}
	if !s.closed && s.generation > t.highest {
		if err := t.sendEpoch(ctx, s.generation, true); err != nil {
			return err
		}
		t.active = s.generation
		t.highest = s.generation
		t.revision = s.revision
	}
	return nil
}
func (t *ParticipantTransport) sendEpoch(ctx context.Context, generation uint64, active bool) error {
	msg := rtcwire.ParticipantEpochFrame(t.callID, generation, active)
	if err := t.peer.SendContext(ctx, msg); err != nil {
		t.failed = err
		return err
	}
	return nil
}

// Watch sends retirement even in the absence of further media packets. A
// complete frame already being written can precede the retirement barrier.
func (t *ParticipantTransport) Watch(ctx context.Context) error {
	for {
		if err := t.lock(ctx); err != nil {
			return err
		}
		s := t.bus.participantEpochSnapshot()
		err := t.syncLocked(ctx, s)
		t.unlock()
		if err != nil {
			return err
		}
		if s.closed {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-s.changed:
		}
	}
}

// RetireScope emits the terminal barrier even if the epoch changed before the
// sender could open it. A cancelled or uncertain socket write stays terminal.
func (t *ParticipantTransport) RetireScope(ctx context.Context) error {
	if err := t.lock(ctx); err != nil {
		return err
	}
	defer t.unlock()
	if t.failed != nil {
		return t.failed
	}
	if t.scopeRetired {
		return nil
	}
	if t.scopeGeneration == 0 {
		return ErrBadFrame
	}
	if err := t.sendEpoch(ctx, t.scopeGeneration, false); err != nil {
		return err
	}
	t.active = 0
	t.highest = t.scopeGeneration
	t.scopeRetired = true
	return nil
}

var ErrParticipantEpochRetired = errors.New("participant media epoch retired")

// Send is called by the owner of an admitted subscription after Next. A
// transport error is terminal; a retired track can simply be unsubscribed.
func (t *ParticipantTransport) Send(ctx context.Context, key ParticipantMediaKey, track *ParticipantMediaTrack, packet ParticipantMediaPacket) error {
	if track == nil {
		return ErrBadFrame
	}
	frame, err := ParticipantWireFrame(t.callID, key, track.ParticipantRosterGeneration(), packet)
	if err != nil {
		return err
	}
	if err = t.lock(ctx); err != nil {
		return err
	}
	defer t.unlock()
	s := t.bus.participantEpochSnapshot()
	if err = t.syncLocked(ctx, s); err != nil {
		return err
	}
	if s.closed {
		return io.EOF
	}
	if t.active == 0 || frame.Generation != t.active || !t.bus.ParticipantMediaCurrent(key, track) {
		return ErrParticipantEpochRetired
	}
	if err = t.peer.SendContext(ctx, frame); err != nil {
		// Conservatively fail the independent session after an uncertain write.
		t.failed = err
		return err
	}
	return nil
}

// SendState shares the same ordered epoch gate as media. Once the epoch
// retires, a delayed observation can never recreate the retired publisher.
func (t *ParticipantTransport) SendState(ctx context.Context, binding rtcwire.ParticipantBinding, state int, roleRevision uint64) error {
	if err := t.lock(ctx); err != nil {
		return err
	}
	defer t.unlock()
	snapshot := t.bus.participantEpochSnapshot()
	if err := t.syncLocked(ctx, snapshot); err != nil {
		return err
	}
	if t.active == 0 || t.active != t.scopeGeneration || !t.bus.ParticipantRosterCurrent(t.active, t.scopeRevision) {
		return ErrParticipantEpochRetired
	}
	msg := rtcwire.Message{Type: "whatsapp_participant_state", CallID: t.callID, Group: true, Generation: t.active, State: state, Timestamp: roleRevision, ParticipantBindings: []rtcwire.ParticipantBinding{binding}}
	if !rtcwire.ValidParticipantState(msg) {
		return ErrBadFrame
	}
	if err := t.peer.SendContext(ctx, msg); err != nil {
		t.failed = err
		return err
	}
	return nil
}
