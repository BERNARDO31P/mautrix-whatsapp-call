package voipbridge

import (
	"context"
	"errors"
	"io"
	"sync"
)

// Only authenticated, validated frames from the committed epoch announce a source.
// Announcements are metadata only; discovery does not retain unsubscribed PCM/video.
func (m *MediaBus) notifyParticipantSourcesLocked() {
	if m.participantSourcesChanged != nil {
		close(m.participantSourcesChanged)
	}
	m.participantSourcesChanged = make(chan struct{})
}

func (m *MediaBus) observeParticipantSourceLocked(key ParticipantMediaKey) bool {
	if _, ok := m.participantSources[key]; ok {
		return true
	}
	if len(m.participantSources) >= maxParticipantMediaTracks {
		return false
	}
	if m.participantSources == nil {
		m.participantSources = make(map[ParticipantMediaKey]struct{})
	}
	m.participantSources[key] = struct{}{}
	m.notifyParticipantSourcesLocked()
	return true
}

type participantSourcesSnapshot struct {
	generation uint64
	revision   uint64
	keys       []ParticipantMediaKey
	changed    <-chan struct{}
	closed     bool
}

func (m *MediaBus) participantSourcesSnapshot() participantSourcesSnapshot {
	m.admissionMu.RLock()
	defer m.admissionMu.RUnlock()
	s := participantSourcesSnapshot{revision: m.participantEpochRevision, changed: m.participantSourcesChanged}
	select {
	case <-m.closed:
		s.closed = true
		return s
	default:
	}
	if !m.participantEpochValid || m.participantEpochRoster != m.participantRosterTransaction {
		return s
	}
	s.generation = uint64(m.participantEpochRoster)<<32 | uint64(m.participantKeyTransaction)
	for key := range m.participantSources {
		s.keys = append(s.keys, key)
	}
	return s
}

// RunDiscovered manages source lifetimes for a separately negotiated group-media
// session. authorize must be a nonblocking, concurrency-safe check of the
// independently authorized Matrix publisher mapping, scoped to the epoch. It
// must remain valid for that epoch; revocation requires cancellation of ctx.
// The caller must not run Watch separately or use this on the aggregate peer.
// On return the owner must close the independent IPC session.
func (t *ParticipantTransport) RunDiscovered(parent context.Context, authorize func(ParticipantMediaKey, uint64) bool) error {
	if authorize == nil {
		return ErrBadFrame
	}
	ctx, cancel := context.WithCancel(parent)
	var tracks sync.WaitGroup
	var watcher sync.WaitGroup
	// Only one error is needed: any non-retirement failure ends the session.
	failures := make(chan error, 1)
	fail := func(err error) {
		select {
		case failures <- err:
		default:
		}
		cancel()
	}
	watcher.Add(1)
	go func() {
		defer watcher.Done()
		if err := t.Watch(ctx); err != nil && ctx.Err() == nil {
			fail(err)
		}
	}()
	defer func() { cancel(); tracks.Wait(); watcher.Wait() }()
	seen := make(map[ParticipantMediaKey]bool)
	var revision uint64
	for {
		if ctx.Err() != nil {
			select {
			case err := <-failures:
				return err
			default:
				return ctx.Err()
			}
		}
		s := t.bus.participantSourcesSnapshot()
		if s.closed {
			return nil
		}
		if s.revision != revision {
			// All old tracks have been revoked by the bus. Do not spawn a new
			// generation until old senders have exited (bounded socket writes).
			tracks.Wait()
			clear(seen)
			revision = s.revision
		}
		for _, key := range s.keys {
			if seen[key] || !authorize(key, s.generation) {
				continue
			}
			track, err := t.bus.subscribeParticipantMedia(key, s.generation, s.revision)
			if err != nil {
				continue
			} // a concurrent epoch change invalidated the snapshot
			seen[key] = true
			tracks.Add(1)
			go func(key ParticipantMediaKey, track *ParticipantMediaTrack) {
				defer tracks.Done()
				defer t.bus.UnsubscribeParticipantMedia(key, track)
				for {
					packet, err := track.Next(ctx)
					if err != nil {
						if !errors.Is(err, io.EOF) && ctx.Err() == nil {
							fail(err)
						}
						return
					}
					if !authorize(key, track.ParticipantRosterGeneration()) {
						if t.scopeGeneration != 0 && !t.bus.ParticipantRosterCurrent(t.scopeGeneration, t.scopeRevision) {
							fail(ErrParticipantEpochRetired)
						} else {
							fail(ErrBadFrame)
						}
						return
					}
					err = t.Send(ctx, key, track, packet)
					if err != nil {
						if !errors.Is(err, ErrParticipantEpochRetired) && !errors.Is(err, io.EOF) && ctx.Err() == nil {
							fail(err)
						}
						return
					}
				}
			}(key, track)
		}
		select {
		case <-ctx.Done():
		case <-s.changed:
		}
	}
}
