package voipbridge

import "sort"

// ParticipantRosterDevice binds a provider-authenticated device to the exact
// participant ID and PID from an authoritative roster transaction.
type ParticipantRosterDevice struct {
	Device        string
	ParticipantID string
	PID           uint32
}

// ReconcileParticipantRoster revokes every old subscription on a newer roster
// transaction. Equal or older snapshots cannot resurrect a removed device.
// Transaction zero (the outgoing target seed) is never authoritative.
func (m *MediaBus) ReconcileParticipantRoster(transaction uint32, devices []ParticipantRosterDevice) {
	m.admissionMu.Lock()
	defer m.admissionMu.Unlock()
	if transaction == 0 || transaction <= m.participantRosterTransaction {
		return
	}
	select {
	case <-m.closed:
		return
	default:
	}
	m.participantRosterTransaction = transaction
	m.revokeParticipantMediaLocked()
	m.participantRoster = nil
	if len(devices) > 256 {
		return
	}
	roster := make(map[string]ParticipantRosterDevice, len(devices))
	ids := make(map[string]bool, len(devices))
	for _, device := range devices {
		if device.Device == "" || len(device.Device) > 512 || device.ParticipantID == "" || len(device.ParticipantID) > 512 || ids[device.ParticipantID] {
			return
		}
		if _, exists := roster[device.Device]; exists {
			return
		}
		roster[device.Device] = device
		ids[device.ParticipantID] = true
	}
	m.participantRoster = roster
}

// ReconcileParticipantEpoch is driven solely by the provider registry commit,
// never by incoming packets. Roster and key snapshots may arrive in either
// order: admission remains closed until the exact committed pair is present.
func (m *MediaBus) ReconcileParticipantEpoch(roster, key uint32, valid bool) {
	m.admissionMu.Lock()
	defer m.admissionMu.Unlock()
	select {
	case <-m.closed:
		return
	default:
	}
	valid = valid && roster != 0 && key != 0 && key <= roster
	if roster < m.participantEpochRoster || (roster == m.participantEpochRoster && key < m.participantKeyTransaction) {
		return
	}
	if m.participantEpochRoster == roster && m.participantKeyTransaction == key && m.participantEpochValid == valid {
		return
	}
	m.participantEpochRoster = roster
	m.participantKeyTransaction = key
	m.participantEpochValid = valid
	m.participantObservedRoster = 0
	m.revokeParticipantMediaLocked()
}

// ParticipantMediaCurrent checks a dequeued subscription before IPC handoff.
// It is a local snapshot, not an atomic fence over external socket writes.
func (m *MediaBus) ParticipantMediaCurrent(key ParticipantMediaKey, track *ParticipantMediaTrack) bool {
	m.admissionMu.RLock()
	defer m.admissionMu.RUnlock()
	return track != nil && m.participantEpochValid && m.participantMedia[key] == track && m.participantEpochRoster == m.participantRosterTransaction && track.generation == uint64(m.participantEpochRoster)<<32|uint64(m.participantKeyTransaction)
}

func (m *MediaBus) revokeParticipantMediaLocked() {
	m.participantEpochRevision++
	m.participantSources = nil
	m.notifyParticipantSourcesLocked()
	if m.participantEpochChanged != nil {
		close(m.participantEpochChanged)
	}
	m.participantEpochChanged = make(chan struct{})
	for key, track := range m.participantMedia {
		delete(m.participantMedia, key)
		close(track.done)
		drainMediaQueue(track.queue)
	}
}

// ParticipantRosterGeneration packs the authoritative roster transaction and
// observed installed key transaction. No key material is exposed.
func (t *ParticipantMediaTrack) ParticipantRosterGeneration() uint64 { return t.generation }

// NeedsParticipantRoster avoids rebuilding the same roster on every media frame.
func (m *MediaBus) NeedsParticipantRoster(transaction uint32) bool {
	m.admissionMu.RLock()
	defer m.admissionMu.RUnlock()
	return transaction > m.participantRosterTransaction
}

// ParticipantRosterSnapshot returns a copy of the entire committed roster,
// including devices that have not yet produced media. The revision is local
// to this bus and must also be checked after asynchronous preparation.
func (m *MediaBus) ParticipantRosterSnapshot() (generation, revision uint64, devices []ParticipantRosterDevice, changed <-chan struct{}, ok bool) {
	m.admissionMu.RLock()
	defer m.admissionMu.RUnlock()
	select {
	case <-m.closed:
		return 0, 0, nil, nil, false
	default:
	}
	changed = m.participantEpochChanged
	if !m.participantEpochValid || m.participantEpochRoster != m.participantRosterTransaction || len(m.participantRoster) == 0 {
		return 0, m.participantEpochRevision, nil, changed, false
	}
	for _, device := range m.participantRoster {
		devices = append(devices, device)
	}
	sort.Slice(devices, func(i, j int) bool { return devices[i].Device < devices[j].Device })
	return uint64(m.participantEpochRoster)<<32 | uint64(m.participantKeyTransaction), m.participantEpochRevision, devices, changed, true
}

// ParticipantRosterCurrent is a local fence for a previously captured roster.
// It is not a fence over already submitted external IPC or Matrix requests.
func (m *MediaBus) ParticipantRosterCurrent(generation, revision uint64) bool {
	m.admissionMu.RLock()
	defer m.admissionMu.RUnlock()
	select {
	case <-m.closed:
		return false
	default:
	}
	return generation != 0 && m.participantEpochValid && m.participantEpochRoster == m.participantRosterTransaction && len(m.participantRoster) != 0 && revision == m.participantEpochRevision && generation == uint64(m.participantEpochRoster)<<32|uint64(m.participantKeyTransaction)
}
