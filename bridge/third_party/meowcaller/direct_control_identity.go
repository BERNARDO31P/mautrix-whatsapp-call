package meowcaller

import (
	"time"

	"go.mau.fi/whatsmeow/types"
)

// A bare user JID may represent a device-qualified peer. Two different
// explicitly identified devices, however, are not interchangeable.
func sameDirectControlIdentity(expected, actual types.JID) bool {
	if expected.User == "" || actual.User == "" || expected.IsEmpty() || actual.IsEmpty() || expected.ToNonAD() != actual.ToNonAD() {
		return false
	}
	return expected.Device == 0 || actual.Device == 0 || expected == actual
}

func directControlMatches(peer, creator, sender, claimedCreator, participant types.JID) bool {
	return sameDirectControlIdentity(peer, sender) &&
		sameDirectControlIdentity(creator, claimedCreator) &&
		(participant.IsEmpty() || sameDirectControlIdentity(sender, participant) && sameDirectControlIdentity(peer, participant))
}

// Direct lifecycle events must identify the same account and creator; group
// events are handled by their own topology policy.
func directLifecycleMatches(peer, creator types.JID, meta types.BasicCallMeta) bool {
	return meta.GroupJID.IsEmpty() && directControlMatches(peer, creator, meta.From, meta.CallCreator, types.JID{})
}

// Before an offer exists, a remote termination owns only its signaling
// identity, not the entire call ID. An unrelated event must not reserve the
// ID of a later direct or group offer. Established local retirements use the
// separate ended registry.
type earlyTerminationIdentity struct {
	callID                 string
	sender, creator, group types.JID
}

func earlyTerminationKey(meta types.BasicCallMeta) (earlyTerminationIdentity, bool) {
	key := earlyTerminationIdentity{meta.CallID, meta.From, meta.CallCreator, meta.GroupJID}
	if key.callID == "" || key.sender.User == "" || key.sender.IsEmpty() || key.creator.User == "" || key.creator.IsEmpty() {
		return key, false
	}
	if key.sender.Server == "call" && key.sender.User != key.callID {
		return key, false
	}
	if !key.group.IsEmpty() && (key.group.Server != types.GroupServer || key.group.User == "") {
		return key, false
	}
	return key, true
}

// An explicit group route is never a direct-call reservation, even if the
// provider reuses an ID. For group offers, only the same creator and group
// (or exact call routing address) can reserve the ID.
func (key earlyTerminationIdentity) matchesOffer(meta types.BasicCallMeta, group bool) bool {
	if key.callID != meta.CallID || !sameDirectControlIdentity(key.creator, meta.CallCreator) {
		return false
	}
	routed := key.sender.Server == "call" && key.sender.User == key.callID
	if !group {
		return key.group.IsEmpty() && !routed && key.sender.Server != types.GroupServer && directLifecycleMatches(meta.From, meta.CallCreator, types.BasicCallMeta{From: key.sender, CallCreator: key.creator, GroupJID: key.group})
	}
	if !key.group.IsEmpty() && key.group != meta.GroupJID {
		return false
	}
	if routed {
		return true
	}
	if key.group.IsEmpty() {
		return false
	}
	return key.sender == key.group || sameDirectControlIdentity(meta.From, key.sender)
}

func rememberEarlyTermination(entries map[earlyTerminationIdentity]time.Time, meta types.BasicCallMeta, now time.Time) map[earlyTerminationIdentity]time.Time {
	key, ok := earlyTerminationKey(meta)
	if !ok {
		return entries
	}
	if now.Before(entries[key]) {
		return entries
	}
	if entries == nil {
		entries = make(map[earlyTerminationIdentity]time.Time)
	}
	var oldest earlyTerminationIdentity
	var oldestExpiry time.Time
	for candidate, expiry := range entries {
		if !now.Before(expiry) {
			delete(entries, candidate)
			continue
		}
		if oldestExpiry.IsZero() || expiry.Before(oldestExpiry) {
			oldest, oldestExpiry = candidate, expiry
		}
	}
	if len(entries) >= maxEndedCalls {
		delete(entries, oldest)
	}
	entries[key] = now.Add(endedCallRetention)
	return entries
}

func hasEarlyTermination(entries map[earlyTerminationIdentity]time.Time, meta types.BasicCallMeta, group bool, now time.Time) bool {
	for key, expiry := range entries {
		if !now.Before(expiry) {
			delete(entries, key)
			continue
		}
		if key.matchesOffer(meta, group) {
			return true
		}
	}
	return false
}

// A relay allocation may select another device of the same user before an
// answer pins the device. It must not substitute a different account, or a
// different explicitly pinned device. Missing peer_pid metadata does not
// replace the already established peer identity.
func directRelayPeerMatches(peer, elected types.JID) bool {
	return elected.IsEmpty() || sameDirectControlIdentity(peer, elected)
}
