package meowcaller

// Both transactions are monotonic within one call. A roster-only change
// must update subscriptions even when the allocation credentials do not rotate.
// The allocation mutex protects this state; commit only after a successful send.
type groupRelaySubscriptionOrder struct {
	initialized bool
	relayTx     uint32
	rosterTx    uint32
}

func (s groupRelaySubscriptionOrder) accepts(relayTx, rosterTx uint32) bool {
	if !s.initialized {
		return true
	}
	return relayTx >= s.relayTx && rosterTx >= s.rosterTx &&
		(relayTx > s.relayTx || rosterTx > s.rosterTx)
}

func (s *groupRelaySubscriptionOrder) commit(relayTx, rosterTx uint32) {
	*s = groupRelaySubscriptionOrder{initialized: true, relayTx: relayTx, rosterTx: rosterTx}
}
