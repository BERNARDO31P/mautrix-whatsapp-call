package meowcaller

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"
)

const (
	relayAllocationSilent       = "no_packets"
	relayAllocationUnconfirmed  = "packets_without_matching_allocation_response"
	relayAllocationAcknowledged = "allocation_acknowledged"
	relayAllocationRejected     = "allocation_rejected"
)

type relayAllocationWatch struct {
	received     atomic.Bool
	acknowledged atomic.Bool
	rejected     atomic.Bool
}

func (w *relayAllocationWatch) status() string {
	if w.acknowledged.Load() {
		return relayAllocationAcknowledged
	}
	if w.rejected.Load() {
		return relayAllocationRejected
	}
	if w.received.Load() {
		return relayAllocationUnconfirmed
	}
	return relayAllocationSilent
}

// A matching rejection before the first direct allocation acknowledgement is
// terminal. A group refresh (or a rejection after acknowledgement) retains its
// existing recovery path; it is not proof that the established media died.
func initialRelayRejection(group, acknowledged bool, code uint16, hasCode bool) error {
	if group || acknowledged {
		return nil
	}
	if hasCode {
		return fmt.Errorf("direct relay allocation rejected: STUN error %d", code)
	}
	return fmt.Errorf("direct relay allocation rejected")
}

// Use absolute checkpoints: the second check is 900ms after setup, not
// 900ms after the first 400ms check. Do not emit after call cancellation.
func runRelayAllocationWatch(ctx context.Context, start time.Time, report func(time.Duration)) {
	for _, checkpoint := range []time.Duration{400 * time.Millisecond, 900 * time.Millisecond} {
		if ctx.Err() != nil {
			return
		}
		timer := time.NewTimer(time.Until(start.Add(checkpoint)))
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
		}
		if ctx.Err() != nil {
			return
		}
		report(time.Since(start))
	}
}
