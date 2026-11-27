package meowcaller

import (
	"context"
	"testing"
	"time"
)

func TestRelayAllocationWatchStatus(t *testing.T) {
	var w relayAllocationWatch
	if got := w.status(); got != relayAllocationSilent {
		t.Fatal(got)
	}
	w.received.Store(true)
	if got := w.status(); got != relayAllocationUnconfirmed {
		t.Fatal(got)
	}
	w.rejected.Store(true)
	if got := w.status(); got != relayAllocationRejected {
		t.Fatal(got)
	}
	w.acknowledged.Store(true)
	if got := w.status(); got != relayAllocationAcknowledged {
		t.Fatal(got)
	}
}

func TestRelayAllocationWatchAbsoluteCheckpoints(t *testing.T) {
	// Start 800ms in the past; both checks should finish in ~100ms,
	// rather than waiting another 900ms after the first check.
	start := time.Now().Add(-800 * time.Millisecond)
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	var checkpoints []time.Duration
	runRelayAllocationWatch(ctx, start, func(elapsed time.Duration) { checkpoints = append(checkpoints, elapsed) })
	if len(checkpoints) != 2 || checkpoints[0] < 800*time.Millisecond || checkpoints[1] < 900*time.Millisecond || checkpoints[1] >= 1300*time.Millisecond {
		t.Fatalf("checkpoints=%v", checkpoints)
	}
}

func TestRelayAllocationWatchCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	runRelayAllocationWatch(ctx, time.Now().Add(-time.Second), func(time.Duration) { t.Fatal("reported after cancellation") })
}

func TestRelayAllocationWatchCancellationBetweenChecks(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	reports := 0
	runRelayAllocationWatch(ctx, time.Now().Add(-time.Second), func(time.Duration) { reports++; cancel() })
	if reports != 1 {
		t.Fatalf("reports=%d", reports)
	}
}

func TestInitialRelayRejection(t *testing.T) {
	for _, tc := range []struct {
		name                         string
		group, acknowledged, hasCode bool
		want                         string
	}{
		{"direct initial rejection", false, false, true, "direct relay allocation rejected: STUN error 403"},
		{"direct missing error code", false, false, false, "direct relay allocation rejected"},
		{"direct acknowledged", false, true, true, ""},
		{"group initial rejection", true, false, true, ""},
		{"group acknowledged", true, true, true, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := initialRelayRejection(tc.group, tc.acknowledged, 403, tc.hasCode)
			if tc.want == "" {
				if err != nil {
					t.Fatal(err)
				}
			} else if err == nil || err.Error() != tc.want {
				t.Fatalf("error=%v, want %s", err, tc.want)
			}
		})
	}
}
