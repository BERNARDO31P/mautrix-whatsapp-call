package meowcaller

import "testing"

func TestGroupRelaySubscriptionOrder(t *testing.T) {
	var s groupRelaySubscriptionOrder
	if !s.accepts(0, 0) {
		t.Fatal("initial allocation refused")
	}
	s.commit(4, 7)
	for _, tc := range []struct {
		name          string
		relay, roster uint32
		want          bool
	}{
		{"duplicate", 4, 7, false},
		{"roster only", 4, 8, true},
		{"credential only", 5, 7, true},
		{"both", 5, 8, true},
		{"stale credentials", 3, 8, false},
		{"stale roster", 5, 6, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := s.accepts(tc.relay, tc.roster); got != tc.want {
				t.Fatalf("accepts = %v, want %v", got, tc.want)
			}
		})
	}
	if !s.accepts(4, 8) || !s.accepts(4, 8) {
		t.Fatal("uncommitted send cannot be retried")
	}
	s.commit(4, 8)
	if s.accepts(4, 7) || s.accepts(4, 8) || !s.accepts(4, 9) {
		t.Fatal("incorrect roster watermark after commit")
	}
}
