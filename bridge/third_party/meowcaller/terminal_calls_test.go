package meowcaller

import (
	"fmt"
	"testing"
	"time"
)

func TestEndedCallReservation(t *testing.T) {
	now := time.Unix(1000, 0)
	ended := rememberEndedCall(nil, "early", now)
	if !ended["early"].Equal(now.Add(endedCallRetention)) {
		t.Fatal("missing terminal reservation")
	}
	rememberEndedCall(ended, "early", now.Add(time.Second))
	if !ended["early"].Equal(now.Add(endedCallRetention)) {
		t.Fatal("duplicate extended terminal reservation")
	}
	rememberEndedCall(ended, "later", now.Add(endedCallRetention))
	if _, ok := ended["early"]; ok || len(ended) != 1 {
		t.Fatal("expired reservation was not reclaimed")
	}
	rememberEndedCall(ended, "", now)
	if len(ended) != 1 {
		t.Fatal("empty ID created a reservation")
	}
}

func TestEndedCallReservationBound(t *testing.T) {
	now := time.Unix(1000, 0)
	ended := make(map[string]time.Time)
	for i := 0; i < maxEndedCalls; i++ {
		ended[fmt.Sprintf("call-%04d", i)] = now.Add(time.Minute)
	}
	rememberEndedCall(ended, "new", now)
	if len(ended) != maxEndedCalls || !now.Before(ended["new"]) {
		t.Fatal("reservation capacity not maintained")
	}
	if _, ok := ended["call-0000"]; ok {
		t.Fatal("oldest reservation not evicted deterministically")
	}
}
