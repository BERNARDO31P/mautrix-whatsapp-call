package meowcaller

import (
	"context"
	"testing"
)

func TestGroupPreacceptFailureEndsCurrentCall(t *testing.T) {
	e := newEngine(nil)
	call := &Call{eng: e, id: "group", phase: CallPhaseRinging}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	e.calls[call.ID()] = &engineCall{call: call, cancel: cancel, group: true}
	var reason string
	call.OnEnd(func(value string) { reason = value })
	e.finishCallIfCurrent(call, "group-preaccept-send-failed")
	if e.calls[call.ID()] != nil || !e.recentlyEnded(call.ID()) || call.State() != CallPhaseEnded || ctx.Err() != context.Canceled || reason != "group-preaccept-send-failed" {
		t.Fatal("failed preparation left a live call")
	}
}

func TestGroupPreacceptFailureDoesNotEndReplacement(t *testing.T) {
	e := newEngine(nil)
	old := &Call{eng: e, id: "group", phase: CallPhaseRinging}
	replacement := &Call{eng: e, id: "group", phase: CallPhaseRinging}
	e.calls[old.ID()] = &engineCall{call: replacement, group: true}
	e.finishCallIfCurrent(old, "group-preaccept-build-failed")
	if e.calls[old.ID()].call != replacement || e.recentlyEnded(old.ID()) || replacement.State() != CallPhaseRinging {
		t.Fatal("stale preparation altered replacement")
	}
	e.finishCallIfCurrent(nil, "ignored")
}
