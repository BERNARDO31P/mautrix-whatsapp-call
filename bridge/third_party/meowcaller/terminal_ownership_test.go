package meowcaller

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"

	waBinary "go.mau.fi/whatsmeow/binary"
)

func TestStaleTerminalHandleCannotEndReplacement(t *testing.T) {
	eng, old := testEngineWithOutgoingCall()
	replacement := &Call{eng: eng, id: old.ID(), phase: CallPhaseRinging}
	current := &engineCall{call: replacement}
	eng.calls[old.ID()] = current
	var sends atomic.Int32
	eng.sendCallNode = func(context.Context, waBinary.Node) error { sends.Add(1); return nil }
	if err := old.Hangup(); err != nil {
		t.Fatal(err)
	}
	if err := old.Reject(); err != nil {
		t.Fatal(err)
	}
	if sends.Load() != 0 || eng.lookup(old.ID()) != current || replacement.State() != CallPhaseRinging {
		t.Fatal("stale terminal handle affected replacement")
	}
}

func TestConcurrentTerminalActionsSendOnlyOnce(t *testing.T) {
	eng, call := testEngineWithOutgoingCall()
	var sends atomic.Int32
	eng.sendCallNode = func(context.Context, waBinary.Node) error { sends.Add(1); return nil }
	var wg sync.WaitGroup
	start := make(chan struct{})
	for i := 0; i < 32; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			var err error
			if i%2 == 0 {
				err = call.Hangup()
			} else {
				err = call.Reject()
			}
			if err != nil {
				t.Error(err)
			}
		}(i)
	}
	close(start)
	wg.Wait()
	if sends.Load() != 1 || call.State() != CallPhaseEnded || eng.lookup(call.ID()) != nil {
		t.Fatalf("terminal action count %d, phase %v", sends.Load(), call.State())
	}
}
