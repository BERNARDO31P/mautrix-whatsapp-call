package connector

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/event"
	"maunium.net/go/mautrix/id"
)

func TestRTCInviteQueueBoundedAndCancellable(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	wa := &WhatsAppConnector{Bridge: &bridgev2.Bridge{BackgroundCtx: ctx}}
	deadline := time.Now().Add(10 * time.Second)
	started := make(chan struct{}, rtcPendingInviteLimit)
	var unexpected atomic.Int32
	for i := 0; i < rtcPendingInviteLimit; i++ {
		evt := &event.Event{RoomID: "!room:example.org", ID: id.EventID(fmt.Sprintf("$invite%d", i)), Sender: "@alice:example.org"}
		wa.queueRTCInvite(evt, "DEVICE", deadline, func(jobCtx context.Context) {
			started <- struct{}{}
			<-jobCtx.Done()
		})
	}
	for i := 0; i < rtcPendingInviteLimit; i++ {
		select {
		case <-started:
		case <-time.After(time.Second):
			t.Fatal("invitation did not start")
		}
	}
	duplicate := &event.Event{RoomID: "!room:example.org", ID: "$invite0", Sender: "@alice:example.org"}
	extra := &event.Event{RoomID: "!room:example.org", ID: "$extra", Sender: "@alice:example.org"}
	wa.queueRTCInvite(duplicate, "DEVICE", deadline, func(context.Context) { unexpected.Add(1) })
	wa.queueRTCInvite(extra, "DEVICE", deadline, func(context.Context) { unexpected.Add(1) })
	wa.rtcMu.Lock()
	if len(wa.rtcPendingInvites) != rtcPendingInviteLimit {
		t.Error("pending queue limit violated")
	}
	pending := wa.rtcPendingInvites[rtcMemberEventKey(duplicate.RoomID.String(), duplicate.ID.String())]
	if pending.sender != "@alice:example.org" || pending.device != "DEVICE" {
		t.Error("wrong pending owner")
	}
	pending.cancel()
	wa.rtcMu.Unlock()
	waitUntil := func(want int) {
		t.Helper()
		until := time.Now().Add(time.Second)
		for {
			wa.rtcMu.Lock()
			count := len(wa.rtcPendingInvites)
			wa.rtcMu.Unlock()
			if count == want {
				return
			}
			if time.Now().After(until) {
				t.Fatalf("pending count %d, want %d", count, want)
			}
			time.Sleep(time.Millisecond)
		}
	}
	waitUntil(rtcPendingInviteLimit - 1)
	cancel()
	waitUntil(0)
	wa.queueRTCInvite(extra, "DEVICE", deadline, func(context.Context) { unexpected.Add(1) })
	if unexpected.Load() != 0 {
		t.Fatal("duplicate, excess or cancelled job ran")
	}
}

func TestRTCInviteQueueExpired(t *testing.T) {
	wa := &WhatsAppConnector{Bridge: &bridgev2.Bridge{BackgroundCtx: context.Background()}}
	evt := &event.Event{RoomID: "!room:example.org", ID: "$expired", Sender: "@alice:example.org"}
	wa.queueRTCInvite(evt, "DEVICE", time.Now().Add(-time.Second), func(context.Context) { t.Error("expired invite ran") })
	wa.rtcMu.Lock()
	defer wa.rtcMu.Unlock()
	if len(wa.rtcPendingInvites) != 0 {
		t.Fatal("expired invite registered")
	}
}
