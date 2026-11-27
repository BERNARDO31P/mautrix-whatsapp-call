package connector

import (
	"context"
	"sync"
	"testing"

	"github.com/purpshell/meowcaller"
	"go.mau.fi/mautrix-whatsapp/pkg/connector/voipbridge"
)

func TestVoIPDialCancellationOnDetach(t *testing.T) {
	wa := &WhatsAppClient{}
	wa.openVoIPAdmission()
	first, gen, finish := wa.beginVoIPDial(context.Background())
	defer finish()
	second, _, finishSecond := wa.beginVoIPDial(context.Background())
	defer finishSecond()
	wa.detachVoIPRegistry("login")
	if first.Err() != context.Canceled || second.Err() != context.Canceled {
		t.Fatal("old attempts survived detach")
	}
	wa.openVoIPAdmission()
	next, nextGen, done := wa.beginVoIPDial(context.Background())
	defer done()
	finish()
	finishSecond()
	if next.Err() != nil || nextGen == gen || len(wa.voipDialAttempts) != 1 {
		t.Fatal("old cleanup affected new attempt")
	}
	done()
	if len(wa.voipDialAttempts) != 0 {
		t.Fatal("completed attempt retained")
	}
}

func TestVoIPDialParentCancellation(t *testing.T) {
	wa := &WhatsAppClient{}
	wa.openVoIPAdmission()
	parent, cancel := context.WithCancel(context.Background())
	ctx, _, done := wa.beginVoIPDial(parent)
	defer done()
	cancel()
	if ctx.Err() != context.Canceled {
		t.Fatal("caller cancellation not propagated")
	}
}

func TestVoIPDialConcurrentDetach(t *testing.T) {
	for i := 0; i < 100; i++ {
		wa := &WhatsAppClient{}
		wa.openVoIPAdmission()
		var wg sync.WaitGroup
		var ctx context.Context
		var generation uint64
		var done func()
		wg.Add(2)
		go func() { defer wg.Done(); ctx, generation, done = wa.beginVoIPDial(context.Background()) }()
		go func() { defer wg.Done(); wa.detachVoIPRegistry("login") }()
		wg.Wait()
		if ctx.Err() != context.Canceled || generation > wa.currentVoIPGeneration() {
			t.Fatal("generation/cancellation mismatch")
		}
		done()
		if len(wa.voipDialAttempts) != 0 {
			t.Fatal("attempt retained")
		}
	}
}

func TestVoIPOfferGenerationAcrossDisconnect(t *testing.T) {
	wa := &WhatsAppClient{}
	if _, open := wa.voipOfferGeneration(); open {
		t.Fatal("new login already admits calls")
	}
	wa.openVoIPAdmission()
	old, open := wa.voipOfferGeneration()
	if !open || !wa.voipAdmissionCurrent(old) {
		t.Fatal("connected offer rejected")
	}
	wa.detachVoIPRegistry("login")
	closed, open := wa.voipOfferGeneration()
	if open || wa.voipAdmissionCurrent(closed) {
		t.Fatal("disconnected offer admitted")
	}
	ctx, _, done := wa.beginVoIPDial(context.Background())
	defer done()
	wa.openVoIPAdmission()
	if ctx.Err() != context.Canceled || wa.voipAdmissionCurrent(old) || !wa.voipAdmissionCurrent(closed) {
		t.Fatal("reconnect revived an old offer or dial")
	}
}

func TestVoIPExplicitStopRejectsLateConnected(t *testing.T) {
	wa := &WhatsAppClient{}
	wa.openVoIPAdmission()
	ctx, gen, done := wa.beginVoIPDial(context.Background())
	defer done()
	wa.stopVoIPAdmission()
	wa.openVoIPAdmission()
	if ctx.Err() != context.Canceled || wa.voipAdmissionCurrent(gen) {
		t.Fatal("late connected reopened explicitly stopped login")
	}
	wa.detachVoIPRegistry("login")
	wa.allowVoIPReconnect()
	if _, open := wa.voipOfferGeneration(); open {
		t.Fatal("explicit reconnect opened before connected event")
	}
	wa.openVoIPAdmission()
	current, open := wa.voipOfferGeneration()
	if !open || current == gen || wa.voipAdmissionCurrent(gen) {
		t.Fatal("new login generation not isolated")
	}
}

func TestVoIPConnectionWorkReplacement(t *testing.T) {
	wa := &WhatsAppClient{}
	parent := context.Background()
	if _, _, ok := wa.bindVoIPConnection(parent); ok {
		t.Fatal("disconnected gateway admitted")
	}
	wa.openVoIPAdmission()
	old, finish, ok := wa.bindVoIPConnection(parent)
	if !ok {
		t.Fatal("connected gateway rejected")
	}
	defer finish()
	var probe context.Context
	wa.startRTCConnectionWork(parent, func(ctx context.Context) { probe = ctx })
	wa.openVoIPAdmission()
	if old.Err() != nil {
		t.Fatal("duplicate Connected cancelled active work")
	}
	wa.detachVoIPRegistryForConnectionEnd("login", true)
	if old.Err() != context.Canceled || probe.Err() != context.Canceled {
		t.Fatal("old call or refresh work survived replacement")
	}
	wa.openVoIPAdmission()
	wa.startRTCConnectionWork(parent, func(context.Context) { t.Fatal("late Connected restarted probes") })
	if _, _, ok := wa.bindVoIPConnection(parent); ok {
		t.Fatal("late Connected reopened admission")
	}
	wa.allowVoIPReconnect()
	wa.openVoIPAdmission()
	next, nextFinish, ok := wa.bindVoIPConnection(parent)
	if !ok {
		t.Fatal("explicit reconnect did not open admission")
	}
	defer nextFinish()
	finish()
	if next.Err() != nil || len(wa.voipConnectionWork) != 1 {
		t.Fatal("old cleanup affected new connection")
	}
}

func TestVoIPConnectionWorkConcurrentDetach(t *testing.T) {
	for i := 0; i < 100; i++ {
		wa := &WhatsAppClient{}
		wa.openVoIPAdmission()
		var wg sync.WaitGroup
		var ctx context.Context
		var finish context.CancelFunc
		var ok bool
		wg.Add(2)
		go func() { defer wg.Done(); ctx, finish, ok = wa.bindVoIPConnection(context.Background()) }()
		go func() { defer wg.Done(); wa.detachVoIPRegistryForConnectionEnd("login", true) }()
		wg.Wait()
		if ok {
			if ctx.Err() != context.Canceled {
				t.Fatal("gateway survived replacement")
			}
			finish()
		}
		if len(wa.voipConnectionWork) != 0 {
			t.Fatal("retained work")
		}
	}
}

func TestVoIPStreamReplacementRegistryIsolation(t *testing.T) {
	manager := voipbridge.NewManager()
	oldID := voipbridge.ID{LoginID: "old", CallID: "call"}
	otherID := voipbridge.ID{LoginID: "other", CallID: "call"}
	for _, id := range []voipbridge.ID{oldID, otherID} {
		if _, err := manager.Start(id, "peer", voipbridge.Outgoing); err != nil {
			t.Fatal(err)
		}
	}
	old := &meowcaller.Call{}
	bus := voipbridge.NewMediaBus(2)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	wa := &WhatsAppClient{callManager: manager, voipCalls: map[string]*meowcaller.Call{"call": old}, voipMedia: map[string]*voipbridge.MediaBus{"call": bus}, voipGatewayCancel: map[string]context.CancelFunc{"call": cancel}}
	wa.openVoIPAdmission()
	calls, media, gateways := wa.detachVoIPRegistryForConnectionEnd("old", true)
	if calls["call"] != old || media["call"] != bus || gateways["call"] == nil || len(wa.voipCalls) != 0 || len(wa.voipMedia) != 0 || len(wa.voipGatewayCancel) != 0 {
		t.Fatal("replacement did not detach exact resources")
	}
	gateways["call"]()
	if ctx.Err() != context.Canceled {
		t.Fatal("gateway cancellation failed")
	}
	if state, _ := manager.Get(oldID); state.State != voipbridge.Ended || state.EndReason != "whatsapp-stream-replaced" {
		t.Fatalf("wrong terminal state: %#v", state)
	}
	if state, _ := manager.Get(otherID); state.State != voipbridge.Ringing {
		t.Fatal("another login was terminated")
	}
}
