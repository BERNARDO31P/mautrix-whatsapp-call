package connector

import (
	"context"
	"sync"
	"testing"

	"github.com/purpshell/meowcaller"
)

func TestIncomingGatewayRequiresCurrentCall(t *testing.T) {
	current := &meowcaller.Call{}
	obsolete := &meowcaller.Call{}
	wa := &WhatsAppClient{voipCalls: map[string]*meowcaller.Call{"call": current}}
	cancel := func() {}
	if wa.tryRegisterIncomingRTCGateway("call", obsolete, cancel) || wa.tryRegisterIncomingRTCGateway("missing", current, cancel) || wa.tryRegisterIncomingRTCGateway("call", nil, cancel) || wa.tryRegisterIncomingRTCGateway("", current, cancel) {
		t.Fatal("unregistered call admitted")
	}
	if len(wa.voipGatewayCancel) != 0 {
		t.Fatal("rejected admission mutated gateway registry")
	}
	if !wa.tryRegisterIncomingRTCGateway("call", current, cancel) || wa.tryRegisterIncomingRTCGateway("call", current, cancel) {
		t.Fatal("current call must be admitted exactly once")
	}
}

func TestIncomingGatewayCannotCrossTeardownSnapshot(t *testing.T) {
	for i := 0; i < 100; i++ {
		call := &meowcaller.Call{}
		wa := &WhatsAppClient{voipCalls: map[string]*meowcaller.Call{"call": call}}
		ctx, cancel := context.WithCancel(context.Background())
		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			defer wg.Done()
			if !wa.tryRegisterIncomingRTCGateway("call", call, cancel) {
				cancel()
			}
		}()
		go func() {
			defer wg.Done()
			// Reproduce the locked registry swap in teardownVoIP, without
			// invoking network hangup on a deliberately inert call handle.
			wa.voipCallsMu.Lock()
			pending := wa.voipGatewayCancel
			wa.voipCalls = make(map[string]*meowcaller.Call)
			wa.voipGatewayCancel = make(map[string]context.CancelFunc)
			wa.voipCallsMu.Unlock()
			for _, stop := range pending {
				stop()
			}
		}()
		wg.Wait()
		if ctx.Err() == nil || len(wa.voipGatewayCancel) != 0 {
			t.Fatal("gateway escaped disconnect cancellation")
		}
		cancel()
	}
}
