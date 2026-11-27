package connector

import (
	"sync"
	"testing"

	"github.com/purpshell/meowcaller"
)

func TestVoIPDialGeneration(t *testing.T) {
	wa := &WhatsAppClient{}
	old := wa.currentVoIPGeneration()
	wa.voipCallsMu.Lock()
	wa.voipGeneration++
	current := &meowcaller.Call{}
	if wa.tryRegisterVoIPCallLocked("call", &meowcaller.Call{}, old) || len(wa.voipCalls) != 0 {
		t.Fatal("obsolete dial admitted")
	}
	if !wa.tryRegisterVoIPCallLocked("call", current, wa.voipGeneration) || wa.tryRegisterVoIPCallLocked("call", &meowcaller.Call{}, wa.voipGeneration) || wa.voipCalls["call"] != current {
		t.Fatal("current call overwritten or rejected")
	}
	wa.voipCallsMu.Unlock()
}

func TestVoIPDialGenerationConcurrentTeardown(t *testing.T) {
	for i := 0; i < 100; i++ {
		wa := &WhatsAppClient{}
		old := wa.currentVoIPGeneration()
		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			defer wg.Done()
			wa.voipCallsMu.Lock()
			defer wa.voipCallsMu.Unlock()
			wa.tryRegisterVoIPCallLocked("call", &meowcaller.Call{}, old)
		}()
		go func() {
			defer wg.Done()
			wa.voipCallsMu.Lock()
			defer wa.voipCallsMu.Unlock()
			wa.voipGeneration++
			wa.voipCalls = make(map[string]*meowcaller.Call)
		}()
		wg.Wait()
		if len(wa.voipCalls) != 0 {
			t.Fatal("old dial escaped teardown")
		}
	}
}
