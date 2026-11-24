package connector

import (
	"sync"
	"testing"
	"time"
)

func TestMatrixRTCAvailabilityCacheAliasPrecedenceAndExpiry(t *testing.T) {
	c := newMatrixRTCAvailabilityCache()
	now := time.Unix(100, 0)
	keys := []string{"login:pn", "login:lid"}
	positive := matrixRTCCallAvailabilityCacheEntry{matrixRTCCallAvailabilityEnabled, "consumer", now.Add(time.Hour)}
	negative := matrixRTCCallAvailabilityCacheEntry{matrixRTCCallAvailabilityDisabled, "business_calling=false", now.Add(time.Minute)}
	c.remember(keys[:1], positive, now)
	c.remember(keys[1:], negative, now)
	if got, _ := c.get(keys, now); got != negative {
		t.Fatalf("alias restriction lost: %+v", got)
	}
	if got := c.remember(keys, positive, now); got != negative {
		t.Fatalf("late positive replaced negative: %+v", got)
	}
	if got, _ := c.get(keys[:1], now); got != negative {
		t.Fatalf("negative not propagated: %+v", got)
	}
	if _, ok := c.get(keys, now.Add(time.Minute)); ok {
		t.Fatal("restriction expiry was extended by fallback")
	}
	if got := c.remember(keys, positive, now.Add(time.Minute)); got != positive {
		t.Fatal("expired negative prevents recovery")
	}
	if _, ok := c.get([]string{"other-login:lid"}, now); ok {
		t.Fatal("login isolation violated")
	}
}

func TestMatrixRTCAvailabilityCacheConcurrentWeakProbesCannotOverwriteNegative(t *testing.T) {
	c := newMatrixRTCAvailabilityCache()
	now := time.Unix(100, 0)
	keys := []string{"pn", "lid"}
	negative := matrixRTCCallAvailabilityCacheEntry{matrixRTCCallAvailabilityDisabled, "server:406", now.Add(time.Hour)}
	positive := matrixRTCCallAvailabilityCacheEntry{matrixRTCCallAvailabilityEnabled, "consumer", now.Add(6 * time.Hour)}
	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); c.remember(keys, positive, now) }()
	}
	c.remember(keys, negative, now)
	wg.Wait()
	if got, ok := c.get(keys, now); !ok || got != negative {
		t.Fatalf("concurrent fallback erased restriction: %+v", got)
	}
}
