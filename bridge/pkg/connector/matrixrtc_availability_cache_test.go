package connector

import (
	"context"
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

func TestMatrixRTCAvailabilityCacheReadReconcilesAliases(t *testing.T) {
	c := newMatrixRTCAvailabilityCache()
	now := time.Unix(100, 0)
	keys := []string{"login:pn", "login:lid", "login:hosted"}
	positive := matrixRTCCallAvailabilityCacheEntry{matrixRTCCallAvailabilityEnabled, "consumer", now.Add(time.Hour)}
	negative := matrixRTCCallAvailabilityCacheEntry{matrixRTCCallAvailabilityDisabled, "server:406", now.Add(time.Minute)}
	c.remember(keys[:1], positive, now)
	c.remember(keys[1:2], negative, now)
	if got, ok := c.get(keys, now.Add(time.Second)); !ok || got != negative {
		t.Fatalf("alias-aware lookup lost restriction: %+v", got)
	}
	for _, key := range keys {
		if got, ok := c.get([]string{key}, now.Add(30*time.Second)); !ok || got != negative {
			t.Fatalf("portal alias %s did not inherit restriction: %+v", key, got)
		}
	}
	for _, key := range keys {
		if _, ok := c.get([]string{key}, negative.expires); ok {
			t.Fatalf("read renewed expiry for %s", key)
		}
	}
	if _, ok := c.get([]string{"other-login:pn"}, now); ok {
		t.Fatal("login isolation violated")
	}
}

func TestMatrixRTCAvailabilityCachePortalAliasLookup(t *testing.T) {
	c := newMatrixRTCAvailabilityCache()
	now := time.Unix(100, 0)
	positive := matrixRTCCallAvailabilityCacheEntry{matrixRTCCallAvailabilityEnabled, "consumer", now.Add(time.Hour)}
	hidden := matrixRTCCallAvailabilityCacheEntry{matrixRTCCallAvailabilityHidden, "business_profile:hide_in_chat", now.Add(time.Minute)}
	c.remember([]string{"login:pn"}, positive, now)
	c.remember([]string{"login:lid"}, hidden, now)
	for _, keys := range [][]string{{"login:pn", "login:lid"}, {"login:lid", "login:pn"}} {
		got, ok := c.get(keys, now)
		if !ok || got != hidden {
			t.Fatalf("portal alias lookup did not preserve hidden capability: %+v", got)
		}
	}
}

func TestRTCAvailabilityWatchExpirySurvivesPruningAndIsLoginScoped(t *testing.T) {
	c := newMatrixRTCAvailabilityCache()
	now := time.Now()
	first := matrixRTCCallAvailabilityCacheEntry{matrixRTCCallAvailabilityDisabled, "server:406", now.Add(time.Minute)}
	c.remember([]string{"a\x00pn"}, first, now)
	c.remember([]string{"b\x00pn"}, matrixRTCCallAvailabilityCacheEntry{matrixRTCCallAvailabilityEnabled, "consumer", now.Add(time.Second)}, now)
	a := c.watch("a")
	defer a.close()
	a.begin(now)
	if deadline, dirty := a.next(); !deadline.Equal(first.expires) || dirty {
		t.Fatalf("wrong initial deadline %v %v", deadline, dirty)
	}
	c.get([]string{"b\x00pn"}, first.expires)
	if _, ok := c.get([]string{"a\x00pn"}, first.expires); ok {
		t.Fatal("expired result retained")
	}
	if deadline, dirty := a.next(); !deadline.Equal(first.expires) || dirty {
		t.Fatal("pruning lost outstanding expiry")
	}
	a.begin(first.expires)
	if deadline, dirty := a.next(); !deadline.IsZero() || dirty {
		t.Fatal("expiry not consumed by scan")
	}
}

func TestRTCAvailabilityWatchPolicyChangesAndAliasRead(t *testing.T) {
	c := newMatrixRTCAvailabilityCache()
	now := time.Now()
	w := c.watch("a")
	defer w.close()
	w.begin(now)
	positive := matrixRTCCallAvailabilityCacheEntry{matrixRTCCallAvailabilityEnabled, "consumer", now.Add(time.Hour)}
	negative := matrixRTCCallAvailabilityCacheEntry{matrixRTCCallAvailabilityDisabled, "server:406", now.Add(time.Minute)}
	c.remember([]string{"b\x00pn"}, negative, now)
	if _, dirty := w.next(); dirty {
		t.Fatal("other login dirtied watch")
	}
	c.remember([]string{"a\x00pn"}, positive, now)
	w.begin(now)
	c.remember([]string{"a\x00lid"}, negative, now)
	if deadline, dirty := w.next(); !dirty || !deadline.Equal(negative.expires) {
		t.Fatal("earlier negative was not scheduled")
	}
	w.begin(now)
	c.get([]string{"a\x00pn", "a\x00lid"}, now)
	if _, dirty := w.next(); !dirty {
		t.Fatal("alias propagation did not request reconciliation")
	}
	w.begin(now)
	c.get([]string{"a\x00pn", "a\x00lid"}, now)
	if _, dirty := w.next(); dirty {
		t.Fatal("unchanged cache hit requests infinite refreshes")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	c.rememberContext(ctx, []string{"a\x00pn"}, positive, now)
	if _, dirty := w.next(); dirty {
		t.Fatal("cancelled probe published")
	}
}

func TestRTCAvailabilityWatchReplacement(t *testing.T) {
	c := newMatrixRTCAvailabilityCache()
	a := c.watch("a")
	b := c.watch("a")
	a.close()
	c.remember([]string{"a\x00pn"}, matrixRTCCallAvailabilityCacheEntry{matrixRTCCallAvailabilityEnabled, "consumer", time.Now().Add(time.Hour)}, time.Now())
	if _, dirty := b.next(); !dirty {
		t.Fatal("old watch removed its replacement")
	}
	b.close()
	if len(c.watches) != 0 {
		t.Fatal("watch leaked")
	}
}

func TestRTCAvailabilityWatchExplicitReconcileAndOffline(t *testing.T) {
	c := newMatrixRTCAvailabilityCache()
	c.requestReconcile("a")
	if len(c.watches) != 0 {
		t.Fatal("offline denial created a worker")
	}
	a := c.watch("a")
	defer a.close()
	b := c.watch("b")
	defer b.close()
	a.begin(time.Now())
	b.begin(time.Now())
	c.requestReconcile("a")
	if _, dirty := a.next(); !dirty {
		t.Fatal("dial denial did not wake scanner")
	}
	if _, dirty := b.next(); dirty {
		t.Fatal("other login affected")
	}
	a.begin(time.Now())
	if _, dirty := a.next(); dirty {
		t.Fatal("reconcile request not consumed")
	}
}
