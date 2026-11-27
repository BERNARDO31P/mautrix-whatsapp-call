package connector

import (
	"context"
	"sync/atomic"
	"testing"
	"time"
)

func TestRTCPortalRefreshBatchBounded(t *testing.T) {
	b := newRTCPortalRefreshBatch(context.Background(), 4)
	var active, count atomic.Int32
	for i := 0; i < 200; i++ {
		if !b.submit(func() {
			if n := active.Add(1); n > 4 {
				t.Errorf("active = %d", n)
			}
			time.Sleep(time.Microsecond)
			count.Add(1)
			active.Add(-1)
		}) {
			t.Fatal("unexpected cancellation")
		}
	}
	b.wait()
	if count.Load() != 200 || active.Load() != 0 {
		t.Fatal("lost work")
	}
}

func TestRTCPortalRefreshBatchCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	b := newRTCPortalRefreshBatch(ctx, 1)
	started := make(chan struct{})
	if !b.submit(func() { close(started); <-ctx.Done() }) {
		t.Fatal("first job rejected")
	}
	<-started
	cancel()
	if b.submit(func() { t.Error("cancelled work executed") }) {
		t.Fatal("cancelled job admitted")
	}
	b.wait()
	if len(b.slots) != 0 {
		t.Fatal("capacity leak")
	}
}

func TestRTCRefreshCoalescerLatest(t *testing.T) {
	var c rtcRefreshCoalescer
	started := make(chan struct{})
	release := make(chan struct{})
	done := make(chan struct{})
	var obsolete atomic.Int32
	c.request(context.Background(), func(context.Context) { close(started); <-release })
	<-started
	for i := 0; i < 100; i++ {
		c.request(context.Background(), func(context.Context) { obsolete.Add(1) })
	}
	c.request(context.Background(), func(context.Context) { close(done) })
	close(release)
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("pending refresh lost")
	}
	if obsolete.Load() != 0 {
		t.Fatal("obsolete refresh ran")
	}
}

func TestRTCRefreshCoalescerIdleRestart(t *testing.T) {
	var c rtcRefreshCoalescer
	done := make(chan struct{})
	c.request(context.Background(), func(context.Context) { close(done) })
	<-done
	deadline := time.Now().Add(time.Second)
	for {
		c.mu.Lock()
		running := c.running
		c.mu.Unlock()
		if !running {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("worker never went idle")
		}
		time.Sleep(time.Millisecond)
	}
	second := make(chan struct{})
	c.request(context.Background(), func(context.Context) { close(second) })
	select {
	case <-second:
	case <-time.After(time.Second):
		t.Fatal("idle worker did not restart")
	}
}

func TestRTCRefreshCoalescerStopDropsPending(t *testing.T) {
	var c rtcRefreshCoalescer
	started := make(chan struct{})
	cancelled := make(chan struct{})
	release := make(chan struct{})
	done := make(chan struct{})
	var obsolete atomic.Int32
	c.request(context.Background(), func(ctx context.Context) {
		close(started)
		<-ctx.Done()
		close(cancelled)
		<-release
	})
	<-started
	c.request(context.Background(), func(context.Context) { obsolete.Add(1) })
	c.stop()
	select {
	case <-cancelled:
	case <-time.After(time.Second):
		t.Fatal("active refresh not cancelled")
	}
	c.request(context.Background(), func(ctx context.Context) {
		if ctx.Err() != nil {
			t.Error("new refresh cancelled")
		}
		close(done)
	})
	select {
	case <-done:
		t.Fatal("new refresh overlapped old batch")
	default:
	}
	close(release)
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("new refresh did not start")
	}
	if obsolete.Load() != 0 {
		t.Fatal("obsolete refresh ran")
	}
}

func TestRTCRefreshCoalescerStopWithoutReconnect(t *testing.T) {
	var c rtcRefreshCoalescer
	started := make(chan struct{})
	var obsolete atomic.Int32
	c.request(context.Background(), func(ctx context.Context) { close(started); <-ctx.Done() })
	<-started
	c.request(context.Background(), func(context.Context) { obsolete.Add(1) })
	c.stop()
	deadline := time.Now().Add(time.Second)
	for {
		c.mu.Lock()
		running := c.running
		c.mu.Unlock()
		if !running {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("worker did not stop")
		}
		time.Sleep(time.Millisecond)
	}
	if obsolete.Load() != 0 {
		t.Fatal("pending refresh survived stop")
	}
}

func TestRTCRefreshCoalescerRecurringAndStop(t *testing.T) {
	var c rtcRefreshCoalescer
	defer c.stop()
	hits := make(chan struct{}, 8)
	c.schedule(context.Background(), func(ctx context.Context) {
		select {
		case hits <- struct{}{}:
		case <-ctx.Done():
		}
	}, 5*time.Millisecond)
	for i := 0; i < 3; i++ {
		select {
		case <-hits:
		case <-time.After(time.Second):
			t.Fatal("periodic refresh did not run")
		}
	}
	c.stop()
	deadline := time.Now().Add(time.Second)
	for {
		c.mu.Lock()
		running := c.running
		c.mu.Unlock()
		if !running {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("periodic worker did not stop")
		}
		time.Sleep(time.Millisecond)
	}
	for len(hits) > 0 {
		<-hits
	}
	select {
	case <-hits:
		t.Fatal("refresh after stop")
	case <-time.After(20 * time.Millisecond):
	}
}

func TestRTCRefreshCoalescerReconnectInterruptsInterval(t *testing.T) {
	var c rtcRefreshCoalescer
	defer c.stop()
	first := make(chan struct{})
	next := make(chan struct{})
	c.schedule(context.Background(), func(context.Context) { close(first) }, time.Hour)
	<-first
	c.schedule(context.Background(), func(context.Context) { close(next) }, time.Hour)
	select {
	case <-next:
	case <-time.After(time.Second):
		t.Fatal("reconnect waited for periodic interval")
	}
}

func TestRTCRefreshCoalescerSlowBatchDoesNotOverlap(t *testing.T) {
	var c rtcRefreshCoalescer
	defer c.stop()
	started := make(chan struct{})
	release := make(chan struct{})
	next := make(chan struct{})
	c.schedule(context.Background(), func(context.Context) { close(started); <-release }, time.Millisecond)
	<-started
	c.schedule(context.Background(), func(context.Context) { close(next) }, time.Hour)
	select {
	case <-next:
		t.Fatal("replacement overlapped current batch")
	case <-time.After(20 * time.Millisecond):
	}
	close(release)
	select {
	case <-next:
	case <-time.After(time.Second):
		t.Fatal("replacement not run")
	}
}

func TestRTCRefreshCoalescerExpiryBeforePeriodicFallback(t *testing.T) {
	cache := newMatrixRTCAvailabilityCache()
	var c rtcRefreshCoalescer
	defer c.stop()
	now := time.Now()
	cache.remember([]string{"a\x00pn"}, matrixRTCCallAvailabilityCacheEntry{matrixRTCCallAvailabilityDisabled, "server:406", now.Add(100 * time.Millisecond)}, now)
	hits := make(chan struct{}, 8)
	c.scheduleWithExpiry(context.Background(), func(ctx context.Context) { hits <- struct{}{} }, time.Hour, cache, "a")
	select {
	case <-hits:
	case <-time.After(time.Second):
		t.Fatal("initial refresh not run")
	}
	select {
	case <-hits:
	case <-time.After(2 * time.Second):
		t.Fatal("expiry did not trigger refresh")
	}
	select {
	case <-hits:
		t.Fatal("consumed expiry spun")
	case <-time.After(25 * time.Millisecond):
	}
}

func TestRTCRefreshCoalescerCacheUpdateDuringScan(t *testing.T) {
	cache := newMatrixRTCAvailabilityCache()
	var c rtcRefreshCoalescer
	defer c.stop()
	entered, release := make(chan struct{}), make(chan struct{})
	second := make(chan struct{})
	var calls atomic.Int32
	c.scheduleWithExpiry(context.Background(), func(ctx context.Context) {
		switch calls.Add(1) {
		case 1:
			close(entered)
			<-release
		case 2:
			close(second)
		}
	}, time.Hour, cache, "a")
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("initial refresh not run")
	}
	now := time.Now()
	cache.remember([]string{"a\x00pn"}, matrixRTCCallAvailabilityCacheEntry{matrixRTCCallAvailabilityDisabled, "server:406", now.Add(time.Hour)}, now)
	close(release)
	select {
	case <-second:
	case <-time.After(time.Second):
		t.Fatal("cache change during batch lost")
	}
	c.stop()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		cache.mu.Lock()
		count := len(cache.watches)
		cache.mu.Unlock()
		if count == 0 {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("stopped scanner leaked watch")
}
