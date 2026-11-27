package connector

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestRTCProbeAdmissionBounds(t *testing.T) {
	a := newRTCProbeAdmission(4, 2, 1)
	r1, ok := a.admit("a", "a:1")
	if !ok {
		t.Fatal("first")
	}
	if _, ok := a.admit("a", "a:1"); ok {
		t.Fatal("duplicate")
	}
	r2, ok := a.admit("a", "a:2")
	if !ok {
		t.Fatal("second")
	}
	if _, ok := a.admit("a", "a:3"); ok {
		t.Fatal("per-login limit")
	}
	r3, ok := a.admit("b", "b:1")
	if !ok {
		t.Fatal("third")
	}
	r4, ok := a.admit("b", "b:2")
	if !ok {
		t.Fatal("fourth")
	}
	if _, ok := a.admit("c", "c:1"); ok {
		t.Fatal("global limit")
	}
	r1()
	r1()
	r5, ok := a.admit("c", "c:1")
	if !ok {
		t.Fatal("capacity not restored")
	}
	r2()
	r3()
	r4()
	r5()
	if len(a.pending) != 0 || len(a.perLogin) != 0 {
		t.Fatal("retained admission")
	}
}

func TestRTCProbeAdmissionCancellation(t *testing.T) {
	a := newRTCProbeAdmission(4, 2, 1)
	if !a.acquire(context.Background()) {
		t.Fatal("slot")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if a.acquire(ctx) {
		t.Fatal("cancelled waiter admitted")
	}
	a.releaseSlot()
	if a.acquire(ctx) {
		t.Fatal("cancelled caller took free slot")
	}
	if !a.acquire(context.Background()) {
		t.Fatal("slot leaked")
	}
	a.releaseSlot()
}

func TestRTCProbeAdmissionConcurrent(t *testing.T) {
	a := newRTCProbeAdmission(64, 16, 4)
	var wg sync.WaitGroup
	var count atomic.Int32
	var mu sync.Mutex
	var releases []func()
	for i := 0; i < 1000; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			if release, ok := a.admit(fmt.Sprint(i%10), fmt.Sprint(i)); ok {
				count.Add(1)
				mu.Lock()
				releases = append(releases, release)
				mu.Unlock()
			}
		}(i)
	}
	wg.Wait()
	if count.Load() != 64 {
		t.Fatalf("admitted %d", count.Load())
	}
	for _, r := range releases {
		r()
	}
	if len(a.pending) != 0 || len(a.perLogin) != 0 {
		t.Fatal("retained admission")
	}
}

func TestRTCProbeLifetimeReplacement(t *testing.T) {
	var lifetime rtcProbeLifetime
	if lifetime.current() != nil {
		t.Fatal("scope before connect")
	}
	first := lifetime.start(context.Background())
	if lifetime.current() != first || first.Err() != nil {
		t.Fatal("invalid first scope")
	}
	second := lifetime.start(context.Background())
	if first.Err() != context.Canceled || second.Err() != nil || lifetime.current() != second {
		t.Fatal("replacement did not retire old scope")
	}
	lifetime.stop()
	lifetime.stop()
	if lifetime.current() != nil || second.Err() != context.Canceled {
		t.Fatal("scope survived stop")
	}
}

func TestRTCProbeLifetimeCancelsQueuedProbe(t *testing.T) {
	a := newRTCProbeAdmission(4, 4, 1)
	if !a.acquire(context.Background()) {
		t.Fatal("slot")
	}
	defer a.releaseSlot()
	var lifetime rtcProbeLifetime
	scope := lifetime.start(context.Background())
	defer lifetime.stop()
	release, ok := a.admit("login", "peer")
	if !ok {
		t.Fatal("admission")
	}
	done := make(chan bool, 1)
	go func() {
		acquired := a.acquire(scope)
		if acquired {
			a.releaseSlot()
		}
		release()
		done <- acquired
	}()
	lifetime.stop()
	select {
	case acquired := <-done:
		if acquired {
			t.Fatal("obsolete work admitted")
		}
	case <-time.After(time.Second):
		t.Fatal("waiter survived disconnect")
	}
	again, ok := a.admit("login", "peer")
	if !ok {
		t.Fatal("admission leaked")
	}
	again()
}

func TestRTCProbeLifetimeCancelsActiveProbe(t *testing.T) {
	var lifetime rtcProbeLifetime
	parent, cancel := context.WithCancel(context.Background())
	defer cancel()
	scope := lifetime.start(parent)
	a := newRTCProbeAdmission(4, 4, 1)
	if !a.acquire(scope) {
		t.Fatal("slot")
	}
	started, done := make(chan struct{}), make(chan struct{})
	go func() {
		close(started)
		<-scope.Done()
		a.releaseSlot()
		close(done)
	}()
	<-started
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("active probe not cancelled")
	}
	lifetime.stop()
	newScope := lifetime.start(context.Background())
	defer lifetime.stop()
	if !a.acquire(newScope) {
		t.Fatal("slot leaked")
	}
	a.releaseSlot()
}
