package meowcaller

import (
	"context"
	"testing"
	"time"
)

func waitDispatch[T any](t *testing.T, ch <-chan T) T {
	t.Helper()
	select {
	case v := <-ch:
		return v
	case <-time.After(3 * time.Second):
		t.Fatal("dispatch timeout")
		var zero T
		return zero
	}
}

func TestCallDispatchFIFOAndBound(t *testing.T) {
	var q callDispatchQueue
	started, release := make(chan struct{}), make(chan struct{})
	result := make(chan int, callDispatchQueueLimit)
	ctx := context.Background()
	if !q.submit(ctx, func() { close(started); <-release }) {
		t.Fatal("initial job rejected")
	}
	waitDispatch(t, started)
	for i := 0; i < callDispatchQueueLimit; i++ {
		n := i
		if !q.submit(ctx, func() { result <- n }) {
			t.Fatalf("job %d rejected", i)
		}
	}
	if q.submit(ctx, func() {}) {
		t.Fatal("queue exceeded bound")
	}
	close(release)
	for i := 0; i < callDispatchQueueLimit; i++ {
		if got := waitDispatch(t, result); got != i {
			t.Fatalf("order %d != %d", got, i)
		}
	}
}

func TestCallDispatchDiscardAndCancellation(t *testing.T) {
	var q callDispatchQueue
	started, release := make(chan struct{}), make(chan struct{})
	result := make(chan int, 3)
	ctx := context.Background()
	q.submit(ctx, func() { close(started); <-release })
	waitDispatch(t, started)
	q.submit(ctx, func() { result <- 1 })
	q.discardPending()
	cancelCtx, cancel := context.WithCancel(ctx)
	q.submit(cancelCtx, func() { result <- 2 })
	cancel()
	if q.submit(cancelCtx, func() {}) {
		t.Fatal("cancelled job admitted")
	}
	q.submit(ctx, func() { result <- 3 })
	close(release)
	if got := waitDispatch(t, result); got != 3 {
		t.Fatalf("discarded job executed: %d", got)
	}
	select {
	case got := <-result:
		t.Fatalf("unexpected job %d", got)
	default:
	}
}
