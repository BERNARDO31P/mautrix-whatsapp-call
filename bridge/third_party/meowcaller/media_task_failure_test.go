package meowcaller

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

func TestMediaTaskFailureWakesReceiver(t *testing.T) {
	ctx, cancel := context.WithCancelCause(context.Background())
	defer cancel(nil)
	failure := errors.New("relay write failed")
	go failMediaTask(cancel, "audio RTP sender", failure)
	select {
	case <-ctx.Done():
		if !errors.Is(context.Cause(ctx), failure) {
			t.Fatal(context.Cause(ctx))
		}
	case <-time.After(time.Second):
		t.Fatal("receiver not notified")
	}
}

func TestMediaTaskFailurePreservesParentCancellation(t *testing.T) {
	parent, stop := context.WithCancel(context.Background())
	ctx, cancel := context.WithCancelCause(parent)
	defer cancel(nil)
	stop()
	failMediaTask(cancel, "relay", errors.New("socket closed"))
	if !errors.Is(context.Cause(ctx), context.Canceled) {
		t.Fatal(context.Cause(ctx))
	}
}

func TestMediaTaskFailureConcurrentFirstCause(t *testing.T) {
	ctx, cancel := context.WithCancelCause(context.Background())
	defer cancel(nil)
	a, b := errors.New("a"), errors.New("b")
	var wg sync.WaitGroup
	for _, err := range []error{a, b} {
		wg.Add(1)
		go func(err error) { defer wg.Done(); failMediaTask(cancel, "relay", err) }(err)
	}
	wg.Wait()
	cause := context.Cause(ctx)
	if !errors.Is(cause, a) && !errors.Is(cause, b) {
		t.Fatal(cause)
	}
	failMediaTask(cancel, "late", errors.New("late"))
	if context.Cause(ctx) != cause {
		t.Fatal("first cause replaced")
	}
}

func TestMediaTaskPacketFailure(t *testing.T) {
	ctx, cancel := context.WithCancelCause(context.Background())
	defer cancel(nil)
	failure := errors.New("write failed")
	n, err := sendMediaTaskPacket(ctx, cancel, "video", func([]byte) (int, error) { return 1, failure }, []byte{1, 2})
	if n != 1 || !errors.Is(err, failure) || !errors.Is(context.Cause(ctx), failure) {
		t.Fatal(n, err, context.Cause(ctx))
	}
	_, err = sendMediaTaskPacket(ctx, cancel, "app-data", func([]byte) (int, error) { t.Fatal("write after cancellation"); return 0, nil }, []byte{1})
	if !errors.Is(err, failure) {
		t.Fatal(err)
	}
}

func TestMediaTaskPacketSuccess(t *testing.T) {
	ctx, cancel := context.WithCancelCause(context.Background())
	defer cancel(nil)
	n, err := sendMediaTaskPacket(ctx, cancel, "app-data", func(p []byte) (int, error) { return len(p), nil }, []byte{1, 2})
	if n != 2 || err != nil || ctx.Err() != nil {
		t.Fatal(n, err, ctx.Err())
	}
}
