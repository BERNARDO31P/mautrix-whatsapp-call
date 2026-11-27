package meowcaller

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestRelayConnectLateResultDisposed(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	started, release, disposed := make(chan struct{}), make(chan struct{}), make(chan int, 1)
	result := make(chan error, 1)
	go func() {
		_, err := awaitRelayConnection(ctx, func() (int, error) {
			close(started)
			<-release
			return 42, nil
		}, func(v int) { disposed <- v })
		result <- err
	}()
	<-started
	cancel()
	if err := <-result; !errors.Is(err, context.Canceled) {
		t.Fatalf("unexpected result: %v", err)
	}
	close(release)
	select {
	case v := <-disposed:
		if v != 42 {
			t.Fatalf("wrong disposed result: %d", v)
		}
	case <-time.After(time.Second):
		t.Fatal("abandoned connection not disposed")
	}
}

func TestRelayConnectTransfersOwnership(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	disposed := make(chan int, 1)
	v, err := awaitRelayConnection(ctx, func() (int, error) { return 42, nil }, func(v int) { disposed <- v })
	if v != 42 || err != nil {
		t.Fatalf("value=%d, err=%v", v, err)
	}
	cancel()
	select {
	case <-disposed:
		t.Fatal("transferred connection disposed by producer")
	default:
	}
}

func TestRelayConnectFailureDisposesPartialResult(t *testing.T) {
	want := errors.New("failed")
	disposed := 0
	v, err := awaitRelayConnection(context.Background(), func() (int, error) { return 42, want }, func(v int) { disposed = v })
	if v != 0 || !errors.Is(err, want) || disposed != 42 {
		t.Fatalf("value=%d, err=%v, disposed=%d", v, err, disposed)
	}
}

func TestRelayConnectAlreadyCancelled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	called := false
	_, err := awaitRelayConnection(ctx, func() (int, error) { called = true; return 42, nil }, func(int) {})
	if called || !errors.Is(err, context.Canceled) {
		t.Fatalf("called=%v, err=%v", called, err)
	}
}
