package relay

import (
	"context"
	"errors"
	"net"
	"reflect"
	"testing"
	"time"
)

func TestSetupCancellationInterruptsSocketRead(t *testing.T) {
	socket, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	defer socket.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	stop := watchSetupCancellation(ctx, socket)
	result := make(chan error, 1)
	go func() { _, _, err := socket.ReadFromUDP(make([]byte, 1)); result <- err }()
	cancel()
	select {
	case err := <-result:
		if err == nil {
			t.Fatal("read unexpectedly succeeded")
		}
	case <-time.After(time.Second):
		t.Fatal("socket read not interrupted")
	}
	if stop() || stop() {
		t.Fatal("cancelled watcher reported disarmed")
	}
}

func TestSetupDisarmPreservesTransferredSocket(t *testing.T) {
	socket, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	defer socket.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	stop := watchSetupCancellation(ctx, socket)
	if !stop() || !stop() {
		t.Fatal("watcher not disarmed")
	}
	cancel()
	if err := socket.SetReadDeadline(time.Now()); err != nil {
		t.Fatal(err)
	}
}

func TestSetupAndCallCancellationHaveSeparateLifetimes(t *testing.T) {
	socket, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	defer socket.Close()
	callCtx, cancelCall := context.WithCancel(context.Background())
	defer cancelCall()
	setupCtx, cancelSetup := context.WithCancel(callCtx)
	defer cancelSetup()
	stopCall := watchSetupCancellation(callCtx, socket)
	defer stopCall()
	stopSetup := watchSetupCancellation(setupCtx, socket)
	defer stopSetup()
	if !stopSetup() {
		t.Fatal("setup watcher did not disarm")
	}
	cancelSetup()
	if err := socket.SetReadDeadline(time.Time{}); err != nil {
		t.Fatalf("setup cancellation closed live socket: %v", err)
	}
	result := make(chan error, 1)
	go func() { _, _, err := socket.ReadFromUDP(make([]byte, 1)); result <- err }()
	cancelCall()
	select {
	case err := <-result:
		if err == nil {
			t.Fatal("call cancellation did not interrupt read")
		}
	case <-time.After(time.Second):
		t.Fatal("call cancellation left read blocked")
	}
	if stopCall() {
		t.Fatal("call watcher incorrectly reported disarmed")
	}
}

func TestAbortFailedSetupInterruptsIOBeforeProtocolCleanup(t *testing.T) {
	socket, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	defer socket.Close()
	var order []string
	done := make(chan struct{})
	go func() {
		defer close(done)
		abortFailedSetup(func() error {
			order = append(order, "socket")
			return socket.Close()
		}, []func() error{
			func() error { order = append(order, "dtls"); return nil },
			func() error {
				_, _, err := socket.ReadFromUDP(make([]byte, 1))
				if !errors.Is(err, net.ErrClosed) {
					t.Errorf("read error = %v", err)
				}
				order = append(order, "sctp")
				return errors.New("protocol cleanup failed")
			},
		})
	}()
	select {
	case <-done:
		if !reflect.DeepEqual(order, []string{"socket", "sctp", "dtls"}) {
			t.Fatalf("cleanup order: %v", order)
		}
	case <-time.After(time.Second):
		t.Fatal("protocol cleanup blocked on socket")
	}
}

func TestAbortFailedSetupBeforeSocketExists(t *testing.T) {
	abortFailedSetup(nil, nil)
}
