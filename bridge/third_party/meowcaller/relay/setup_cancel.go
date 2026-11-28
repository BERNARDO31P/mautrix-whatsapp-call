package relay

import (
	"context"
	"io"
	"sync"
)

// watchSetupCancellation interrupts network setup by closing the underlying
// socket. Stop is idempotent and waits for a racing close before returning.
// A true result means the watcher was disarmed before it ran.
func watchSetupCancellation(ctx context.Context, socket io.Closer) func() bool {
	done := make(chan struct{})
	stop := context.AfterFunc(ctx, func() {
		defer close(done)
		_ = socket.Close()
	})
	var once sync.Once
	var disarmed bool
	return func() bool {
		once.Do(func() {
			disarmed = stop()
			if !disarmed {
				<-done
			}
		})
		return disarmed
	}
}

// abortFailedSetup closes the socket before unwinding protocol resources.
// A failed SCTP/DataChannel setup cannot rely on a functioning peer to finish
// a graceful shutdown. Interrupt blocked transport I/O before closing layers.
func abortFailedSetup(abortSocket func() error, cleanup []func() error) {
	if abortSocket != nil {
		_ = abortSocket()
	}
	for i := len(cleanup) - 1; i >= 0; i-- {
		_ = cleanup[i]()
	}
}
