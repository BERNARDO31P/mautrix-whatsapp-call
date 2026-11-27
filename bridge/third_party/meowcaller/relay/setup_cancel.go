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
