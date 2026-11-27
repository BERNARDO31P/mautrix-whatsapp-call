package meowcaller

import (
	"context"
	"fmt"
)

// failMediaTask propagates a fatal child write error to the media receive loop.
// The first cancellation cause wins, including cancellation by the call owner.
func failMediaTask(cancel context.CancelCauseFunc, task string, err error) {
	if err != nil {
		cancel(fmt.Errorf("%s: %w", task, err))
	}
}

// sendMediaTaskPacket prevents new writes after cancellation and forwards transport
// failures to the owning media session, without changing the first cause.
func sendMediaTaskPacket(ctx context.Context, cancel context.CancelCauseFunc, task string, send func([]byte) (int, error), packet []byte) (int, error) {
	if ctx != nil && ctx.Err() != nil {
		return 0, context.Cause(ctx)
	}
	n, err := send(packet)
	if cancel != nil {
		failMediaTask(cancel, task, err)
	}
	return n, err
}
