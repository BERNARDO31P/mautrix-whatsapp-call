package meowcaller

import (
	"context"
	"errors"
	"fmt"
)

// ErrPickupNotSubmitted means this invocation did not submit an accept. The
// caller still needs to check that the original call remains current before
// trying again. It does not describe an earlier, already submitted attempt.
var ErrPickupNotSubmitted = errors.New("meowcaller: pickup not submitted")

// ErrPickupOutcomeUnknown means the accept was handed to the provider but
// successful remote acceptance was not established. Never retry that accept.
var ErrPickupOutcomeUnknown = errors.New("meowcaller: pickup outcome unknown")

func pickupNotSubmitted(err error) error {
	return fmt.Errorf("%w: %w", ErrPickupNotSubmitted, err)
}

func pickupOutcomeUnknown(err error) error {
	return fmt.Errorf("%w: %w", ErrPickupOutcomeUnknown, err)
}

// waitPickupSettlement orders local terminal signalling after the accept
// submission has returned. Cancellation does not prove that an accept was
// never written, and timing out must not permit an out-of-order terminate.
func waitPickupSettlement(ctx context.Context, done <-chan struct{}) error {
	if done == nil {
		return nil
	}
	select {
	case <-done:
		return nil
	default:
	}
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return pickupOutcomeUnknown(ctx.Err())
	}
}
