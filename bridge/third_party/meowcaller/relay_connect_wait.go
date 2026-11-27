package meowcaller

import (
	"context"
	"time"
)

// awaitRelayConnection transfers ownership only to an active waiter. A late
// result is disposed by the producer, even if the underlying dial ignores
// cancellation. The caller owns the resource only after a successful return.
func awaitRelayConnection[T any](ctx context.Context, connect func() (T, error), dispose func(T)) (T, error) {
	type result struct {
		value T
		err   error
	}
	var zero T
	if err := ctx.Err(); err != nil {
		return zero, err
	}
	done := make(chan result)
	go func() {
		value, err := connect()
		select {
		case done <- result{value, err}:
		case <-ctx.Done():
			dispose(value)
		}
	}()
	select {
	case r := <-done:
		if err := ctx.Err(); err != nil {
			dispose(r.value)
			return zero, err
		}
		if r.err != nil {
			dispose(r.value)
			return zero, r.err
		}
		return r.value, nil
	case <-ctx.Done():
		return zero, ctx.Err()
	}
}

// Direct relay attempts share a 24-second budget. Keep the original maximum
// for a single candidate (and the authoritative group relay), but leave time
// for the last direct candidate before the provider's call timeout.
func relayAttemptTimeout(candidateCount int) time.Duration {
	const maximum = 12 * time.Second
	const budget = 24 * time.Second
	if candidateCount <= 2 {
		return maximum
	}
	return budget / time.Duration(candidateCount)
}
