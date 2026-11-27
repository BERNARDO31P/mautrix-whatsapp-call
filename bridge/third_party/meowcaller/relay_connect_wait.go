package meowcaller

import "context"

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
