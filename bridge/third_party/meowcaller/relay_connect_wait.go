package meowcaller

import (
	"context"
	"errors"
	"fmt"
	"time"
)

// Keep the cancellation classification and the original media failure, if any.
// A child deadline still reports DeadlineExceeded even with a custom cause.
func relayContextError(ctx context.Context) error {
	status := ctx.Err()
	if status == nil {
		return nil
	}
	cause := context.Cause(ctx)
	if cause == nil {
		return status
	}
	if errors.Is(cause, status) {
		return cause
	}
	return errors.Join(status, cause)
}

// awaitRelayConnection transfers ownership only to an active waiter. A late
// result is disposed by the producer, even if the underlying dial ignores
// cancellation. The caller owns the resource only after a successful return.
func awaitRelayConnection[T any](ctx context.Context, connect func() (T, error), dispose func(T)) (T, error) {
	type result struct {
		value T
		err   error
	}
	var zero T
	if err := relayContextError(ctx); err != nil {
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
		if err := relayContextError(ctx); err != nil {
			dispose(r.value)
			return zero, err
		}
		if r.err != nil {
			dispose(r.value)
			return zero, r.err
		}
		return r.value, nil
	case <-ctx.Done():
		return zero, relayContextError(ctx)
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

// tryRelayCandidates keeps the reason for every failed endpoint. Cancellation
// is terminal even when the last transport returns a different error.
func tryRelayCandidates[T, C any](ctx context.Context, candidates []C, attempt func(C, time.Duration) (T, error), report func(int, C, error), dispose func(T)) (T, error) {
	var zero T
	if len(candidates) == 0 {
		return zero, fmt.Errorf("relay has no usable endpoint")
	}
	failures := make([]error, 0, len(candidates)+1)
	for i, candidate := range candidates {
		if err := relayContextError(ctx); err != nil {
			return zero, errors.Join(append(failures, err)...)
		}
		value, err := attempt(candidate, relayAttemptTimeout(len(candidates)))
		if err == nil {
			// A successful attempt transfers ownership only while the call
			// is still active. Otherwise close the entire channel, not just
			// the socket interrupted by its lifetime watcher.
			if err := relayContextError(ctx); err != nil {
				dispose(value)
				return zero, errors.Join(append(failures, err)...)
			}
			return value, nil
		}
		failures = append(failures, fmt.Errorf("relay candidate %d: %w", i+1, err))
		report(i+1, candidate, err)
	}
	if err := relayContextError(ctx); err != nil {
		failures = append(failures, err)
	}
	return zero, fmt.Errorf("relay setup exhausted: %w", errors.Join(failures...))
}

// dialDirectRelayPorts preserves the signalled port first, but permits one
// alternate web transport port before handing a responsive channel to media.
// A failed attempt must close its channel before returning.
// Group media keeps its original elected transport endpoint.
func dialDirectRelayPorts[T any](ctx context.Context, advertised uint16, group bool, dial func(context.Context, uint16) (T, error)) (T, uint16, error) {
	var zero T
	if err := relayContextError(ctx); err != nil {
		return zero, advertised, err
	}
	if advertised == 0 {
		return zero, advertised, fmt.Errorf("invalid advertised relay port: 0")
	}
	const webRelayPort uint16 = 3480
	if group || advertised == webRelayPort {
		v, err := dial(ctx, advertised)
		return v, advertised, err
	}
	// The enclosing endpoint deadline is shared; reserve half for the fallback.
	firstBudget := 5 * time.Second
	if deadline, ok := ctx.Deadline(); ok {
		if half := time.Until(deadline) / 2; half < firstBudget {
			firstBudget = half
		}
	}
	if firstBudget <= 0 {
		return zero, advertised, context.DeadlineExceeded
	}
	firstCtx, cancel := context.WithTimeout(ctx, firstBudget)
	v, firstErr := dial(firstCtx, advertised)
	cancel()
	if firstErr == nil {
		return v, advertised, nil
	}
	if err := relayContextError(ctx); err != nil {
		return zero, advertised, errors.Join(firstErr, err)
	}
	v, err := dial(ctx, webRelayPort)
	if err != nil {
		return zero, webRelayPort, errors.Join(fmt.Errorf("advertised relay port %d: %w; alternate relay port %d: %w", advertised, firstErr, webRelayPort, err), relayContextError(ctx))
	}
	return v, webRelayPort, nil
}

// awaitRelayPackets preserves early packets in arrival order. A consent pong or
// an unrelated STUN response alone cannot confirm allocation. The caller decides
// which packets permit handoff and which are explicit allocation failures.
func awaitRelayPackets(ctx context.Context, recv func([]byte) (int, error), watch func(context.Context) func() bool, ready func([]byte) (bool, error)) ([][]byte, error) {
	readCtx, cancel := context.WithTimeout(ctx, 900*time.Millisecond)
	defer cancel()
	stop := watch(readCtx)
	defer stop()
	if err := relayContextError(readCtx); err != nil {
		return nil, err
	}
	const maxPackets = 32
	packets := make([][]byte, 0, 4)
	for len(packets) < maxPackets {
		buf := make([]byte, 1500)
		n, err := recv(buf)
		if ctxErr := relayContextError(readCtx); ctxErr != nil {
			return nil, ctxErr
		}
		if err != nil {
			return nil, err
		}
		if n <= 0 || n > len(buf) {
			return nil, fmt.Errorf("invalid initial relay packet length %d", n)
		}
		packet := buf[:n]
		ok, err := ready(packet)
		if err != nil {
			return nil, err
		}
		packets = append(packets, packet)
		if ok {
			if !stop() || relayContextError(readCtx) != nil {
				if err := relayContextError(readCtx); err != nil {
					return nil, err
				}
				return nil, context.Canceled
			}
			return packets, nil
		}
	}
	return nil, fmt.Errorf("direct relay allocation unconfirmed after %d packets", maxPackets)
}
