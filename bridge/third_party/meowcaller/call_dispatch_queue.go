package meowcaller

import (
	"context"
	"sync"
)

// Call work may perform network or crypto operations. Keep it out of the
// provider's shared node dispatcher, while preserving call/ACK arrival order.
const callDispatchQueueLimit = 128

type callDispatchJob struct {
	ctx context.Context
	run func()
}

type callDispatchQueue struct {
	mu      sync.Mutex
	pending []callDispatchJob
	running bool
}

// submit never waits for a running job. An idle queue has no resident goroutine.
func (q *callDispatchQueue) submit(ctx context.Context, run func()) bool {
	q.mu.Lock()
	defer q.mu.Unlock()
	if ctx.Err() != nil || len(q.pending) >= callDispatchQueueLimit {
		return false
	}
	q.pending = append(q.pending, callDispatchJob{ctx: ctx, run: run})
	if !q.running {
		q.running = true
		go q.drain()
	}
	return true
}

// Invalidate queued work on disconnect without starting a competing worker.
// A job already executing remains responsible for its own cancellation.
func (q *callDispatchQueue) discardPending() {
	q.mu.Lock()
	clear(q.pending)
	q.pending = nil
	q.mu.Unlock()
}

func (q *callDispatchQueue) drain() {
	for {
		q.mu.Lock()
		if len(q.pending) == 0 {
			q.pending = nil
			q.running = false
			q.mu.Unlock()
			return
		}
		job := q.pending[0]
		q.pending[0] = callDispatchJob{}
		q.pending = q.pending[1:]
		q.mu.Unlock()
		if job.ctx.Err() == nil {
			job.run()
		}
	}
}
