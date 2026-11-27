package connector

import (
	"context"
	"sync"
	"time"
)

// A single producer acquires capacity before spawning work. No queued goroutine
// is needed for each portal. The producer must stop submitting before wait.
type rtcPortalRefreshBatch struct {
	ctx   context.Context
	slots chan struct{}
	wg    sync.WaitGroup
}

func newRTCPortalRefreshBatch(ctx context.Context, limit int) *rtcPortalRefreshBatch {
	return &rtcPortalRefreshBatch{ctx: ctx, slots: make(chan struct{}, limit)}
}

func (b *rtcPortalRefreshBatch) submit(fn func()) bool {
	select {
	case <-b.ctx.Done():
		return false
	case b.slots <- struct{}{}:
	}
	if b.ctx.Err() != nil {
		<-b.slots
		return false
	}
	b.wg.Add(1)
	go func() {
		defer b.wg.Done()
		defer func() { <-b.slots }()
		if b.ctx.Err() == nil {
			fn()
		}
	}()
	return true
}

func (b *rtcPortalRefreshBatch) wait() { b.wg.Wait() }

// A client owns one reconnect/periodic refresh worker. Repeated notifications
// replace pending work and interrupt the idle interval, without overlapping batches.
type rtcRefreshCoalescer struct {
	mu      sync.Mutex
	running bool
	pending *rtcRefreshRequest
	cancel  context.CancelFunc
	wake    chan struct{}
}

type rtcRefreshRequest struct {
	ctx      context.Context
	fn       func(context.Context)
	interval time.Duration
	cache    *matrixRTCAvailabilityCache
	login    string
}

func (c *rtcRefreshCoalescer) request(ctx context.Context, fn func(context.Context)) {
	c.schedule(ctx, fn, 0)
}

func (c *rtcRefreshCoalescer) schedule(ctx context.Context, fn func(context.Context), interval time.Duration) {
	c.scheduleRequest(&rtcRefreshRequest{ctx: ctx, fn: fn, interval: interval})
}

func (c *rtcRefreshCoalescer) scheduleWithExpiry(ctx context.Context, fn func(context.Context), interval time.Duration, cache *matrixRTCAvailabilityCache, login string) {
	c.scheduleRequest(&rtcRefreshRequest{ctx: ctx, fn: fn, interval: interval, cache: cache, login: login})
}

func (c *rtcRefreshCoalescer) scheduleRequest(req *rtcRefreshRequest) {
	c.mu.Lock()
	c.pending = req
	if c.wake != nil {
		select {
		case c.wake <- struct{}{}:
		default:
		}
	}
	if c.running {
		c.mu.Unlock()
		return
	}
	c.running = true
	c.mu.Unlock()
	go c.run()
}

// Drop queued work and cancel the active batch. Keep the worker reserved
// until it exits, so a subsequent reconnect cannot overlap the old batch.
func (c *rtcRefreshCoalescer) stop() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.pending = nil
	if c.cancel != nil {
		c.cancel()
	}
}

func (c *rtcRefreshCoalescer) run() {
	for {
		c.mu.Lock()
		req := c.pending
		c.pending = nil
		if req == nil {
			c.running = false
			c.mu.Unlock()
			return
		}
		ctx, cancel := context.WithCancel(req.ctx)
		c.cancel = cancel
		wake := make(chan struct{}, 1)
		c.wake = wake
		c.mu.Unlock()
		var watch *rtcAvailabilityWatch
		if req.cache != nil && req.login != "" {
			watch = req.cache.watch(req.login)
		}
		for ctx.Err() == nil {
			if watch != nil {
				watch.begin(time.Now())
			}
			req.fn(ctx)
			if req.interval <= 0 || ctx.Err() != nil {
				break
			}
			if !waitRTCRefreshExpiry(ctx, wake, watch, req.interval) {
				break
			}
			// A reconnect may have arrived simultaneously with the timer.
			c.mu.Lock()
			replaced := c.pending != nil
			c.mu.Unlock()
			if replaced {
				break
			}
		}
		if watch != nil {
			watch.close()
		}
		cancel()
		c.mu.Lock()
		c.cancel = nil
		c.wake = nil
		c.mu.Unlock()
	}
}

// Wait from the end of a batch rather than accumulating periodic work while
// a slow batch runs. Stop/reconnect can interrupt the wait immediately.
func waitRTCRefreshInterval(ctx context.Context, wake <-chan struct{}, interval time.Duration) bool {
	timer := time.NewTimer(interval)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-wake:
		return false
	case <-timer.C:
		return ctx.Err() == nil
	}
}

// A periodic scan remains a fallback for newly discovered portals and failed
// lookups. Cache writes can move the next scan earlier; a reconnect or stop
// interrupts the timer without starting overlapping batches.
func waitRTCRefreshExpiry(ctx context.Context, wake <-chan struct{}, watch *rtcAvailabilityWatch, interval time.Duration) bool {
	if watch == nil {
		return waitRTCRefreshInterval(ctx, wake, interval)
	}
	fallback := time.Now().Add(interval)
	for ctx.Err() == nil {
		expiry, dirty := watch.next()
		if dirty {
			return ctx.Err() == nil
		}
		deadline := fallback
		if !expiry.IsZero() && expiry.Before(deadline) {
			deadline = expiry
		}
		timer := time.NewTimer(time.Until(deadline))
		select {
		case <-ctx.Done():
			timer.Stop()
			return false
		case <-wake:
			timer.Stop()
			return false
		case <-watch.signal:
			timer.Stop()
		case <-timer.C:
			return ctx.Err() == nil
		}
	}
	return false
}
