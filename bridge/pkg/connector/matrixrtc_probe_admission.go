package connector

import (
	"context"
	"sync"
)

// Limit both active and waiting probes before creating any goroutine.
// A rejected request is not marked pending, so a later portal view can retry.
type rtcProbeAdmission struct {
	mu                     sync.Mutex
	pending                map[string]string
	perLogin               map[string]int
	slots                  chan struct{}
	totalLimit, loginLimit int
}

func newRTCProbeAdmission(total, perLogin, active int) *rtcProbeAdmission {
	return &rtcProbeAdmission{pending: make(map[string]string), perLogin: make(map[string]int), slots: make(chan struct{}, active), totalLimit: total, loginLimit: perLogin}
}

func (a *rtcProbeAdmission) admit(login, key string) (func(), bool) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if login == "" || key == "" || len(a.pending) >= a.totalLimit || a.perLogin[login] >= a.loginLimit {
		return nil, false
	}
	if _, exists := a.pending[key]; exists {
		return nil, false
	}
	a.pending[key] = login
	a.perLogin[login]++
	var once sync.Once
	return func() {
		once.Do(func() {
			a.mu.Lock()
			defer a.mu.Unlock()
			delete(a.pending, key)
			a.perLogin[login]--
			if a.perLogin[login] == 0 {
				delete(a.perLogin, login)
			}
		})
	}, true
}

func (a *rtcProbeAdmission) acquire(ctx context.Context) bool {
	select {
	case <-ctx.Done():
		return false
	case a.slots <- struct{}{}:
		if ctx.Err() != nil {
			a.releaseSlot()
			return false
		}
		return true
	}
}

func (a *rtcProbeAdmission) releaseSlot() { <-a.slots }

// A connection owns a cancellation scope for background capability probes.
// Replacement cancels all admitted work from the previous connection, including
// waiters that have not yet acquired a global probe slot.
type rtcProbeLifetime struct {
	mu     sync.Mutex
	ctx    context.Context
	cancel context.CancelFunc
}

func (l *rtcProbeLifetime) start(parent context.Context) context.Context {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.cancel != nil {
		l.cancel()
	}
	l.ctx, l.cancel = context.WithCancel(parent)
	return l.ctx
}

func (l *rtcProbeLifetime) current() context.Context {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.ctx
}

func (l *rtcProbeLifetime) stop() {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.cancel != nil {
		l.cancel()
	}
	l.ctx, l.cancel = nil, nil
}
