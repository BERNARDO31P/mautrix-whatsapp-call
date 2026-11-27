package connector

import (
	"context"
	"strings"
	"sync"
	"time"
)

type matrixRTCCallAvailability uint8

const (
	matrixRTCCallAvailabilityUnknown matrixRTCCallAvailability = iota
	matrixRTCCallAvailabilityEnabled
	matrixRTCCallAvailabilityHidden
	matrixRTCCallAvailabilityDisabled
)

type matrixRTCCallAvailabilityCacheEntry struct {
	availability matrixRTCCallAvailability
	source       string
	expires      time.Time
}

var matrixRTCCallAvailabilityCache = newMatrixRTCAvailabilityCache()

// Alias updates are atomic: a late positive/unknown probe must not erase a
// still-valid negative from a business response or server:406. Expiry allows
// the real capability to change without permanently blacklisting a contact.
type matrixRTCAvailabilityCache struct {
	mu        sync.Mutex
	entries   map[string]matrixRTCCallAvailabilityCacheEntry
	lastPrune time.Time
	watches   map[string]*rtcAvailabilityWatch
}

func newMatrixRTCAvailabilityCache() *matrixRTCAvailabilityCache {
	return &matrixRTCAvailabilityCache{entries: make(map[string]matrixRTCCallAvailabilityCacheEntry), watches: make(map[string]*rtcAvailabilityWatch)}
}

func (c *matrixRTCAvailabilityCache) getLocked(keys []string, now time.Time) (best matrixRTCCallAvailabilityCacheEntry, found bool) {
	if c.lastPrune.IsZero() || now.Sub(c.lastPrune) >= time.Minute {
		for key, entry := range c.entries {
			if !now.Before(entry.expires) {
				delete(c.entries, key)
			}
		}
		c.lastPrune = now
	}
	for _, key := range keys {
		entry, ok := c.entries[key]
		if !ok {
			continue
		}
		if !now.Before(entry.expires) {
			delete(c.entries, key)
			continue
		}
		if !found || entry.availability > best.availability || (entry.availability == best.availability && entry.expires.After(best.expires)) {
			best, found = entry, true
		}
	}
	return
}

func (c *matrixRTCAvailabilityCache) get(keys []string, now time.Time) (matrixRTCCallAvailabilityCacheEntry, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	best, found := c.getLocked(keys, now)
	if found {
		// The portal projection may only know one alias, whereas the dial
		// gate resolves all of them. Publish the winning observation to
		// every resolved alias without renewing its original expiry.
		for _, key := range keys {
			c.storeLocked(key, best, now)
		}
	}
	return best, found
}

func (c *matrixRTCAvailabilityCache) remember(keys []string, entry matrixRTCCallAvailabilityCacheEntry, now time.Time) matrixRTCCallAvailabilityCacheEntry {
	return c.rememberContext(context.Background(), keys, entry, now)
}

func (c *matrixRTCAvailabilityCache) rememberContext(ctx context.Context, keys []string, entry matrixRTCCallAvailabilityCacheEntry, now time.Time) matrixRTCCallAvailabilityCacheEntry {
	c.mu.Lock()
	defer c.mu.Unlock()
	if ctx.Err() != nil {
		return matrixRTCCallAvailabilityCacheEntry{}
	}
	if old, ok := c.getLocked(keys, now); ok && old.availability >= matrixRTCCallAvailabilityHidden && old.availability > entry.availability {
		entry = old
	}
	if !now.Before(entry.expires) {
		return entry
	}
	for _, key := range keys {
		c.storeLocked(key, entry, now)
	}
	return entry
}

// One connection-owned watch per login. The earliest expiry is retained even
// when a different caller prunes the expired cache entry before the timer fires.
// A policy change wakes the same bounded portal scanner, not a per-peer task.
type rtcAvailabilityWatch struct {
	cache    *matrixRTCAvailabilityCache
	login    string
	signal   chan struct{}
	deadline time.Time
	dirty    bool
}

func (c *matrixRTCAvailabilityCache) watch(login string) *rtcAvailabilityWatch {
	c.mu.Lock()
	defer c.mu.Unlock()
	w := &rtcAvailabilityWatch{cache: c, login: login, signal: make(chan struct{}, 1)}
	c.watches[login] = w
	return w
}

func (w *rtcAvailabilityWatch) close() {
	w.cache.mu.Lock()
	defer w.cache.mu.Unlock()
	if w.cache.watches[w.login] == w {
		delete(w.cache.watches, w.login)
	}
}

// Called immediately before a complete bounded portal scan. Any later expiry
// or policy change remains pending even if it happens during the scan.
func (w *rtcAvailabilityWatch) begin(now time.Time) {
	c := w.cache
	c.mu.Lock()
	defer c.mu.Unlock()
	w.deadline, w.dirty = time.Time{}, false
	select {
	case <-w.signal:
	default:
	}
	prefix := w.login + "\x00"
	for key, entry := range c.entries {
		if strings.HasPrefix(key, prefix) && now.Before(entry.expires) && (w.deadline.IsZero() || entry.expires.Before(w.deadline)) {
			w.deadline = entry.expires
		}
	}
}

func (w *rtcAvailabilityWatch) next() (time.Time, bool) {
	w.cache.mu.Lock()
	defer w.cache.mu.Unlock()
	return w.deadline, w.dirty
}

func (c *matrixRTCAvailabilityCache) storeLocked(key string, entry matrixRTCCallAvailabilityCacheEntry, now time.Time) {
	old, exists := c.entries[key]
	c.entries[key] = entry
	login, _, ok := strings.Cut(key, "\x00")
	if !ok {
		return
	}
	w := c.watches[login]
	if w == nil {
		return
	}
	changed := !exists || !now.Before(old.expires) || old.availability != entry.availability
	earlier := w.deadline.IsZero() || entry.expires.Before(w.deadline)
	if earlier {
		w.deadline = entry.expires
	}
	if changed {
		w.dirty = true
	}
	if changed || earlier {
		select {
		case w.signal <- struct{}{}:
		default:
		}
	}
}

// Explicit provider refusals and cached/static dial denials can request a
// reconciliation without starting a detached permission-writing goroutine.
// A disconnected login has no watcher; reconnect always performs a full scan.
func (c *matrixRTCAvailabilityCache) requestReconcile(login string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if w := c.watches[login]; w != nil {
		w.dirty = true
		select {
		case w.signal <- struct{}{}:
		default:
		}
	}
}
