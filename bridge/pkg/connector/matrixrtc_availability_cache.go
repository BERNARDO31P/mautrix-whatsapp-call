package connector

import (
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
}

func newMatrixRTCAvailabilityCache() *matrixRTCAvailabilityCache {
	return &matrixRTCAvailabilityCache{entries: make(map[string]matrixRTCCallAvailabilityCacheEntry)}
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
	return c.getLocked(keys, now)
}

func (c *matrixRTCAvailabilityCache) remember(keys []string, entry matrixRTCCallAvailabilityCacheEntry, now time.Time) matrixRTCCallAvailabilityCacheEntry {
	c.mu.Lock()
	defer c.mu.Unlock()
	if old, ok := c.getLocked(keys, now); ok && old.availability >= matrixRTCCallAvailabilityHidden && old.availability > entry.availability {
		entry = old
	}
	if !now.Before(entry.expires) {
		return entry
	}
	for _, key := range keys {
		c.entries[key] = entry
	}
	return entry
}
