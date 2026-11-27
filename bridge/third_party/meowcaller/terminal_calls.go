package meowcaller

import "time"

const (
	endedCallRetention = 3 * time.Minute
	maxEndedCalls      = 4096
)

// rememberEndedCall is called with the engine lock held. Duplicate terminal
// events do not extend the original reservation. Expired entries are reclaimed
// before the oldest reservation is evicted when the bounded registry is full.
func rememberEndedCall(ended map[string]time.Time, callID string, now time.Time) map[string]time.Time {
	if callID == "" {
		return ended
	}
	if ended == nil {
		ended = make(map[string]time.Time)
	}
	if now.Before(ended[callID]) {
		return ended
	}
	var oldestID string
	var oldestExpiry time.Time
	for id, expiry := range ended {
		if !now.Before(expiry) {
			delete(ended, id)
			continue
		}
		if oldestID == "" || expiry.Before(oldestExpiry) || (expiry.Equal(oldestExpiry) && id < oldestID) {
			oldestID, oldestExpiry = id, expiry
		}
	}
	if len(ended) >= maxEndedCalls {
		delete(ended, oldestID)
	}
	ended[callID] = now.Add(endedCallRetention)
	return ended
}
