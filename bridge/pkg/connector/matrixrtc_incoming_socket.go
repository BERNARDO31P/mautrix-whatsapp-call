package connector

import "os"

// This is only a negative admission check, not a worker health probe. A stale
// socket can still fail on dial, which the existing gateway handles locally.
func incomingRTCSocketPresent(path string) bool {
	if path == "" {
		return false
	}
	info, err := os.Stat(path)
	return err == nil && info.Mode()&os.ModeSocket != 0
}
