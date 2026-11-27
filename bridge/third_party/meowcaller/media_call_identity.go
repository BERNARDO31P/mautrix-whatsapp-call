package meowcaller

// mediaCallLocked returns only the registry entry that owns this media loop.
// The caller must hold e.mu until its reads or writes are complete.
func (e *engine) mediaCallLocked(callID string, expected *Call) *engineCall {
	current := e.calls[callID]
	if expected == nil || current == nil || current.call != expected {
		return nil
	}
	return current
}
