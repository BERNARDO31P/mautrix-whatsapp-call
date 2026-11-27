package meowcaller

import "testing"

func TestMediaCallIdentity(t *testing.T) {
	first, second := &Call{}, &Call{}
	e := &engine{calls: map[string]*engineCall{"id": {call: first}}}
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.mediaCallLocked("id", first) == nil || e.mediaCallLocked("id", nil) != nil || e.mediaCallLocked("missing", first) != nil {
		t.Fatal("incorrect initial registry match")
	}
	replacement := &engineCall{call: second}
	e.calls["id"] = replacement
	if e.mediaCallLocked("id", first) != nil || e.mediaCallLocked("id", second) != replacement {
		t.Fatal("stale media loop matched replacement")
	}
	delete(e.calls, "id")
	if e.mediaCallLocked("id", second) != nil {
		t.Fatal("removed call matched")
	}
}
