package voipbridge

import (
	"errors"
	"sync"
	"testing"
	"time"
)

func TestLifecycle(t *testing.T) {
	m := NewManager()
	id := ID{LoginID: "a", CallID: "call"}
	s, err := m.Start(id, "!room:example.org", Incoming)
	if err != nil || s.State != Ringing {
		t.Fatalf("start: %+v, %v", s, err)
	}
	if _, err = m.Transition(id, Connecting); err != nil {
		t.Fatal(err)
	}
	if _, err = m.Transition(id, Active); err != nil {
		t.Fatal(err)
	}
	if _, err = m.End(id, "remote-hangup"); err != nil {
		t.Fatal(err)
	}
	if s, _ = m.Get(id); s.State != Ended || s.EndReason != "remote-hangup" {
		t.Fatalf("end: %+v", s)
	}
	if _, err = m.Transition(id, Active); !errors.Is(err, ErrTransition) {
		t.Fatalf("expected invalid transition, got %v", err)
	}
}

func TestDuplicateAndIsolation(t *testing.T) {
	m := NewManager()
	a := ID{LoginID: "a", CallID: "same"}
	b := ID{LoginID: "b", CallID: "same"}
	if _, err := m.Start(a, "!a:server", Incoming); err != nil {
		t.Fatal(err)
	}
	if _, err := m.Start(b, "!b:server", Outgoing); err != nil {
		t.Fatal(err)
	}
	if _, err := m.Start(a, "!a:server", Incoming); err != nil {
		t.Fatal(err)
	}
	if _, err := m.Start(a, "!wrong:server", Incoming); !errors.Is(err, ErrConflict) {
		t.Fatalf("expected conflict, got %v", err)
	}
	if len(m.EndLogin("a", "logout")) != 1 {
		t.Fatal("expected one terminated call")
	}
	if s, _ := m.Get(b); s.State != Ringing {
		t.Fatalf("other login affected: %+v", s)
	}
}

func TestConcurrentDuplicateEvents(t *testing.T) {
	m := NewManager()
	id := ID{LoginID: "a", CallID: "call"}
	_, _ = m.Start(id, "!room:server", Incoming)
	_, _ = m.Transition(id, Connecting)
	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, _ = m.Transition(id, Active)
			_, _ = m.Get(id)
		}()
	}
	wg.Wait()
	if s, _ := m.Get(id); s.State != Active {
		t.Fatalf("unexpected state: %+v", s)
	}
}

func TestPruneAndValidation(t *testing.T) {
	m := NewManager()
	if _, err := m.Start(ID{}, "room", Incoming); !errors.Is(err, ErrInvalid) {
		t.Fatal(err)
	}
	id := ID{LoginID: "a", CallID: "call"}
	_, _ = m.Start(id, "!room:server", Incoming)
	if n := m.PruneEnded(0); n != 0 {
		t.Fatalf("removed active call: %d", n)
	}
	_, _ = m.End(id, "done")
	m.now = func() time.Time { return time.Now().Add(time.Second) }
	if n := m.PruneEnded(time.Millisecond); n != 1 {
		t.Fatalf("expected prune=1, got %d", n)
	}
}
