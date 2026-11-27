package meowcaller

import (
	"reflect"
	"testing"
)

func TestMediaNotificationTransitionsAreNotCoalesced(t *testing.T) {
	c := &Call{}
	var got []string
	c.OnMuteState(func(muted bool) {
		if muted {
			got = append(got, "muted")
		} else {
			got = append(got, "unmuted")
		}
	})
	c.OnVideoState(func(v VideoState) {
		switch v.Raw {
		case 11:
			got = append(got, "upgrade")
		case 6:
			got = append(got, "stop")
		case 1:
			got = append(got, "enable")
		}
	})
	// A blocked notification keeps the drainer busy while a burst is recorded.
	entered, release, done := make(chan struct{}), make(chan struct{}), make(chan struct{})
	go func() {
		c.enqueueNotification(func() { close(entered); <-release })
		close(done)
	}()
	<-entered
	c.recordMute(true)
	c.recordVideoState(VideoState{Raw: 11, Upgrade: true})
	c.recordVideoState(VideoState{Raw: 6})
	c.recordMute(false)
	c.recordVideoState(VideoState{Raw: 1, Active: true})
	close(release)
	<-done
	want := []string{"muted", "upgrade", "stop", "unmuted", "enable"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("notifications=%v want=%v", got, want)
	}
}

func TestMediaNotificationReplacementAndReplay(t *testing.T) {
	c := &Call{}
	var old, next []bool
	c.OnMuteState(func(v bool) { old = append(old, v) })
	c.enqueueNotification(func() {
		c.recordMute(true)
		c.recordMute(false)
		c.OnMuteState(func(v bool) { next = append(next, v) })
		c.recordMute(true)
	})
	if len(old) != 0 || !reflect.DeepEqual(next, []bool{false, true}) {
		t.Fatalf("old=%v next=%v", old, next)
	}
}

func TestMediaNotificationTerminationDropsQueuedAndLateUpdates(t *testing.T) {
	c := &Call{}
	n := 0
	c.OnVideoState(func(VideoState) { n++ })
	c.OnMuteState(func(bool) { n++ })
	c.enqueueNotification(func() {
		c.recordVideoState(VideoState{Raw: 1})
		c.recordMute(true)
		c.mu.Lock()
		c.phase = CallPhaseEnded
		c.mu.Unlock()
		c.recordVideoState(VideoState{Raw: 6})
		c.recordMute(false)
		c.OnVideoState(func(VideoState) { n++ })
		c.OnMuteState(func(bool) { n++ })
	})
	if n != 0 {
		t.Fatalf("late callbacks=%d", n)
	}
	if c.lastVideoState == nil || c.lastVideoState.Raw != 1 || c.lastMute == nil || !*c.lastMute {
		t.Fatal("late state mutated terminal call")
	}
}

func TestMediaNotificationCanReenterRegistration(t *testing.T) {
	c := &Call{}
	var got []bool
	c.OnMuteState(func(v bool) {
		got = append(got, v)
		c.OnMuteState(func(v bool) { got = append(got, v) })
		c.recordMute(false)
	})
	c.recordMute(true)
	if !reflect.DeepEqual(got, []bool{true, true, false}) {
		t.Fatalf("callbacks=%v", got)
	}
}
