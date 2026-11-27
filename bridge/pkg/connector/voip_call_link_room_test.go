package connector

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestRTCLinkRoomSingleUseAndOwner(t *testing.T) {
	now := time.Now()
	var rooms rtcLinkRooms[string]
	item := rtcLinkReservation[string]{owner: "login1", sender: "@a:test", generation: 3, token: "secret", media: "audio", expires: now.Add(time.Minute)}
	if !rooms.put("!room:test", item, now) || rooms.available("login1", now) || rooms.put("!other:test", item, now) {
		t.Fatal("reservation not exclusive")
	}
	if _, ok := rooms.peek("!room:test", "@b:test", now); ok {
		t.Fatal("wrong sender")
	}
	if rooms.remove("!room:test", "login2") {
		t.Fatal("wrong login removed room")
	}
	stale := item
	stale.generation++
	if rooms.consume("!room:test", stale, now) {
		t.Fatal("wrong generation consumed room")
	}
	var accepted atomic.Int32
	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if rooms.consume("!room:test", item, now) {
				accepted.Add(1)
			}
		}()
	}
	wg.Wait()
	if accepted.Load() != 1 || !rooms.available("login1", now) {
		t.Fatal("not exactly one use")
	}
}

func TestRTCLinkRoomExpiryAndCancellation(t *testing.T) {
	now := time.Now()
	var rooms rtcLinkRooms[string]
	item := rtcLinkReservation[string]{owner: "owner", sender: "@a:test", generation: 1, token: "secret", media: "video", expires: now.Add(time.Second)}
	if !rooms.put("!r:test", item, now) {
		t.Fatal("put failed")
	}
	if _, ok := rooms.peek("!r:test", item.sender, item.expires); ok || rooms.consume("!r:test", item, item.expires) {
		t.Fatal("expired room used")
	}
	next := item
	next.generation++
	next.expires = now.Add(2 * time.Second)
	if !rooms.put("!new:test", next, item.expires) || !rooms.remove("!new:test", next.owner) || rooms.consume("!new:test", next, now) {
		t.Fatal("cancelled room used")
	}
}

func TestRTCCallLinkRoomPreparationAndLeaveHTTP(t *testing.T) {
	for _, body := range []string{`{"media":"audio","link":"secret","room_id":"!existing:test"}`, `{"media":"audio","link":"secret","link":"other"}`, `{"media":"audio","link":"https://evil.example/secret"}`} {
		w := httptest.NewRecorder()
		serveRTCCallLinkRoom(w, httptest.NewRequest(http.MethodPost, "/join", strings.NewReader(body)), func(context.Context, rtcCallLinkRequest) (rtcCallLinkRoomResult, error) {
			t.Fatal("invalid request reached provider")
			return rtcCallLinkRoomResult{}, nil
		})
		if w.Code != 400 {
			t.Fatal(w.Code)
		}
	}
	w := httptest.NewRecorder()
	serveRTCCallLinkRoom(w, httptest.NewRequest(http.MethodPost, "/join", strings.NewReader(`{"media":"video","link":"secret"}`)), func(ctx context.Context, req rtcCallLinkRequest) (rtcCallLinkRoomResult, error) {
		if req.Link != "secret" || req.Media != "video" {
			t.Fatal("bad request")
		}
		return rtcCallLinkRoomResult{RoomID: "!new:test", Media: "video"}, nil
	})
	if w.Code != 200 || strings.Contains(w.Body.String(), "secret") || w.Header().Get("Cache-Control") != "no-store" {
		t.Fatal(w.Code, w.Body.String())
	}
	for _, test := range []struct {
		body  string
		allow bool
		code  int
	}{{`{"room_id":"!new:test"}`, true, 200}, {`{"room_id":"!other:test"}`, false, 404}, {`{"room_id":"!new:test","room_id":"!other:test"}`, false, 400}, {`{"room_id":null}`, false, 400}} {
		w := httptest.NewRecorder()
		serveRTCCallLinkLeave(w, httptest.NewRequest(http.MethodPost, "/leave", strings.NewReader(test.body)), func(room string) bool { return test.allow && room == "!new:test" })
		if w.Code != test.code {
			t.Fatal(w.Code, w.Body.String())
		}
		if test.allow && !strings.Contains(w.Body.String(), "cancelled_locally") {
			t.Fatal(w.Body.String())
		}
	}
}
