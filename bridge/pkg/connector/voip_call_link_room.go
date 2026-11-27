package connector

import (
	"sync"
	"time"
)

// Each link reservation belongs to a newly created private room and exactly
// one connected login. It can be consumed once; it is never stored in Matrix.
type rtcLinkReservation[T comparable] struct {
	owner        T
	sender       string
	generation   uint64
	token, media string
	expires      time.Time
}

type rtcLinkRooms[T comparable] struct {
	mu    sync.Mutex
	rooms map[string]rtcLinkReservation[T]
}

func (r *rtcLinkRooms[T]) put(room string, item rtcLinkReservation[T], now time.Time) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	if room == "" || !now.Before(item.expires) {
		return false
	}
	if r.rooms == nil {
		r.rooms = make(map[string]rtcLinkReservation[T])
	}
	for key, existing := range r.rooms {
		if !now.Before(existing.expires) {
			delete(r.rooms, key)
		}
	}
	if len(r.rooms) >= 64 {
		return false
	}
	if _, exists := r.rooms[room]; exists {
		return false
	}
	for _, existing := range r.rooms {
		if existing.owner == item.owner {
			return false
		}
	}
	r.rooms[room] = item
	time.AfterFunc(item.expires.Sub(now), func() {
		r.mu.Lock()
		defer r.mu.Unlock()
		if current, ok := r.rooms[room]; ok && current == item {
			delete(r.rooms, room)
		}
	})
	return true
}

func (r *rtcLinkRooms[T]) peek(room, sender string, now time.Time) (rtcLinkReservation[T], bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	item, ok := r.rooms[room]
	if !ok || item.sender != sender || !now.Before(item.expires) {
		return rtcLinkReservation[T]{}, false
	}
	return item, true
}

func (r *rtcLinkRooms[T]) consume(room string, item rtcLinkReservation[T], now time.Time) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	if current, ok := r.rooms[room]; !ok || current != item || !now.Before(item.expires) {
		return false
	}
	delete(r.rooms, room)
	return true
}

func (r *rtcLinkRooms[T]) available(owner T, now time.Time) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	count := 0
	for _, item := range r.rooms {
		if now.Before(item.expires) {
			count++
			if item.owner == owner {
				return false
			}
		}
	}
	return count < 64
}
func (r *rtcLinkRooms[T]) remove(room string, owner T) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	item, ok := r.rooms[room]
	if !ok || item.owner != owner {
		return false
	}
	delete(r.rooms, room)
	return true
}
