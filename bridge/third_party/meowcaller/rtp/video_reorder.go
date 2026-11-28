package rtp

import "time"

const (
	videoReorderWindow     = 64
	videoReorderDelay      = 50 * time.Millisecond
	videoReorderMaxPayload = 64 << 10
)

// VideoPacket is an authenticated video fragment and its original metadata.
type VideoPacket struct {
	Sequence    uint16
	Timestamp   uint32
	Marker      bool
	Orientation int
	Payload     []byte
}

// VideoReorder holds short forward gaps without delaying in-order packets.
// A missing fragment is skipped on the next arrival after the bounded wait,
// or when the sequence window is exceeded. It belongs to one receiver epoch.
type VideoReorder struct {
	started  bool
	next     uint16
	pending  map[uint16]VideoPacket
	gapSince time.Time
}

func (r *VideoReorder) Push(p VideoPacket, now time.Time) []VideoPacket {
	if len(p.Payload) > videoReorderMaxPayload {
		return nil
	}
	if !r.started {
		r.started = true
		r.next = p.Sequence
	}
	distance := int16(p.Sequence - r.next)
	if distance < 0 {
		return nil
	}
	if r.pending == nil {
		r.pending = make(map[uint16]VideoPacket)
	}
	if _, exists := r.pending[p.Sequence]; !exists {
		// The receive loop may reuse its backing buffer.
		p.Payload = append([]byte(nil), p.Payload...)
		r.pending[p.Sequence] = p
	}
	var ready []VideoPacket
	for {
		if p, ok := r.pending[r.next]; ok {
			delete(r.pending, r.next)
			ready = append(ready, p)
			r.next++
			continue
		}
		if len(r.pending) == 0 {
			r.gapSince = time.Time{}
			break
		}
		if r.gapSince.IsZero() {
			r.gapSince = now
		}
		nearest := int16(32767)
		furthest := int16(0)
		for seq := range r.pending {
			d := int16(seq - r.next)
			if d < nearest {
				nearest = d
			}
			if d > furthest {
				furthest = d
			}
		}
		if furthest < videoReorderWindow && now.Sub(r.gapSince) < videoReorderDelay {
			break
		}
		// The assembler observes the sequence gap and invalidates any
		// affected access unit. Never fabricate or replay missing data.
		r.next += uint16(nearest)
	}
	return ready
}
