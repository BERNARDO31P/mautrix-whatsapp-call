package connector

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"sync"
	"time"
	"unicode"
	"unicode/utf8"
)

const rtcReactionCapacity = 64
const rtcReactionTTL = 10 * time.Second

type rtcReactionRequest struct {
	CallID, Stream string
	After          uint64
	Emoji          *string
}
type rtcReactionEvent struct {
	Sequence    uint64 `json:"sequence"`
	Participant string `json:"participant_id"`
	Emoji       string `json:"emoji"`
	Removed     bool   `json:"removed"`
	received    time.Time
}
type rtcReactionResult struct {
	CallID  string             `json:"call_id"`
	Stream  string             `json:"stream"`
	Roster  uint32             `json:"roster_transaction"`
	Cursor  uint64             `json:"cursor"`
	Reset   bool               `json:"reset"`
	Events  []rtcReactionEvent `json:"events"`
	Outcome string             `json:"outcome"`
}
type rtcReactionFeed struct {
	mu         sync.Mutex
	stream     string
	seq, floor uint64
	events     []rtcReactionEvent
	last       map[string]rtcReactionEvent
	roster     uint32
	sent       time.Time
	busy       bool
}

func newRTCReactionFeed() *rtcReactionFeed {
	var raw [16]byte
	if _, err := rand.Read(raw[:]); err != nil {
		return nil
	}
	return &rtcReactionFeed{stream: hex.EncodeToString(raw[:]), last: make(map[string]rtcReactionEvent)}
}

// A bounded transient emoji sequence, not an arbitrary chat message. Empty clears.
func validRTCReaction(s string) bool {
	if s == "" {
		return true
	}
	if len(s) > 128 || !utf8.ValidString(s) {
		return false
	}
	symbol := false
	for _, r := range s {
		switch {
		case r == utf8.RuneError:
			return false
		case unicode.Is(unicode.So, r), r == 0x20e3:
			symbol = true
		case r == 0x200d, r == 0xfe0f, r == 0xfe0e, r >= 0x1f3fb && r <= 0x1f3ff, r >= 0xe0020 && r <= 0xe007f, r >= '0' && r <= '9', r == '#', r == '*':
		default:
			return false
		}
	}
	return symbol
}
func (f *rtcReactionFeed) prune(now time.Time) {
	for len(f.events) > 0 && now.Sub(f.events[0].received) >= rtcReactionTTL {
		f.floor = f.events[0].Sequence
		f.events = f.events[1:]
	}
	for k, t := range f.last {
		if now.Sub(t.received) >= rtcReactionTTL {
			delete(f.last, k)
		}
	}
}

// A roster change invalidates earlier transient attribution rather than guessing
// whether a participant has left and rejoined with the same identity.
func (f *rtcReactionFeed) setRoster(roster uint32) bool {
	if roster < f.roster {
		return false
	}
	if f.roster != roster {
		f.roster = roster
		f.floor = f.seq
		f.events = nil
		clear(f.last)
	}
	return true
}
func (f *rtcReactionFeed) append(now time.Time, roster uint32, participant, emoji string, removed bool) {
	if !validRTCGroupTarget(participant) || !validRTCReaction(emoji) || removed != (emoji == "") {
		return
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if !f.setRoster(roster) {
		return
	}
	f.prune(now)
	if last, ok := f.last[participant]; ok && ((removed && last.Removed) || (now.Sub(last.received) < 200*time.Millisecond && !removed)) {
		return
	}
	if _, ok := f.last[participant]; !ok && len(f.last) >= 256 {
		return
	}
	// Clear actions are still bounded by the ring, and should not be lost just
	// because a reaction from that participant was recently displayed.
	f.seq++
	if len(f.events) == rtcReactionCapacity {
		f.floor = f.events[0].Sequence
		f.events = f.events[1:]
	}
	ev := rtcReactionEvent{Sequence: f.seq, Participant: participant, Emoji: emoji, Removed: removed, received: now}
	f.last[participant] = ev
	f.events = append(f.events, ev)
}
func (f *rtcReactionFeed) snapshot(now time.Time, roster uint32, req rtcReactionRequest) rtcReactionResult {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.setRoster(roster)
	f.prune(now)
	out := rtcReactionResult{CallID: req.CallID, Stream: f.stream, Roster: f.roster, Cursor: f.seq, Events: make([]rtcReactionEvent, 0), Outcome: "snapshot"}
	after := req.After
	if req.Stream != f.stream || after < f.floor || after > f.seq {
		out.Reset = true
		after = f.floor
	}
	for _, ev := range f.events {
		if ev.Sequence > after {
			out.Events = append(out.Events, ev)
		}
	}
	return out
}
func (f *rtcReactionFeed) begin(now time.Time, stream string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if stream != f.stream {
		return errRTCGroupNotAllowed
	}
	if f.busy || (!f.sent.IsZero() && now.Sub(f.sent) < time.Second) {
		return errRTCGroupBusy
	}
	f.busy = true
	f.sent = now
	return nil
}
func (f *rtcReactionFeed) finish() { f.mu.Lock(); f.busy = false; f.mu.Unlock() }
func decodeRTCReaction(w http.ResponseWriter, r *http.Request) (rtcReactionRequest, bool) {
	var req rtcReactionRequest
	d := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1024))
	start, err := d.Token()
	if err != nil || start != json.Delim('{') {
		return req, false
	}
	seen := map[string]bool{}
	for d.More() {
		token, err := d.Token()
		key, ok := token.(string)
		if err != nil || !ok || seen[key] {
			return req, false
		}
		seen[key] = true
		switch key {
		case "call_id", "stream":
			var v *string
			if d.Decode(&v) != nil || v == nil {
				return req, false
			}
			if key == "call_id" {
				req.CallID = *v
			} else {
				req.Stream = *v
			}
		case "after":
			var v *uint64
			if d.Decode(&v) != nil || v == nil {
				return req, false
			}
			req.After = *v
		case "emoji":
			if d.Decode(&req.Emoji) != nil || req.Emoji == nil {
				return req, false
			}
		default:
			return req, false
		}
	}
	end, err := d.Token()
	if err != nil || end != json.Delim('}') || d.Decode(new(any)) != io.EOF || len(req.CallID) > 128 || !validRTCCallLinkToken(req.CallID) {
		return req, false
	}
	if req.Stream != "" {
		if b, e := hex.DecodeString(req.Stream); e != nil || len(b) != 16 {
			return req, false
		}
	}
	if req.Emoji != nil && (req.Stream == "" || !validRTCReaction(*req.Emoji)) {
		return req, false
	}
	if req.Stream == "" && req.After != 0 {
		return req, false
	}
	return req, true
}
func rtcReactionError(w http.ResponseWriter, status int, msg string) {
	writeRTCCallLinkJSON(w, status, map[string]string{"errcode": "FI.MAU.CALL_REACTION", "error": msg})
}
func serveRTCReaction(w http.ResponseWriter, r *http.Request, operation func(context.Context, rtcReactionRequest) (rtcReactionResult, error)) {
	req, ok := decodeRTCReaction(w, r)
	if !ok {
		rtcReactionError(w, 400, "Invalid call reaction request")
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()
	if ctx.Err() != nil {
		rtcReactionError(w, 504, "Request cancelled")
		return
	}
	result, err := operation(ctx, req)
	switch {
	case ctx.Err() != nil || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) || errors.Is(err, errRTCGroupUnknown):
		rtcReactionError(w, 504, "Reaction outcome unknown; no automatic retry")
	case errors.Is(err, errRTCGroupUnavailable):
		rtcReactionError(w, 503, "Calling unavailable")
	case errors.Is(err, errRTCGroupBusy):
		rtcReactionError(w, 429, "Reaction submission rate limited")
	case errors.Is(err, errRTCGroupNotAllowed):
		rtcReactionError(w, 409, "Call or reaction stream is no longer current")
	case err != nil:
		rtcReactionError(w, 502, "Reaction operation failed")
	default:
		writeRTCCallLinkJSON(w, 200, result)
	}
}
