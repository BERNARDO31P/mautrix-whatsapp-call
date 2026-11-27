package connector

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
	"time"
)

type rtcWaitingRoomRequest struct {
	RoomID, Action, Target string
	Generation             uint64
	Enabled                *bool
}
type rtcWaitingRoomResult struct {
	RoomID      string   `json:"room_id"`
	Generation  uint64   `json:"generation"`
	Transaction uint32   `json:"transaction"`
	Enabled     bool     `json:"approval_required"`
	Admin       bool     `json:"is_admin"`
	Waiting     bool     `json:"in_waiting_room"`
	Admitted    bool     `json:"admitted"`
	Pending     []string `json:"pending"`
	Outcome     string   `json:"outcome"`
}

func decodeRTCWaitingRoom(w http.ResponseWriter, r *http.Request) (rtcWaitingRoomRequest, bool) {
	var req rtcWaitingRoomRequest
	d := json.NewDecoder(http.MaxBytesReader(w, r.Body, 2048))
	start, err := d.Token()
	if err != nil || start != json.Delim('{') {
		return req, false
	}
	seen := map[string]bool{}
	for d.More() {
		raw, err := d.Token()
		key, ok := raw.(string)
		if err != nil || !ok || seen[key] {
			return req, false
		}
		seen[key] = true
		switch key {
		case "room_id", "action", "target":
			var v *string
			if d.Decode(&v) != nil || v == nil {
				return req, false
			}
			switch key {
			case "room_id":
				req.RoomID = *v
			case "action":
				req.Action = *v
			case "target":
				req.Target = *v
			}
		case "generation":
			var v *uint64
			if d.Decode(&v) != nil || v == nil {
				return req, false
			}
			req.Generation = *v
		case "enabled":
			if d.Decode(&req.Enabled) != nil || req.Enabled == nil {
				return req, false
			}
		default:
			return req, false
		}
	}
	end, err := d.Token()
	if err != nil || end != json.Delim('}') || d.Decode(new(any)) != io.EOF || !strings.HasPrefix(req.RoomID, "!") || len(req.RoomID) < 2 || len(req.RoomID) > 512 || strings.ContainsAny(req.RoomID, " \t\r\n") {
		return req, false
	}
	switch req.Action {
	case "snapshot":
		return req, !seen["generation"] && !seen["enabled"] && !seen["target"]
	case "approval":
		return req, req.Generation > 0 && req.Enabled != nil && !seen["target"]
	case "admit", "deny":
		return req, req.Generation > 0 && validRTCGroupTarget(req.Target) && !seen["enabled"]
	default:
		return req, false
	}
}

func serveRTCWaitingRoom(w http.ResponseWriter, r *http.Request, op func(context.Context, rtcWaitingRoomRequest) (rtcWaitingRoomResult, error)) {
	req, ok := decodeRTCWaitingRoom(w, r)
	if !ok {
		rtcCallLinkError(w, 400, "Invalid waiting-room request")
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()
	if ctx.Err() != nil {
		rtcCallLinkError(w, 504, "Waiting-room request cancelled")
		return
	}
	result, err := op(ctx, req)
	switch {
	case ctx.Err() != nil || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) || errors.Is(err, errRTCGroupUnknown):
		rtcCallLinkError(w, 504, "Waiting-room outcome unknown; refresh snapshot, no automatic retry")
	case errors.Is(err, errRTCCallLinkUnavailable):
		rtcCallLinkError(w, 503, "Calling unavailable for this login")
	case errors.Is(err, errRTCCallLinkBusy):
		rtcCallLinkError(w, 409, "Waiting-room action already in progress")
	case errors.Is(err, errRTCGroupNotAllowed):
		rtcCallLinkError(w, 409, "No current authorized waiting-room operation")
	case err != nil:
		rtcCallLinkError(w, 502, "Waiting-room operation failed; no automatic retry")
	default:
		writeRTCCallLinkJSON(w, 200, result)
	}
}
