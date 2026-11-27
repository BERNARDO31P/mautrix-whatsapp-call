package connector

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"time"
)

type rtcVideoDecisionRequest struct {
	CallID     string
	Action     string
	Generation uint64
}
type rtcVideoDecisionResult struct {
	CallID             string `json:"call_id"`
	Manual             bool   `json:"manual"`
	LocalCamera        bool   `json:"local_camera"`
	PeerCamera         bool   `json:"peer_camera"`
	OutgoingPending    bool   `json:"outgoing_pending"`
	IncomingGeneration uint64 `json:"incoming_generation"`
	Outcome            string `json:"outcome"`
}

func decodeRTCVideoDecision(w http.ResponseWriter, r *http.Request) (rtcVideoDecisionRequest, bool) {
	var req rtcVideoDecisionRequest
	d := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1024))
	start, err := d.Token()
	if err != nil || start != json.Delim('{') {
		return req, false
	}
	seen := map[string]bool{}
	for d.More() {
		token, err := d.Token()
		name, ok := token.(string)
		if err != nil || !ok || seen[name] {
			return req, false
		}
		seen[name] = true
		switch name {
		case "call_id", "action":
			var value *string
			if d.Decode(&value) != nil || value == nil {
				return req, false
			}
			if name == "call_id" {
				req.CallID = *value
			} else {
				req.Action = *value
			}
		case "generation":
			var value *uint64
			if d.Decode(&value) != nil || value == nil {
				return req, false
			}
			req.Generation = *value
		default:
			return req, false
		}
	}
	end, err := d.Token()
	if err != nil || end != json.Delim('}') || d.Decode(new(any)) != io.EOF || len(req.CallID) > 128 || !validRTCCallLinkToken(req.CallID) {
		return req, false
	}
	switch req.Action {
	case "snapshot", "manual":
		return req, !seen["generation"]
	case "accept", "reject":
		return req, req.Generation != 0
	default:
		return req, false
	}
}
func rtcVideoDecisionError(w http.ResponseWriter, status int, msg string) {
	writeRTCCallLinkJSON(w, status, map[string]string{"errcode": "FI.MAU.CALL_VIDEO_DECISION", "error": msg})
}
func serveRTCVideoDecision(w http.ResponseWriter, r *http.Request, op func(context.Context, rtcVideoDecisionRequest) (rtcVideoDecisionResult, error)) {
	req, ok := decodeRTCVideoDecision(w, r)
	if !ok {
		rtcVideoDecisionError(w, 400, "Invalid video decision request")
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()
	if ctx.Err() != nil {
		rtcVideoDecisionError(w, 504, "Request cancelled before submission")
		return
	}
	result, err := op(ctx, req)
	switch {
	case ctx.Err() != nil || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) || errors.Is(err, errRTCGroupUnknown):
		rtcVideoDecisionError(w, 504, "Outcome unknown; refresh call snapshot, do not retry the decision")
	case errors.Is(err, errRTCGroupUnavailable):
		rtcVideoDecisionError(w, 503, "Calling unavailable for this login")
	case errors.Is(err, errRTCGroupNotAllowed):
		rtcVideoDecisionError(w, 409, "No matching active direct call or pending manual decision")
	case err != nil:
		rtcVideoDecisionError(w, 502, "Video operation failed; no automatic retry")
	default:
		writeRTCCallLinkJSON(w, 200, result)
	}
}
