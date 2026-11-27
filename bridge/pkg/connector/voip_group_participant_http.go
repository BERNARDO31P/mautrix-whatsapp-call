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

type rtcGroupParticipantRequest struct{ CallID, Target, Action string }
type rtcGroupCallRef struct {
	CallID string `json:"call_id"`
	Group  string `json:"group"`
}

var errRTCGroupUnavailable = errors.New("group calling unavailable")
var errRTCGroupBusy = errors.New("group action already in progress")
var errRTCGroupNotAllowed = errors.New("group action not allowed")
var errRTCGroupRejected = errors.New("provider rejected group action")
var errRTCGroupUnknown = errors.New("group action outcome unknown")

func validRTCGroupTarget(raw string) bool {
	user, server, ok := strings.Cut(raw, "@")
	if !ok || len(user) == 0 || len(user) > 32 || (server != "s.whatsapp.net" && server != "lid") {
		return false
	}
	for _, c := range user {
		if c < '0' || c > '9' {
			return false
		}
	}
	return true
}

func decodeRTCGroupParticipant(w http.ResponseWriter, r *http.Request) (rtcGroupParticipantRequest, bool) {
	var req rtcGroupParticipantRequest
	d := json.NewDecoder(http.MaxBytesReader(w, r.Body, 2048))
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
		var value *string
		if d.Decode(&value) != nil || value == nil {
			return req, false
		}
		switch key {
		case "call_id":
			req.CallID = *value
		case "target":
			req.Target = *value
		case "action":
			req.Action = *value
		default:
			return req, false
		}
	}
	end, err := d.Token()
	if err != nil || end != json.Delim('}') || d.Decode(new(any)) != io.EOF {
		return req, false
	}
	return req, len(req.CallID) <= 128 && validRTCCallLinkToken(req.CallID) && validRTCGroupTarget(req.Target) && (req.Action == "invite" || req.Action == "ring")
}

func rtcGroupParticipantError(w http.ResponseWriter, status int, msg string) {
	writeRTCCallLinkJSON(w, status, map[string]string{"errcode": "FI.MAU.GROUP_CALL_PARTICIPANT", "error": msg})
}

func serveRTCGroupParticipant(w http.ResponseWriter, r *http.Request, operation func(context.Context, rtcGroupParticipantRequest) error) {
	req, ok := decodeRTCGroupParticipant(w, r)
	if !ok {
		rtcGroupParticipantError(w, 400, "Invalid group participant request")
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 20*time.Second)
	defer cancel()
	if ctx.Err() != nil {
		rtcGroupParticipantError(w, 504, "Request cancelled before submission")
		return
	}
	err := operation(ctx, req)
	switch {
	case ctx.Err() != nil || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) || errors.Is(err, errRTCGroupUnknown):
		rtcGroupParticipantError(w, 504, "Participant outcome unknown; no automatic retry")
	case errors.Is(err, errRTCGroupUnavailable):
		rtcGroupParticipantError(w, 503, "Group calling unavailable for this login")
	case errors.Is(err, errRTCGroupBusy):
		rtcGroupParticipantError(w, 409, "Group action already in progress")
	case errors.Is(err, errRTCGroupNotAllowed):
		rtcGroupParticipantError(w, 409, "Action not allowed for current call or member")
	case errors.Is(err, errRTCGroupRejected):
		rtcGroupParticipantError(w, 502, "WhatsApp rejected the participant action")
	case err != nil:
		rtcGroupParticipantError(w, 502, "Participant operation failed; no automatic retry")
	default:
		writeRTCCallLinkJSON(w, 200, map[string]string{"action": req.Action, "target": req.Target, "outcome": "acknowledged"})
	}
}
