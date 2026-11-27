package connector

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"sort"
	"time"
)

type rtcGroupHandRequest struct {
	CallID string
	Raised *bool // omitted: read-only snapshot; false: explicit lower
}

type rtcGroupHandParticipant struct {
	ParticipantID string `json:"participant_id"`
	Self          bool   `json:"self"`
	Raised        bool   `json:"raised"`
}

type rtcGroupHandResult struct {
	CallID       string                    `json:"call_id"`
	Transaction  uint32                    `json:"roster_transaction"`
	HandRevision uint64                    `json:"hand_revision"`
	Participants []rtcGroupHandParticipant `json:"participants"`
	Outcome      string                    `json:"outcome"`
}

// Return a complete replacement snapshot, never merge a departed participant
// into a new roster. Do not manufacture a Matrix identity for a provider JID.
func rtcGroupHandSnapshot(callID string, transaction uint32, revision uint64, rows []rtcGroupHandParticipant) (rtcGroupHandResult, error) {
	result := rtcGroupHandResult{CallID: callID, Transaction: transaction, HandRevision: revision, Participants: make([]rtcGroupHandParticipant, 0, len(rows)), Outcome: "snapshot"}
	if transaction == 0 || revision == 0 || len(rows) == 0 || len(rows) > 32 {
		return rtcGroupHandResult{}, errRTCGroupNotAllowed
	}
	seen := map[string]bool{}
	own := 0
	for _, row := range rows {
		if !validRTCGroupTarget(row.ParticipantID) || seen[row.ParticipantID] {
			return rtcGroupHandResult{}, errRTCGroupNotAllowed
		}
		seen[row.ParticipantID] = true
		if row.Self {
			own++
		}
		result.Participants = append(result.Participants, row)
	}
	if own != 1 {
		return rtcGroupHandResult{}, errRTCGroupNotAllowed
	}
	sort.Slice(result.Participants, func(i, j int) bool {
		return result.Participants[i].ParticipantID < result.Participants[j].ParticipantID
	})
	return result, nil
}

func decodeRTCGroupHand(w http.ResponseWriter, r *http.Request) (rtcGroupHandRequest, bool) {
	var req rtcGroupHandRequest
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
		case "call_id":
			var value *string
			if d.Decode(&value) != nil || value == nil {
				return req, false
			}
			req.CallID = *value
		case "raised":
			if d.Decode(&req.Raised) != nil || req.Raised == nil {
				return req, false
			}
		default:
			return req, false
		}
	}
	end, err := d.Token()
	return req, err == nil && end == json.Delim('}') && d.Decode(new(any)) == io.EOF && len(req.CallID) <= 128 && validRTCCallLinkToken(req.CallID)
}

func rtcGroupHandError(w http.ResponseWriter, status int, msg string) {
	writeRTCCallLinkJSON(w, status, map[string]string{"errcode": "FI.MAU.GROUP_CALL_HAND", "error": msg})
}

func serveRTCGroupHand(w http.ResponseWriter, r *http.Request, operation func(context.Context, rtcGroupHandRequest) (rtcGroupHandResult, error)) {
	req, ok := decodeRTCGroupHand(w, r)
	if !ok {
		rtcGroupHandError(w, 400, "Invalid hand-state request")
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()
	if ctx.Err() != nil {
		rtcGroupHandError(w, 504, "Request cancelled before submission")
		return
	}
	result, err := operation(ctx, req)
	switch {
	case ctx.Err() != nil || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) || errors.Is(err, errRTCGroupUnknown):
		rtcGroupHandError(w, 504, "Hand-state outcome unknown; refresh snapshot before another action")
	case errors.Is(err, errRTCGroupUnavailable):
		rtcGroupHandError(w, 503, "Group calling unavailable for this login")
	case errors.Is(err, errRTCGroupBusy):
		rtcGroupHandError(w, 409, "Hand action already in progress")
	case errors.Is(err, errRTCGroupNotAllowed):
		rtcGroupHandError(w, 409, "No unambiguous connected participant roster for this call")
	case err != nil:
		rtcGroupHandError(w, 502, "Hand-state operation failed; no automatic retry")
	default:
		writeRTCCallLinkJSON(w, 200, result)
	}
}
