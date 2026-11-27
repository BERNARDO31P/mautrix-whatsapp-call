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

// The provisioning transport authenticates before calling this handler. Keep
// tokens in the JSON body and success response only, never in paths or errors.
type rtcCallLinkRequest struct {
	Media string `json:"media"`
	Link  string `json:"link,omitempty"`
}

type rtcCallLinkResult struct {
	Media            string `json:"media"`
	URL              string `json:"url,omitempty"`
	ApprovalRequired *bool  `json:"approval_required,omitempty"`
	IsAdmin          *bool  `json:"is_admin,omitempty"`
}

type rtcCallLinkOperation func(context.Context, rtcCallLinkRequest) (rtcCallLinkResult, error)

var errRTCCallLinkUnavailable = errors.New("call link operation unavailable")
var errRTCCallLinkInvalidResult = errors.New("invalid call link provider result")
var errRTCCallLinkBusy = errors.New("call link operation already in progress")

func validRTCCallLinkToken(token string) bool {
	if len(token) == 0 || len(token) > 256 {
		return false
	}
	for _, c := range token {
		if !(c >= 'A' && c <= 'Z' || c >= 'a' && c <= 'z' || c >= '0' && c <= '9' || c == '-' || c == '_') {
			return false
		}
	}
	return true
}

// Accept a raw token or the exact canonical URL matching the selected media.
// No URL is fetched. Reject encoded delimiters, queries, fragments and userinfo.
func rtcCallLinkToken(raw, media string) (string, bool) {
	if media != "audio" && media != "video" {
		return "", false
	}
	token := strings.TrimPrefix(raw, "https://call.whatsapp.com/"+media+"/")
	return token, validRTCCallLinkToken(token)
}

func decodeRTCCallLinkRequest(w http.ResponseWriter, r *http.Request, preview bool) (rtcCallLinkRequest, bool) {
	var req rtcCallLinkRequest
	d := json.NewDecoder(http.MaxBytesReader(w, r.Body, 2048))
	// Explicitly reject duplicate fields and null values rather than accepting
	// the last field from an ambiguous request.
	start, err := d.Token()
	if err != nil || start != json.Delim('{') {
		return req, false
	}
	seen := make(map[string]bool, 2)
	for d.More() {
		key, err := d.Token()
		name, ok := key.(string)
		if err != nil || !ok || seen[name] {
			return req, false
		}
		seen[name] = true
		var value *string
		if d.Decode(&value) != nil || value == nil {
			return req, false
		}
		switch name {
		case "media":
			req.Media = *value
		case "link":
			req.Link = *value
		default:
			return req, false
		}
	}
	end, err := d.Token()
	if err != nil || end != json.Delim('}') || d.Decode(new(any)) != io.EOF {
		return req, false
	}
	if req.Media != "audio" && req.Media != "video" {
		return req, false
	}
	if !preview {
		return req, !seen["link"]
	}
	token, ok := rtcCallLinkToken(req.Link, req.Media)
	req.Link = token
	return req, ok
}

func writeRTCCallLinkJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("Pragma", "no-cache")
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}

func rtcCallLinkError(w http.ResponseWriter, status int, msg string) {
	writeRTCCallLinkJSON(w, status, map[string]string{"errcode": "FI.MAU.CALL_LINK", "error": msg})
}

func serveRTCCallLink(w http.ResponseWriter, r *http.Request, preview bool, operation rtcCallLinkOperation) {
	req, ok := decodeRTCCallLinkRequest(w, r, preview)
	if !ok {
		rtcCallLinkError(w, http.StatusBadRequest, "Invalid call-link request")
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 15*time.Second)
	defer cancel()
	if ctx.Err() != nil {
		rtcCallLinkError(w, http.StatusGatewayTimeout, "Call-link request cancelled")
		return
	}
	result, err := operation(ctx, req)
	if ctx.Err() != nil || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		rtcCallLinkError(w, http.StatusGatewayTimeout, "Call-link outcome unavailable; no automatic retry")
		return
	}
	if errors.Is(err, errRTCCallLinkUnavailable) {
		rtcCallLinkError(w, http.StatusServiceUnavailable, "Calling is unavailable for this login")
		return
	}
	if errors.Is(err, errRTCCallLinkBusy) {
		rtcCallLinkError(w, http.StatusConflict, "Call-link request already in progress")
		return
	}
	if err != nil || result.Media != req.Media {
		rtcCallLinkError(w, http.StatusBadGateway, "WhatsApp call-link outcome unavailable; no automatic retry")
		return
	}
	if preview {
		if result.URL != "" || result.ApprovalRequired == nil || result.IsAdmin == nil {
			rtcCallLinkError(w, http.StatusBadGateway, "Incomplete WhatsApp call-link preview")
			return
		}
	} else {
		token, valid := rtcCallLinkToken(result.URL, result.Media)
		if !valid || result.URL != "https://call.whatsapp.com/"+result.Media+"/"+token || result.ApprovalRequired != nil || result.IsAdmin != nil {
			rtcCallLinkError(w, http.StatusBadGateway, "Invalid WhatsApp call-link result")
			return
		}
	}
	writeRTCCallLinkJSON(w, http.StatusOK, result)
}

// A join request prepares a dedicated room; it does not submit a WhatsApp
// join until the client's trusted MatrixRTC membership is admitted.
type rtcCallLinkRoomResult struct {
	RoomID string `json:"room_id"`
	Media  string `json:"media"`
}

func serveRTCCallLinkRoom(w http.ResponseWriter, r *http.Request, op func(context.Context, rtcCallLinkRequest) (rtcCallLinkRoomResult, error)) {
	req, ok := decodeRTCCallLinkRequest(w, r, true)
	if !ok {
		rtcCallLinkError(w, 400, "Invalid call-link request")
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()
	if ctx.Err() != nil {
		rtcCallLinkError(w, 504, "Call-link request cancelled")
		return
	}
	result, err := op(ctx, req)
	switch {
	case ctx.Err() != nil || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded):
		rtcCallLinkError(w, 504, "Call-link room outcome unavailable; no automatic retry")
	case errors.Is(err, errRTCCallLinkUnavailable):
		rtcCallLinkError(w, 503, "Calling is unavailable for this login")
	case errors.Is(err, errRTCCallLinkBusy):
		rtcCallLinkError(w, 409, "Call-link request already pending")
	case err != nil || result.Media != req.Media || !strings.HasPrefix(result.RoomID, "!") || len(result.RoomID) > 512:
		rtcCallLinkError(w, 502, "Call-link room outcome unavailable; no automatic retry")
	default:
		writeRTCCallLinkJSON(w, 200, result)
	}
}

func serveRTCCallLinkLeave(w http.ResponseWriter, r *http.Request, cancel func(string) bool) {
	d := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1024))
	start, err := d.Token()
	if err != nil || start != json.Delim('{') {
		rtcCallLinkError(w, 400, "Invalid call-link room")
		return
	}
	key, err := d.Token()
	var room string
	if err != nil || key != "room_id" || d.Decode(&room) != nil || !strings.HasPrefix(room, "!") || len(room) > 512 || strings.ContainsAny(room, " \t\r\n") {
		rtcCallLinkError(w, 400, "Invalid call-link room")
		return
	}
	end, err := d.Token()
	if err != nil || end != json.Delim('}') || d.Decode(new(any)) != io.EOF {
		rtcCallLinkError(w, 400, "Invalid call-link room")
		return
	}
	if r.Context().Err() != nil {
		rtcCallLinkError(w, 504, "Call-link request cancelled")
		return
	}
	if !cancel(room) {
		rtcCallLinkError(w, 404, "Call-link session not found")
		return
	}
	// Local cancellation initiated, never claim remote termination confirmed.
	writeRTCCallLinkJSON(w, 200, map[string]string{"outcome": "cancelled_locally"})
}
