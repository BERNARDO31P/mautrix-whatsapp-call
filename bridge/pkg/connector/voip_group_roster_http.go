package connector

import (
	"context"
	"errors"
	"net/http"
	"sort"
	"strings"
	"time"
)

type rtcGroupRosterDevice struct {
	ID         string `json:"device_id"`
	Microphone string `json:"microphone"`
	Camera     string `json:"camera"`
}
type rtcGroupRosterParticipant struct {
	ID      string                 `json:"participant_id"`
	State   string                 `json:"state"`
	Self    bool                   `json:"self"`
	Devices []rtcGroupRosterDevice `json:"devices"`
}
type rtcGroupRosterResult struct {
	CallID              string                      `json:"call_id"`
	Transaction         uint32                      `json:"roster_transaction"`
	Revision            uint64                      `json:"media_revision"`
	Participants        []rtcGroupRosterParticipant `json:"participants"`
	MediaRepresentation string                      `json:"media_representation"`
}

func rosterDeviceMatches(participant, device string) bool {
	if device == participant {
		return true
	}
	p, server, _ := strings.Cut(participant, "@")
	user, ds, ok := strings.Cut(device, "@")
	base, number, hasDevice := strings.Cut(user, ":")
	if !ok || !hasDevice || ds != server || base != p || len(number) == 0 || len(number) > 5 {
		return false
	}
	for _, c := range number {
		if c < '0' || c > '9' {
			return false
		}
	}
	return true
}

// Validate a bounded replacement snapshot. Unknown is distinct from off/muted;
// the participant roster is not a set of MatrixRTC memberships or media tracks.
func validateRTCGroupRoster(r *rtcGroupRosterResult) error {
	if r.Transaction == 0 || r.Revision == 0 || len(r.Participants) == 0 || len(r.Participants) > 256 {
		return errRTCGroupNotAllowed
	}
	own, total := 0, 0
	ids, devices := map[string]bool{}, map[string]bool{}
	for i := range r.Participants {
		p := &r.Participants[i]
		if !validRTCGroupTarget(p.ID) || ids[p.ID] || len(p.State) > 32 || len(p.Devices) > 32 {
			return errRTCGroupNotAllowed
		}
		ids[p.ID] = true
		if p.Self && p.State == "connected" {
			own++
		}
		if p.Devices == nil {
			p.Devices = []rtcGroupRosterDevice{}
		}
		for j := range p.Devices {
			d := &p.Devices[j]
			total++
			if total > 256 || !rosterDeviceMatches(p.ID, d.ID) || devices[d.ID] {
				return errRTCGroupNotAllowed
			}
			devices[d.ID] = true
			if p.State != "connected" || d.Microphone == "" {
				d.Microphone = "unknown"
			}
			if p.State != "connected" || d.Camera == "" {
				d.Camera = "unknown"
			}
			if d.Microphone != "unknown" && d.Microphone != "muted" && d.Microphone != "unmuted" || d.Camera != "unknown" && d.Camera != "on" && d.Camera != "off" {
				return errRTCGroupNotAllowed
			}
		}
		sort.Slice(p.Devices, func(a, b int) bool { return p.Devices[a].ID < p.Devices[b].ID })
	}
	if own != 1 {
		return errRTCGroupNotAllowed
	}
	sort.Slice(r.Participants, func(a, b int) bool { return r.Participants[a].ID < r.Participants[b].ID })
	r.MediaRepresentation = "aggregate"
	return nil
}

func serveRTCGroupRoster(w http.ResponseWriter, r *http.Request, operation func(context.Context, string) (rtcGroupRosterResult, error)) {
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("Pragma", "no-cache")
	req, ok := decodeRTCGroupHand(w, r)
	if !ok || req.Raised != nil {
		rtcGroupParticipantError(w, 400, "Invalid roster request")
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	if ctx.Err() != nil {
		rtcGroupParticipantError(w, 504, "Roster request cancelled")
		return
	}
	result, err := operation(ctx, req.CallID)
	if ctx.Err() != nil || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) || errors.Is(err, errRTCGroupUnknown) {
		rtcGroupParticipantError(w, 504, "Roster unavailable; refresh")
		return
	}
	if errors.Is(err, errRTCGroupUnavailable) {
		rtcGroupParticipantError(w, 503, "Group calling unavailable for this login")
		return
	}
	if errors.Is(err, errRTCGroupNotAllowed) {
		rtcGroupParticipantError(w, 409, "No unambiguous active roster")
		return
	}
	if err != nil {
		rtcGroupParticipantError(w, 502, "Roster unavailable")
		return
	}
	if result.CallID != req.CallID || validateRTCGroupRoster(&result) != nil {
		rtcGroupParticipantError(w, 409, "No unambiguous active roster")
		return
	}
	writeRTCCallLinkJSON(w, 200, result)
}
