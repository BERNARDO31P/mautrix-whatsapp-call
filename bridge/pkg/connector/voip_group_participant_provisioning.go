package connector

import (
	"context"
	"errors"
	"net/http"
	"sort"

	"github.com/purpshell/meowcaller"
	"go.mau.fi/whatsmeow/types"
	"maunium.net/go/mautrix/bridgev2"
)

func (wa *WhatsAppConnector) registerRTCGroupParticipantProvisioning() {
	if !wa.Config.ExperimentalWhatsAppVoIP || wa.Config.RTCGatewaySocket == "" || wa.Bridge.Background {
		return
	}
	matrix, ok := wa.Bridge.Matrix.(bridgev2.MatrixConnectorWithProvisioning)
	if !ok {
		return
	}
	prov := matrix.GetProvisioning()
	if prov == nil || prov.GetRouter() == nil {
		return
	}
	for _, route := range []struct {
		path string
		list bool
	}{{"GET /v3/whatsapp/group_call/active", true}, {"POST /v3/whatsapp/group_call/participant", false}} {
		list := route.list
		prov.GetRouter().HandleFunc(route.path, func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Cache-Control", "no-store")
			w.Header().Set("Pragma", "no-cache")
			login, failed := prov.GetExplicitLoginForRequest(w, r)
			if failed {
				return
			}
			if login == nil {
				rtcGroupParticipantError(w, 400, "Explicit login_id required")
				return
			}
			client, ok := login.Client.(*WhatsAppClient)
			if !ok || client == nil || client.Main != wa || client.UserLogin != login || login.UserMXID != prov.GetUser(r).MXID {
				rtcGroupParticipantError(w, 404, "Login not found")
				return
			}
			if list {
				if !client.groupParticipantAvailable() {
					rtcGroupParticipantError(w, 503, "Group calling unavailable for this login")
					return
				}
				writeRTCCallLinkJSON(w, 200, map[string]any{"calls": client.activeRTCGroupCalls()})
				return
			}
			serveRTCGroupParticipant(w, r, client.performRTCGroupParticipant)
		})
	}
}

func (wa *WhatsAppClient) groupParticipantAvailable() bool {
	return wa.rtcForegroundCalls && wa.voipClient != nil && wa.Client != nil && wa.Client.IsLoggedIn() && wa.Client.IsConnected()
}

func (wa *WhatsAppClient) activeRTCGroupCalls() []rtcGroupCallRef {
	wa.voipCallsMu.RLock()
	defer wa.voipCallsMu.RUnlock()
	result := make([]rtcGroupCallRef, 0)
	if !wa.voipAdmissionOpen {
		return result
	}
	for id, call := range wa.voipCalls {
		if call == nil || call.State() != meowcaller.CallPhaseActive || len(id) > 128 || !validRTCCallLinkToken(id) {
			continue
		}
		group, ok := call.GroupState()
		if !ok || group.GroupJID.IsEmpty() || group.GroupJID.Server != types.GroupServer {
			continue
		}
		result = append(result, rtcGroupCallRef{CallID: id, Group: group.GroupJID.String()})
	}
	sort.Slice(result, func(i, j int) bool { return result[i].CallID < result[j].CallID })
	return result
}

func (wa *WhatsAppClient) performRTCGroupParticipant(parent context.Context, req rtcGroupParticipantRequest) error {
	if !wa.groupParticipantAvailable() {
		return errRTCGroupUnavailable
	}
	if !wa.rtcGroupParticipantBusy.CompareAndSwap(false, true) {
		return errRTCGroupBusy
	}
	defer wa.rtcGroupParticipantBusy.Store(false)
	ctx, release, ok := wa.bindVoIPConnection(parent)
	if !ok {
		return errRTCGroupUnavailable
	}
	defer release()
	call, ok := wa.WhatsAppVoIPCall(req.CallID)
	if !ok || call == nil || call.State() != meowcaller.CallPhaseActive {
		return errRTCGroupNotAllowed
	}
	target, err := types.ParseJID(req.Target)
	if err != nil || wa.IsOwnJID(target) {
		return errRTCGroupNotAllowed
	}
	err = wa.voipClient.ExplicitGroupParticipant(ctx, call, target, req.Action == "ring")
	current, ok := wa.WhatsAppVoIPCall(req.CallID)
	if ctx.Err() != nil || !ok || current != call {
		return errRTCGroupUnknown
	}
	switch {
	case errors.Is(err, meowcaller.ErrGroupParticipantNotAllowed):
		return errRTCGroupNotAllowed
	case errors.Is(err, meowcaller.ErrGroupParticipantRejected):
		return errRTCGroupRejected
	case errors.Is(err, meowcaller.ErrGroupParticipantUnknown):
		return errRTCGroupUnknown
	default:
		return err
	}
}
