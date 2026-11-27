package connector

import (
	"context"
	"net/http"

	"github.com/purpshell/meowcaller"
	"maunium.net/go/mautrix/bridgev2"
)

func (wa *WhatsAppConnector) registerRTCGroupHandProvisioning() {
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
	for _, route := range []string{"POST /v3/whatsapp/group_call/hand", "POST /v3/whatsapp/group_call/roster"} {
		roster := route == "POST /v3/whatsapp/group_call/roster"
		prov.GetRouter().HandleFunc(route, func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Cache-Control", "no-store")
			w.Header().Set("Pragma", "no-cache")
			login, failed := prov.GetExplicitLoginForRequest(w, r)
			if failed {
				return
			}
			if login == nil {
				rtcGroupHandError(w, 400, "Explicit login_id required")
				return
			}
			client, ok := login.Client.(*WhatsAppClient)
			if !ok || client == nil || client.Main != wa || client.UserLogin != login || login.UserMXID != prov.GetUser(r).MXID {
				rtcGroupHandError(w, 404, "Login not found")
				return
			}
			if roster {
				serveRTCGroupRoster(w, r, client.performRTCGroupRoster)
				return
			}
			serveRTCGroupHand(w, r, client.performRTCGroupHand)
		})
	}
}

func (wa *WhatsAppClient) groupHandSnapshot(call *meowcaller.Call) (rtcGroupHandResult, error) {
	if call == nil || call.State() != meowcaller.CallPhaseActive {
		return rtcGroupHandResult{}, errRTCGroupNotAllowed
	}
	group, ok := call.GroupState()
	if !ok {
		return rtcGroupHandResult{}, errRTCGroupNotAllowed
	}
	if wa.Client == nil || wa.Client.Store == nil {
		return rtcGroupHandResult{}, errRTCGroupUnavailable
	}
	own := wa.Client.Store.GetLID().ToNonAD()
	if own.IsEmpty() || len(group.Participants) > 256 {
		return rtcGroupHandResult{}, errRTCGroupNotAllowed
	}
	rows := make([]rtcGroupHandParticipant, 0, len(group.Participants))
	aliases := map[string]int{}
	for index, participant := range group.Participants {
		jid := participant.JID.ToNonAD()
		identities := []string{jid.String()}
		if !participant.PN.IsEmpty() {
			identities = append(identities, participant.PN.ToNonAD().String())
		}
		for _, identity := range identities {
			if previous, exists := aliases[identity]; !validRTCGroupTarget(identity) || (exists && previous != index) {
				return rtcGroupHandResult{}, errRTCGroupNotAllowed
			}
			aliases[identity] = index
		}
		if participant.State != "connected" {
			continue
		}
		rows = append(rows, rtcGroupHandParticipant{ParticipantID: jid.String(), Self: jid == own, Raised: participant.HandRaised})
	}
	return rtcGroupHandSnapshot(call.ID(), group.TransactionID, group.HandRevision, rows)
}

func (wa *WhatsAppClient) performRTCGroupHand(parent context.Context, req rtcGroupHandRequest) (rtcGroupHandResult, error) {
	if !wa.groupParticipantAvailable() {
		return rtcGroupHandResult{}, errRTCGroupUnavailable
	}
	if req.Raised != nil {
		if !wa.rtcGroupHandBusy.CompareAndSwap(false, true) {
			return rtcGroupHandResult{}, errRTCGroupBusy
		}
		defer wa.rtcGroupHandBusy.Store(false)
	}
	ctx, release, ok := wa.bindVoIPConnection(parent)
	if !ok {
		return rtcGroupHandResult{}, errRTCGroupUnavailable
	}
	defer release()
	call, ok := wa.WhatsAppVoIPCall(req.CallID)
	if !ok {
		return rtcGroupHandResult{}, errRTCGroupNotAllowed
	}
	result, err := wa.groupHandSnapshot(call)
	if err != nil {
		return rtcGroupHandResult{}, err
	}
	if ctx.Err() != nil {
		return rtcGroupHandResult{}, ctx.Err()
	}
	if req.Raised != nil {
		if err = call.SetHandRaisedContext(ctx, *req.Raised); err != nil {
			return rtcGroupHandResult{}, errRTCGroupUnknown
		}
		result, err = wa.groupHandSnapshot(call)
		if err != nil {
			return rtcGroupHandResult{}, errRTCGroupUnknown
		}
		result.Outcome = "submitted"
	}
	current, ok := wa.WhatsAppVoIPCall(req.CallID)
	if ctx.Err() != nil || !ok || current != call || call.State() != meowcaller.CallPhaseActive {
		return rtcGroupHandResult{}, errRTCGroupUnknown
	}
	return result, nil
}
