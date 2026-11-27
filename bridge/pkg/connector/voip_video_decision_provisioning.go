package connector

import (
	"context"
	"errors"
	"net/http"

	"github.com/purpshell/meowcaller"
	"maunium.net/go/mautrix/bridgev2"
)

func (wa *WhatsAppConnector) registerRTCVideoDecisionProvisioning() {
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
	prov.GetRouter().HandleFunc("POST /v3/whatsapp/call/video_decision", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "no-store")
		w.Header().Set("Pragma", "no-cache")
		login, failed := prov.GetExplicitLoginForRequest(w, r)
		if failed {
			return
		}
		if login == nil {
			rtcVideoDecisionError(w, 400, "Explicit login_id required")
			return
		}
		client, ok := login.Client.(*WhatsAppClient)
		if !ok || client == nil || client.Main != wa || client.UserLogin != login || login.UserMXID != prov.GetUser(r).MXID {
			rtcVideoDecisionError(w, 404, "Login not found")
			return
		}
		serveRTCVideoDecision(w, r, client.performRTCVideoDecision)
	})
}

func (wa *WhatsAppClient) performRTCVideoDecision(parent context.Context, req rtcVideoDecisionRequest) (rtcVideoDecisionResult, error) {
	if !wa.groupParticipantAvailable() {
		return rtcVideoDecisionResult{}, errRTCGroupUnavailable
	}
	ctx, release, ok := wa.bindVoIPConnection(parent)
	if !ok {
		return rtcVideoDecisionResult{}, errRTCGroupUnavailable
	}
	defer release()
	wa.voipCallsMu.RLock()
	call := wa.voipCalls[req.CallID]
	owned := wa.voipAdmissionOpen && call != nil && wa.voipMedia[req.CallID] != nil
	wa.voipCallsMu.RUnlock()
	if !owned || call.State() != meowcaller.CallPhaseActive {
		return rtcVideoDecisionResult{}, errRTCGroupNotAllowed
	}
	if err := ctx.Err(); err != nil {
		return rtcVideoDecisionResult{}, err
	}
	state, err := call.VideoNegotiationState(req.Action == "manual")
	if err != nil {
		return rtcVideoDecisionResult{}, errRTCGroupNotAllowed
	}
	outcome := "snapshot"
	if req.Action == "accept" || req.Action == "reject" {
		if !state.Manual || state.IncomingGeneration != req.Generation {
			return rtcVideoDecisionResult{}, errRTCGroupNotAllowed
		}
		err = call.DecideVideoUpgrade(ctx, req.Generation, req.Action == "accept")
		if errors.Is(err, meowcaller.ErrNoPendingVideoUpgrade) || errors.Is(err, meowcaller.ErrVideoDecisionMode) || errors.Is(err, meowcaller.ErrVideoDecisionBusy) {
			return rtcVideoDecisionResult{}, errRTCGroupNotAllowed
		}
		if err != nil {
			return rtcVideoDecisionResult{}, errRTCGroupUnknown
		}
		outcome = "submitted"
		state, err = call.VideoNegotiationState(false)
		if err != nil {
			return rtcVideoDecisionResult{}, errRTCGroupUnknown
		}
	}
	wa.voipCallsMu.RLock()
	defer wa.voipCallsMu.RUnlock()
	if ctx.Err() != nil || !wa.voipAdmissionOpen || wa.voipCalls[req.CallID] != call || wa.voipMedia[req.CallID] == nil || call.State() != meowcaller.CallPhaseActive {
		return rtcVideoDecisionResult{}, errRTCGroupUnknown
	}
	return rtcVideoDecisionResult{CallID: req.CallID, Manual: state.Manual, LocalCamera: state.LocalCamera, PeerCamera: state.PeerCamera, OutgoingPending: state.OutgoingPending, IncomingGeneration: state.IncomingGeneration, Outcome: outcome}, nil
}
