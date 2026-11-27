package connector

import (
	"context"
	"net/http"
	"time"

	"github.com/purpshell/meowcaller"
	"maunium.net/go/mautrix/bridgev2"
)

func (wa *WhatsAppConnector) registerRTCReactionProvisioning() {
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
	prov.GetRouter().HandleFunc("POST /v3/whatsapp/call/reaction", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "no-store")
		w.Header().Set("Pragma", "no-cache")
		login, failed := prov.GetExplicitLoginForRequest(w, r)
		if failed {
			return
		}
		if login == nil {
			rtcReactionError(w, 400, "Explicit login_id required")
			return
		}
		client, ok := login.Client.(*WhatsAppClient)
		if !ok || client == nil || client.Main != wa || client.UserLogin != login || login.UserMXID != prov.GetUser(r).MXID {
			rtcReactionError(w, 404, "Login not found")
			return
		}
		serveRTCReaction(w, r, client.performRTCReaction)
	})
}

func (wa *WhatsAppClient) receiveRTCReaction(call *meowcaller.Call, feed *rtcReactionFeed, reaction meowcaller.CallReaction) {
	sender := reaction.Sender.ToNonAD()
	if sender.IsEmpty() || wa.IsOwnJID(sender) || !validRTCGroupTarget(sender.String()) {
		return
	}
	wa.voipCallsMu.RLock()
	defer wa.voipCallsMu.RUnlock()
	if !wa.voipAdmissionOpen || wa.voipCalls[call.ID()] != call || wa.voipReactionFeeds[call.ID()] != feed || call.State() != meowcaller.CallPhaseActive {
		return
	}
	var roster uint32
	if group, ok := call.GroupState(); ok {
		roster = group.TransactionID
		if roster == 0 {
			return
		}
		matches := 0
		canonical := sender.String()
		for _, p := range group.Participants {
			if p.JID.ToNonAD() == sender || (!p.PN.IsEmpty() && p.PN.ToNonAD() == sender) {
				if p.State != "connected" || wa.IsOwnJID(p.JID) || (!p.PN.IsEmpty() && wa.IsOwnJID(p.PN)) {
					return
				}
				matches++
				senderID := p.JID.ToNonAD()
				canonical = senderID.String()
				if !validRTCGroupTarget(canonical) {
					return
				}
			}
		}
		if matches != 1 {
			return
		}
		// Canonicalize only an exact, unique current roster alias.
		feed.append(time.Now(), roster, canonical, reaction.Emoji, reaction.Removed)
		return
	} else if sender != call.Peer().ToNonAD() {
		return
	}
	feed.append(time.Now(), roster, sender.String(), reaction.Emoji, reaction.Removed)
}

func (wa *WhatsAppClient) performRTCReaction(parent context.Context, req rtcReactionRequest) (rtcReactionResult, error) {
	if !wa.groupParticipantAvailable() {
		return rtcReactionResult{}, errRTCGroupUnavailable
	}
	ctx, release, ok := wa.bindVoIPConnection(parent)
	if !ok {
		return rtcReactionResult{}, errRTCGroupUnavailable
	}
	defer release()
	wa.voipCallsMu.RLock()
	call, feed := wa.voipCalls[req.CallID], wa.voipReactionFeeds[req.CallID]
	wa.voipCallsMu.RUnlock()
	if call == nil || feed == nil || call.State() != meowcaller.CallPhaseActive {
		return rtcReactionResult{}, errRTCGroupNotAllowed
	}
	if req.Emoji != nil {
		if group, ok := call.GroupState(); ok {
			own := 0
			if group.TransactionID == 0 {
				return rtcReactionResult{}, errRTCGroupNotAllowed
			}
			for _, p := range group.Participants {
				if wa.IsOwnJID(p.JID) || (!p.PN.IsEmpty() && wa.IsOwnJID(p.PN)) {
					if p.State != "connected" {
						return rtcReactionResult{}, errRTCGroupNotAllowed
					}
					own++
				}
			}
			if own != 1 {
				return rtcReactionResult{}, errRTCGroupNotAllowed
			}
		}
		if err := feed.begin(time.Now(), req.Stream); err != nil {
			return rtcReactionResult{}, err
		}
		defer feed.finish()
		if err := ctx.Err(); err != nil {
			return rtcReactionResult{}, err
		}
		if err := call.SendReactionContext(ctx, *req.Emoji); err != nil {
			return rtcReactionResult{}, errRTCGroupUnknown
		}
	}
	wa.voipCallsMu.RLock()
	defer wa.voipCallsMu.RUnlock()
	if ctx.Err() != nil || !wa.voipAdmissionOpen || wa.voipCalls[req.CallID] != call || wa.voipReactionFeeds[req.CallID] != feed || call.State() != meowcaller.CallPhaseActive {
		return rtcReactionResult{}, errRTCGroupUnknown
	}
	var roster uint32
	if group, ok := call.GroupState(); ok {
		roster = group.TransactionID
		if roster == 0 {
			return rtcReactionResult{}, errRTCGroupNotAllowed
		}
	}
	out := feed.snapshot(time.Now(), roster, req)
	if req.Emoji != nil {
		out.Outcome = "submitted"
	}
	return out, nil
}
