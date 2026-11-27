package connector

import (
	"context"
	"net/http"
	"time"

	"github.com/purpshell/meowcaller"
	"maunium.net/go/mautrix"
	"maunium.net/go/mautrix/bridgev2"
	bridgev2matrix "maunium.net/go/mautrix/bridgev2/matrix"
	"maunium.net/go/mautrix/event"
	"maunium.net/go/mautrix/id"
)

// Register on the existing authenticated provisioning router, not the public
// appservice router. The pinned bridge starts Matrix before Network.Start.
func (wa *WhatsAppConnector) registerRTCCallLinkProvisioning() {
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
	prov.GetRouter().HandleFunc("POST /v3/whatsapp/call_link/waiting_room", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "no-store")
		w.Header().Set("Pragma", "no-cache")
		login, failed := prov.GetExplicitLoginForRequest(w, r)
		if failed {
			return
		}
		if login == nil {
			rtcCallLinkError(w, 400, "Explicit login_id required")
			return
		}
		client, ok := login.Client.(*WhatsAppClient)
		if !ok || client == nil || client.Main != wa || client.UserLogin != login || login.UserMXID != prov.GetUser(r).MXID {
			rtcCallLinkError(w, 404, "Login not found")
			return
		}
		serveRTCWaitingRoom(w, r, client.performRTCWaitingRoom)
	})
	for _, route := range []struct {
		path    string
		preview bool
		join    bool
		leave   bool
	}{
		{"POST /v3/whatsapp/call_link/create", false, false, false},
		{"POST /v3/whatsapp/call_link/preview", true, false, false},
		{"POST /v3/whatsapp/call_link/join", true, true, false},
		{"POST /v3/whatsapp/call_link/leave", false, false, true},
	} {
		preview, join, leave := route.preview, route.join, route.leave
		prov.GetRouter().HandleFunc(route.path, func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Cache-Control", "no-store")
			w.Header().Set("Pragma", "no-cache")
			// Explicit login selection is mandatory; never silently use another account.
			login, failed := prov.GetExplicitLoginForRequest(w, r)
			if failed {
				return
			}
			if login == nil {
				rtcCallLinkError(w, http.StatusBadRequest, "Explicit login_id required")
				return
			}
			client, ok := login.Client.(*WhatsAppClient)
			if !ok || client == nil || client.Main != wa || client.UserLogin != login || login.UserMXID != prov.GetUser(r).MXID {
				rtcCallLinkError(w, http.StatusNotFound, "Login not found")
				return
			}
			if leave {
				serveRTCCallLinkLeave(w, r, client.cancelRTCCallLinkRoom)
				return
			}
			if join {
				serveRTCCallLinkRoom(w, r, client.prepareRTCCallLinkRoom)
				return
			}
			serveRTCCallLink(w, r, preview, func(ctx context.Context, req rtcCallLinkRequest) (rtcCallLinkResult, error) {
				return client.performRTCCallLink(ctx, req, preview)
			})
		})
	}
}

func (wa *WhatsAppClient) performRTCCallLink(parent context.Context, req rtcCallLinkRequest, preview bool) (rtcCallLinkResult, error) {
	var result rtcCallLinkResult
	if !wa.rtcForegroundCalls || wa.voipClient == nil || wa.Client == nil || !wa.Client.IsLoggedIn() || !wa.Client.IsConnected() {
		return result, errRTCCallLinkUnavailable
	}
	if !wa.rtcCallLinkBusy.CompareAndSwap(false, true) {
		return result, errRTCCallLinkBusy
	}
	defer wa.rtcCallLinkBusy.Store(false)
	ctx, release, ok := wa.bindVoIPConnection(parent)
	if !ok {
		return result, errRTCCallLinkUnavailable
	}
	defer release()
	if err := ctx.Err(); err != nil {
		return result, err
	}
	opts := meowcaller.CallLinkOptions{Video: req.Media == "video"}
	if preview {
		metadata, err := wa.voipClient.PreviewCallLink(ctx, req.Link, opts)
		if err != nil {
			return result, err
		}
		if metadata.Token != req.Link || metadata.Video != opts.Video {
			return result, errRTCCallLinkInvalidResult
		}
		result = rtcCallLinkResult{Media: req.Media, ApprovalRequired: &metadata.ApprovalRequired, IsAdmin: &metadata.IsAdmin}
	} else {
		link, err := wa.voipClient.CreateCallLink(ctx, opts)
		if err != nil {
			return result, err
		}
		if !validRTCCallLinkToken(link.Token) || link.Video != opts.Video || link.URL != "https://call.whatsapp.com/"+req.Media+"/"+link.Token {
			return result, errRTCCallLinkInvalidResult
		}
		result = rtcCallLinkResult{Media: req.Media, URL: link.URL}
	}
	if err := ctx.Err(); err != nil {
		return rtcCallLinkResult{}, err
	}
	return result, nil
}

// Create a separate room, never a portal or the existing management DM. The
// client joins this invitation and sends an encrypted RTC notification with
// group_call_mode=call_link; the provider join waits for worker admission.
func (wa *WhatsAppClient) prepareRTCCallLinkRoom(parent context.Context, req rtcCallLinkRequest) (rtcCallLinkRoomResult, error) {
	var result rtcCallLinkRoomResult
	if !wa.rtcForegroundCalls || wa.voipClient == nil || wa.Client == nil || !wa.Client.IsLoggedIn() || !wa.Client.IsConnected() {
		return result, errRTCCallLinkUnavailable
	}
	if !wa.rtcCallLinkBusy.CompareAndSwap(false, true) {
		return result, errRTCCallLinkBusy
	}
	defer wa.rtcCallLinkBusy.Store(false)
	generation, open := wa.voipOfferGeneration()
	ctx, release, ok := wa.bindVoIPConnection(parent)
	if !open || !ok {
		if ok {
			release()
		}
		return result, errRTCCallLinkUnavailable
	}
	defer release()
	if !wa.voipAdmissionCurrent(generation) {
		return result, errRTCCallLinkUnavailable
	}
	mx, ok := wa.Main.Bridge.Matrix.(*bridgev2matrix.Connector)
	if !ok || mx.Bot == nil {
		return result, errRTCCallLinkUnavailable
	}
	user, bot := wa.UserLogin.UserMXID, wa.Main.Bridge.Bot.GetMXID()
	if user == "" || bot == "" || user == bot || user.Homeserver() != bot.Homeserver() || wa.Main.Bridge.IsGhostMXID(user) {
		return result, errRTCCallLinkUnavailable
	}
	if !wa.Main.rtcLinkRooms.available(wa, time.Now()) {
		return result, errRTCCallLinkBusy
	}
	// Preview validates the media/token against the provider without joining.
	metadata, err := wa.voipClient.PreviewCallLink(ctx, req.Link, meowcaller.CallLinkOptions{Video: req.Media == "video"})
	if err != nil {
		return result, err
	}
	if metadata.Token != req.Link || metadata.Video != (req.Media == "video") || ctx.Err() != nil {
		return result, errRTCCallLinkInvalidResult
	}
	empty := ""
	powers := map[string]int{}
	for typ, level := range matrixRTCDMPowerLevels(nil) {
		powers[typ.Type] = level
	}
	room, err := mx.Bot.CreateRoom(ctx, &mautrix.ReqCreateRoom{
		Visibility: "private", Preset: "private_chat", Name: "WhatsApp call link", Invite: []id.UserID{user},
		InitialState:       []*event.Event{{Type: event.StateEncryption, StateKey: &empty, Content: event.Content{Raw: map[string]any{"algorithm": "m.megolm.v1.aes-sha2"}}}},
		CreationContent:    map[string]any{"m.federate": false},
		PowerLevelOverride: &event.PowerLevelsEventContent{Events: powers},
	})
	if err != nil || room == nil || room.RoomID == "" {
		return result, errRTCCallLinkInvalidResult
	}
	now := time.Now()
	if ctx.Err() != nil || !wa.voipAdmissionCurrent(generation) {
		return result, errRTCCallLinkUnavailable
	}
	if !wa.Main.rtcLinkRooms.put(room.RoomID.String(), rtcLinkReservation[*WhatsAppClient]{owner: wa, sender: user.String(), generation: generation, token: req.Link, media: req.Media, expires: now.Add(5 * time.Minute)}, now) {
		return result, errRTCCallLinkBusy
	}
	return rtcCallLinkRoomResult{RoomID: room.RoomID.String(), Media: req.Media}, nil
}

// Use the normal outgoing worker admission with the bot as the aggregate peer
// in an exclusive room. A link never becomes a chat-bound group or portal.
func (wa *WhatsAppConnector) completeRTCCallLinkInvite(ctx context.Context, evt *event.Event, content rtcInviteContent, notificationType string, device id.DeviceID, deadline time.Time, format, memberEvent, memberJoinID string, memberJoinTS uint64) {
	if notificationType != "notification" || device == "" || content.SlotID != matrixRTCSlotDefault || memberEvent == "" || memberJoinID == "" {
		return
	}
	reservation, ok := wa.rtcLinkRooms.peek(evt.RoomID.String(), evt.Sender.String(), time.Now())
	if !ok || reservation.owner == nil {
		return
	}
	client := reservation.owner
	if client.Main != wa || client.UserLogin == nil || client.UserLogin.UserMXID != evt.Sender || !client.voipAdmissionCurrent(reservation.generation) {
		return
	}
	intent, valid := rtcInvitationIntent(content.Intent, content.Application.Intent)
	if !valid || intent != reservation.media {
		return
	}
	if portal, err := wa.Bridge.DB.Portal.GetByMXID(ctx, evt.RoomID); err != nil || portal != nil {
		return
	}
	bot := wa.Bridge.Bot.GetMXID()
	if !mentionsGhost(&content, bot.String()) {
		return
	}
	mx, ok := wa.Bridge.Matrix.(*bridgev2matrix.Connector)
	if !ok || mx.Bot == nil {
		return
	}
	members, err := mx.Bot.JoinedMembers(ctx, evt.RoomID)
	if err != nil || len(members.Joined) != 2 {
		return
	}
	if _, ok := members.Joined[bot]; !ok {
		return
	}
	if _, ok := members.Joined[evt.Sender]; !ok {
		return
	}
	callCtx, cancel, admitted := client.bindVoIPConnection(wa.Bridge.BackgroundCtx)
	if !admitted {
		return
	}
	key := rtcSessionKey(evt.RoomID.String(), content.SlotID)
	wa.rtcMu.Lock()
	if ctx.Err() != nil || callCtx.Err() != nil || !time.Now().Before(deadline) || !client.voipAdmissionCurrent(reservation.generation) {
		wa.rtcMu.Unlock()
		cancel()
		return
	}
	if _, exists := wa.rtcOutgoing[key]; exists || !wa.rtcLinkRooms.consume(evt.RoomID.String(), reservation, time.Now()) {
		wa.rtcMu.Unlock()
		cancel()
		return
	}
	if wa.rtcOutgoing == nil {
		wa.rtcOutgoing = make(map[string]rtcOutgoingSession)
	}
	wa.rtcOutgoing[key] = rtcOutgoingSession{linkOwner: client, ctx: callCtx, eventID: evt.ID.String(), sender: evt.Sender.String(), device: device, cancel: cancel}
	wa.rtcMu.Unlock()
	trust := evt.Mautrix.TrustSource
	go func() {
		defer cancel()
		defer wa.finishMatrixRTCOutgoing(key, evt.ID.String())
		// Group is true only locally. The worker sees an exclusive two-party
		// encrypted room, not a WhatsApp chat JID; no follow-up group invitations.
		_ = client.runOutgoingRTCGateway(callCtx, evt.ID.String(), evt.RoomID.String(), content.SlotID, format, bot.String(), evt.Sender.String(), string(device), string(trust.SigningKey), string(trust.IdentityKey), memberEvent, memberJoinID, memberJoinTS, "call-link", true, "", intent, deadline, nil, nil, true, reservation.token)
	}()
}

func (wa *WhatsAppClient) cancelRTCCallLinkRoom(room string) bool {
	// Serialize with the one-use reservation to active-session handoff.
	main := wa.Main
	main.rtcMu.Lock()
	pending := main.rtcLinkRooms.remove(room, wa)
	key := rtcSessionKey(room, matrixRTCSlotDefault)
	active, ok := main.rtcOutgoing[key]
	ok = ok && active.linkOwner == wa
	if ok {
		delete(main.rtcOutgoing, key)
	}
	main.rtcMu.Unlock()
	if ok {
		active.cancel()
	}
	return pending || ok
}
