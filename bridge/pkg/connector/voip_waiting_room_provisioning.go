package connector

import (
	"context"
	"errors"
	"sort"

	"github.com/purpshell/meowcaller"
)

func (wa *WhatsAppClient) bindRTCLinkCall(room, slot, event string, call *meowcaller.Call) bool {
	key := rtcSessionKey(room, slot)
	main := wa.Main
	main.rtcMu.Lock()
	defer main.rtcMu.Unlock()
	session, ok := main.rtcOutgoing[key]
	if !ok || session.linkOwner != wa || session.eventID != event || session.ctx.Err() != nil || session.linkCall != nil || call == nil {
		return false
	}
	session.linkCall = call
	main.rtcOutgoing[key] = session
	return true
}

func (wa *WhatsAppClient) rtcWaitingRoomSession(room string) (rtcOutgoingSession, bool) {
	main := wa.Main
	main.rtcMu.Lock()
	defer main.rtcMu.Unlock()
	session, ok := main.rtcOutgoing[rtcSessionKey(room, matrixRTCSlotDefault)]
	return session, ok && session.linkOwner == wa && session.sender == wa.UserLogin.UserMXID.String() && session.linkCall != nil && session.ctx.Err() == nil
}

func (wa *WhatsAppClient) performRTCWaitingRoom(parent context.Context, req rtcWaitingRoomRequest) (rtcWaitingRoomResult, error) {
	var result rtcWaitingRoomResult
	if !wa.groupParticipantAvailable() {
		return result, errRTCCallLinkUnavailable
	}
	if req.Action != "snapshot" {
		if !wa.rtcCallLinkBusy.CompareAndSwap(false, true) {
			return result, errRTCCallLinkBusy
		}
		defer wa.rtcCallLinkBusy.Store(false)
	}
	session, ok := wa.rtcWaitingRoomSession(req.RoomID)
	if !ok {
		return result, errRTCGroupNotAllowed
	}
	ctx, release, ok := wa.bindVoIPConnection(parent)
	if !ok {
		return result, errRTCCallLinkUnavailable
	}
	defer release()
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	stop := context.AfterFunc(session.ctx, cancel)
	defer stop()
	call := session.linkCall
	current, ok := wa.WhatsAppVoIPCall(call.ID())
	if !ok || current != call || ctx.Err() != nil || session.ctx.Err() != nil {
		return result, errRTCGroupNotAllowed
	}
	if req.Action != "snapshot" {
		err := call.SubmitWaitingRoomControl(ctx, req.Generation, req.Action, req.Target, req.Enabled)
		if errors.Is(err, meowcaller.ErrWaitingRoomStale) {
			return result, errRTCGroupNotAllowed
		}
		if err != nil {
			return result, errRTCGroupUnknown
		}
	}
	state, generation, ok := call.WaitingRoomSnapshot()
	if !ok {
		return result, errRTCGroupUnknown
	}
	admitted := call.IsCallLinkAdmitted()
	result = rtcWaitingRoomResult{RoomID: req.RoomID, Generation: generation, Transaction: state.TransactionID, Enabled: state.Enabled, Admin: state.IsAdmin, Waiting: state.InWaitingRoom, Admitted: admitted, Pending: []string{}, Outcome: "snapshot"}
	if state.IsAdmin && admitted && !state.InWaitingRoom {
		if len(state.Users) > 256 {
			return rtcWaitingRoomResult{}, errRTCGroupNotAllowed
		}
		seen := map[string]bool{}
		for _, user := range state.Users {
			jid := user.JID.ToNonAD().String()
			if !validRTCGroupTarget(jid) || seen[jid] {
				return rtcWaitingRoomResult{}, errRTCGroupNotAllowed
			}
			seen[jid] = true
			if user.State == "waiting_room_joined" {
				result.Pending = append(result.Pending, jid)
			}
		}
		sort.Strings(result.Pending)
	}
	currentSession, ok := wa.rtcWaitingRoomSession(req.RoomID)
	current, registered := wa.WhatsAppVoIPCall(call.ID())
	_, latestGeneration, latestOK := call.WaitingRoomSnapshot()
	if !latestOK || latestGeneration != generation || ctx.Err() != nil || session.ctx.Err() != nil || !ok || currentSession.linkCall != call || currentSession.eventID != session.eventID || !registered || current != call {
		return rtcWaitingRoomResult{}, errRTCGroupUnknown
	}
	if req.Action != "snapshot" {
		result.Outcome = "submitted"
	}
	return result, nil
}
