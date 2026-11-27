package connector

import (
	"context"
	"errors"
	"github.com/purpshell/meowcaller"
	"sync"
	"time"

	"go.mau.fi/mautrix-whatsapp/pkg/connector/voipbridge"
	"go.mau.fi/mautrix-whatsapp/pkg/connector/voipbridge/rtcwire"
)

const participantNegotiationBudget = 45 * time.Second

// Only the established gateway's existing reader owns the transition. The
// aggregate media pumps have a separate read cancellation, but their writes
// remain governed by the parent session until the entire frame finishes.
// A roster proposal is the ordered barrier after the last aggregate frame.
// The worker must suppress aggregate publications and admit the exact roster
// before acknowledging it. No fallback to aggregate media after the barrier.
type gatewayParticipantHandoff struct {
	wa             *WhatsAppClient
	ctx            context.Context
	stop           context.CancelFunc
	wire           *rtcwire.Peer
	callID         string
	bus            *voipbridge.MediaBus
	call           *meowcaller.Call
	eligible       bool
	stopReads      context.CancelFunc
	pumps          sync.WaitGroup
	offer          *participantNegotiation
	deadline       *time.Timer
	active         bool
	finished       chan error
	lastGeneration uint64
}

func (wa *WhatsAppClient) startGatewayParticipantHandoff(ctx context.Context, stop context.CancelFunc, wire *rtcwire.Peer, callID string, bus *voipbridge.MediaBus, call *meowcaller.Call, eligible bool) *gatewayParticipantHandoff {
	if !eligible {
		// Keep direct calls and call links on their original pump path.
		go wa.pumpWhatsAppAudio(ctx, stop, wire, callID, bus)
		go wa.pumpWhatsAppVideo(ctx, stop, wire, callID, bus)
		return &gatewayParticipantHandoff{eligible: false}
	}
	reads, cancel := context.WithCancel(ctx)
	h := &gatewayParticipantHandoff{wa: wa, ctx: ctx, stop: stop, wire: wire, callID: callID, bus: bus, call: call, eligible: eligible, stopReads: cancel}
	h.pumps.Add(2)
	go func() { defer h.pumps.Done(); wa.pumpWhatsAppAudioUntil(reads, ctx, stop, wire, callID, bus) }()
	go func() { defer h.pumps.Done(); wa.pumpWhatsAppVideoUntil(reads, ctx, stop, wire, callID, bus) }()
	return h
}

func (h *gatewayParticipantHandoff) handle(msg rtcwire.Message) error {
	if h == nil || !h.eligible || h.ctx.Err() != nil || msg.CallID != h.callID {
		return errors.New("participant negotiation outside an admitted group gateway")
	}
	switch msg.Type {
	case "matrix_participant_request":
		if !rtcwire.IsParticipantRequest(msg, h.callID) {
			return errors.New("invalid participant request")
		}
		if h.active {
			select {
			case err := <-h.finished:
				if !errors.Is(err, voipbridge.ErrParticipantEpochRetired) {
					return errors.New("previous participant sender did not retire")
				}
				h.active = false
				h.offer = nil
				h.finished = nil
			case <-h.ctx.Done():
				return h.ctx.Err()
			case <-time.After(5 * time.Second):
				return errors.New("previous participant sender still active")
			}
		}
		if h.offer != nil {
			return errors.New("invalid or repeated participant negotiation request")
		}
		// Snapshot before touching the established aggregate pumps. A missing,
		// ambiguous or not-yet-authoritative roster is a retryable refusal.
		proposal, revision, _, err := h.wa.participantRosterProposal(h.bus, h.callID)
		if err != nil || proposal.Generation <= h.lastGeneration || !h.bus.ParticipantRosterCurrent(proposal.Generation, revision) {
			return h.wire.SendContext(h.ctx, rtcwire.Message{Type: "whatsapp_participant_unavailable", CallID: h.callID, Group: true})
		}
		// Keep this timeout active until the exact acknowledgement is read;
		// a worker that stops reading or never responds must end the call.
		// Stop the timer on ordinary parent cancellation as well.
		h.deadline = time.AfterFunc(participantNegotiationBudget, h.stop)
		timer := h.deadline
		context.AfterFunc(h.ctx, func() { timer.Stop() })
		h.stopReads()
		h.pumps.Wait()
		if err := h.ctx.Err(); err != nil {
			return err
		}
		offer, err := beginParticipantNegotiation(h.ctx, h.wire, h.bus, h.callID, msg, proposal, revision)
		if err != nil {
			return err
		}
		h.offer = offer
		h.lastGeneration = proposal.Generation
		return nil
	case "matrix_participant_ready":
		if h.offer == nil || h.active || !rtcwire.MatchesParticipantReady(h.offer.proposal, msg) {
			return errors.New("unexpected participant readiness")
		}
		if !h.deadline.Stop() || h.ctx.Err() != nil {
			return errors.New("participant negotiation expired")
		}
		h.active = true
		// The single gateway reader remains available for Matrix media and
		// terminal controls. The independent sender never reads the socket.
		// Retirement or a failed write ends this call; there is no implicit
		// re-admission or aggregate fallback on a newer provider epoch.
		offer := h.offer
		finished := make(chan error, 1)
		h.finished = finished
		go func() {
			err := offer.Run(h.ctx, msg, h.call)
			finished <- err
			if errors.Is(err, voipbridge.ErrParticipantEpochRetired) {
				return
			}
			defer h.stop()
			if err != nil && h.ctx.Err() == nil {
				h.wa.UserLogin.Log.Warn().Err(err).Str("call_id", h.callID).Msg("Independent group media ended")
			}
		}()
		return nil
	case "matrix_participant_keyframe":
		if !h.active || h.offer == nil || !rtcwire.ValidParticipantKeyframe(msg) || msg.Generation != h.offer.proposal.Generation {
			return errors.New("invalid participant keyframe request")
		}
		source := msg.ParticipantSource
		for _, binding := range h.offer.proposal.ParticipantBindings {
			if binding.ParticipantID == source.ParticipantID && binding.Device == source.Device {
				// A subscription may retire while this request is in flight.
				// Never fall back to a different device or aggregate source.
				h.bus.RequestParticipantKeyframe(voipbridge.ParticipantMediaKey{ParticipantID: source.ParticipantID, Device: source.Device, SSRC: source.SSRC, Video: true}, msg.Generation, h.offer.revision)
				return nil
			}
		}
		return errors.New("participant keyframe source outside admitted roster")
	default:
		return errors.New("unexpected participant negotiation control")
	}
}
