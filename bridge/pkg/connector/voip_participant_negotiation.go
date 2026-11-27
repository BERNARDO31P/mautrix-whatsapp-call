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

// One offer per admitted group epoch. Only the established group's gateway
// reader may call these functions; neither function reads from the peer.
// Until Run accepts the acknowledgement the aggregate path must remain the only media path.
// The controller must quiesce its aggregate publications before Run is called.
type participantNegotiation struct {
	bus      *voipbridge.MediaBus
	peer     *rtcwire.Peer
	proposal rtcwire.Message
	revision uint64
	mu       sync.Mutex
	used     bool
}

// Publish only the previously captured proposal after the aggregate barrier.
// A fresh roster snapshot here would silently substitute another epoch.
func beginParticipantNegotiation(ctx context.Context, peer *rtcwire.Peer, bus *voipbridge.MediaBus, callID string, request rtcwire.Message, proposal rtcwire.Message, revision uint64) (*participantNegotiation, error) {
	if peer == nil || bus == nil || !rtcwire.IsParticipantRequest(request, callID) || proposal.CallID != callID {
		return nil, errors.New("independent group media was not requested")
	}
	if !bus.ParticipantRosterCurrent(proposal.Generation, revision) {
		return nil, voipbridge.ErrParticipantEpochRetired
	}
	if err := peer.SendContext(ctx, proposal); err != nil {
		return nil, err
	}
	if !bus.ParticipantRosterCurrent(proposal.Generation, revision) {
		return nil, voipbridge.ErrParticipantEpochRetired
	}
	return &participantNegotiation{bus: bus, peer: peer, proposal: proposal, revision: revision}, nil
}

// Run accepts one exact acknowledgement and starts the scoped sender. The
// caller must close the independent connection on return, including errors.
func (n *participantNegotiation) Run(ctx context.Context, ready rtcwire.Message, call *meowcaller.Call) error {
	n.mu.Lock()
	if n.used {
		n.mu.Unlock()
		return errors.New("participant negotiation already consumed")
	}
	n.used = true
	n.mu.Unlock()
	if !rtcwire.MatchesParticipantReady(n.proposal, ready) {
		return errors.New("participant acknowledgement does not match proposal")
	}
	transport, err := voipbridge.NewScopedParticipantTransport(n.bus, n.peer, n.proposal.CallID, n.proposal.Generation, n.revision)
	if err != nil {
		if errors.Is(err, voipbridge.ErrParticipantEpochRetired) {
			if sendErr := n.peer.SendContext(ctx, rtcwire.ParticipantEpochFrame(n.proposal.CallID, n.proposal.Generation, false)); sendErr != nil {
				return sendErr
			}
		}
		return err
	}
	bindings := make(map[string]string, len(n.proposal.ParticipantBindings))
	for _, b := range n.proposal.ParticipantBindings {
		bindings[b.ParticipantID] = b.Device
	}
	if call == nil {
		return errors.New("missing group state source")
	}
	parent := ctx
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	stateDone := make(chan error, 1)
	go func() {
		err := n.forwardStates(ctx, call, transport)
		stateDone <- err
		cancel()
	}()
	mediaErr := transport.RunDiscovered(ctx, func(key voipbridge.ParticipantMediaKey, generation uint64) bool {
		return generation == n.proposal.Generation && bindings[key.ParticipantID] == key.Device && n.bus.ParticipantRosterCurrent(generation, n.revision)
	})
	cancel()
	stateErr := <-stateDone
	for _, failure := range []error{mediaErr, stateErr} {
		if failure != nil && !errors.Is(failure, context.Canceled) && !errors.Is(failure, voipbridge.ErrParticipantEpochRetired) {
			return failure
		}
	}
	if parent.Err() != nil {
		return parent.Err()
	}
	if errors.Is(stateErr, voipbridge.ErrParticipantEpochRetired) || errors.Is(mediaErr, voipbridge.ErrParticipantEpochRetired) {
		if err := transport.RetireScope(parent); err != nil {
			return err
		}
		return voipbridge.ErrParticipantEpochRetired
	}
	return mediaErr
}

// Observations are provider-owned and tied to exact connected devices and
// PIDs. Unknown fields are not synthesized from silence or decoder output.
func (n *participantNegotiation) forwardStates(ctx context.Context, call *meowcaller.Call, transport *voipbridge.ParticipantTransport) error {
	type statePair struct {
		microphone, camera string
		screenShare        bool
		roleRevision       uint64
	}
	last := map[string]statePair{}
	ticker := time.NewTicker(150 * time.Millisecond)
	defer ticker.Stop()
	for {
		generation, revision, devices, _, ok := n.bus.ParticipantRosterSnapshot()
		if !ok || generation != n.proposal.Generation || revision != n.revision {
			return voipbridge.ErrParticipantEpochRetired
		}
		state, roles, group := call.GroupVideoRoles()
		if group && state.TransactionID == uint32(generation>>32) {
			// All accepted provider devices have exactly one committed PID.
			byDevice := make(map[string]uint32, len(devices))
			for _, device := range devices {
				byDevice[device.Device] = device.PID
			}
			observed := make(map[string]statePair, len(devices))
			for _, participant := range state.Participants {
				if participant.State != "connected" {
					continue
				}
				for _, device := range participant.Devices {
					id := device.JID.String()
					pid, exists := byDevice[id]
					if !exists || !device.HasPID || pid != device.PID {
						continue
					}
					if _, duplicate := observed[id]; duplicate {
						return errors.New("duplicate group media observation")
					}
					observed[id] = statePair{device.Media.Microphone, device.Media.Camera, roles[id].ScreenShare, roles[id].Revision}
				}
			}
			for _, binding := range n.proposal.ParticipantBindings {
				current, exists := observed[binding.Device]
				if !exists {
					continue
				}
				prior, seen := last[binding.Device]
				// Send initial role and every observed transition even when
				// video is silent. The receiver gates frames by this state.
				if !seen || current.screenShare != prior.screenShare || current.roleRevision != prior.roleRevision {
					value := 6 // camera role / screen share stopped
					if current.screenShare {
						value = 5
					}
					if err := transport.SendState(ctx, binding, value, current.roleRevision); err != nil {
						return err
					}
				}
				if current.microphone != prior.microphone {
					var value int
					switch current.microphone {
					case "muted":
						value = 1
					case "unmuted":
						value = 2
					}
					if value != 0 {
						if err := transport.SendState(ctx, binding, value, 0); err != nil {
							return err
						}
					}
				}
				if current.camera != prior.camera {
					var value int
					switch current.camera {
					case "off":
						value = 3
					case "on":
						value = 4
					}
					if value != 0 {
						if err := transport.SendState(ctx, binding, value, 0); err != nil {
							return err
						}
					}
				}
				last[binding.Device] = current
			}
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}
