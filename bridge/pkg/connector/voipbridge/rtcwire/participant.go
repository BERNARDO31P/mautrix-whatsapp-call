package rtcwire

import (
	"bytes"
	"encoding/binary"
	"errors"
	"math"
)

// ParticipantSource describes one provider-authenticated group device/source.
// It does not itself authorize that device to publish in a Matrix room.
// Generation is carried in Message.Generation and must be scoped to an admitted
// roster/key epoch by the caller. Timestamp is the source's uint32 RTP clock.
// These frame kinds must not be sent to a worker without an explicit admission
// and capability negotiation; the current aggregate session does not consume them.
type ParticipantSource struct {
	ParticipantID string `json:"participant_id"`
	Device        string `json:"device"`
	SSRC          uint32 `json:"ssrc"`
	RTPClockRate  uint32 `json:"rtp_clock_rate"`
	SampleRate    uint32 `json:"sample_rate,omitempty"`
	Discontinuity bool   `json:"discontinuity,omitempty"`
	ScreenShare   bool   `json:"screen_share,omitempty"`
	RoleRevision  uint64 `json:"role_revision,omitempty"`
}

var ErrBadParticipantMedia = errors.New("rtcwire: invalid participant media frame")

const MaxParticipantSamples = 1920 // 120ms at 16kHz

func validParticipantMedia(m Message) bool {
	s := m.ParticipantSource
	if s == nil || !m.Group || m.CallID == "" || m.Generation == 0 || m.Timestamp > math.MaxUint32 || s.ParticipantID == "" || len(s.ParticipantID) > 512 || s.Device == "" || len(s.Device) > 512 || s.SSRC == 0 {
		return false
	}
	switch m.Type {
	case "whatsapp_participant_audio":
		return !s.ScreenShare && s.RoleRevision == 0 && s.SampleRate == 16000 && (s.RTPClockRate == 16000 || s.RTPClockRate == 48000) && m.Orientation == 0 && validParticipantPCM(m.Data)
	case "whatsapp_participant_video":
		return s.SampleRate == 0 && s.RTPClockRate == 90000 && len(m.Data) > 3 && len(m.Data) <= 1024*1024 && (bytes.HasPrefix(m.Data, []byte{0, 0, 1}) || bytes.HasPrefix(m.Data, []byte{0, 0, 0, 1}))
	default:
		return false
	}
}

// Participant PCM is variable length; the existing aggregate audio contract
// continues to require exactly 960 samples. No padding or timestamp rewriting.
func validParticipantPCM(raw []byte) bool {
	if len(raw) == 0 || len(raw)%4 != 0 || len(raw) > MaxParticipantSamples*4 {
		return false
	}
	for i := 0; i < len(raw); i += 4 {
		v := math.Float32frombits(binary.LittleEndian.Uint32(raw[i:]))
		if math.IsNaN(float64(v)) || v < -1 || v > 1 {
			return false
		}
	}
	return true
}

func EncodeParticipantPCM(samples []float32) ([]byte, error) {
	if len(samples) == 0 || len(samples) > MaxParticipantSamples {
		return nil, ErrBadAudio
	}
	raw := make([]byte, len(samples)*4)
	for i, v := range samples {
		if math.IsNaN(float64(v)) || v < -1 || v > 1 {
			return nil, ErrBadAudio
		}
		binary.LittleEndian.PutUint32(raw[i*4:], math.Float32bits(v))
	}
	return raw, nil
}

// ValidateParticipantFrame applies normal wire admission without writing bytes.
func ValidateParticipantFrame(m Message) error {
	if !validParticipantMedia(m) || m.Orientation < 0 || m.Orientation > 3 || len(m.CallID) > 256 {
		return ErrBadParticipantMedia
	}
	return nil
}

// ParticipantEpochFrame is a control barrier on a negotiated independent
// group-media connection. State 1 opens, state 0 irreversibly retires.
func ParticipantEpochFrame(callID string, generation uint64, active bool) Message {
	m := Message{Type: "whatsapp_participant_epoch", CallID: callID, Group: true, Generation: generation}
	if active {
		m.State = 1
	}
	return m
}

func validParticipantEpoch(m Message) bool {
	roster, key := uint32(m.Generation>>32), uint32(m.Generation)
	return m.Group && m.CallID != "" && roster != 0 && key != 0 && key <= roster && (m.State == 0 || m.State == 1) && m.ParticipantSource == nil && len(m.Data) == 0 && m.Timestamp == 0 && m.Orientation == 0 && m.DurationUS == 0
}

// ParticipantBinding is a gateway-derived proposal, not a Matrix admission.
// The worker must load an existing prepared identity and validate its room.
type ParticipantBinding struct {
	ParticipantID string `json:"participant_id"`
	Device        string `json:"device"`
	GhostMXID     string `json:"ghost_mxid"`
}

func validParticipantRoster(m Message) bool {
	if (m.Type != "whatsapp_participant_roster" && m.Type != "matrix_participant_ready") || !m.Group || m.CallID == "" || m.State != 0 || m.ParticipantSource != nil || len(m.Data) != 0 || m.Timestamp != 0 || m.DurationUS != 0 || m.Orientation != 0 || m.Muted || m.Video || m.Reason != "" || m.RoomID != "" || m.SlotID != "" || m.LoginID != "" || m.GroupJID != "" || m.GhostMXID != "" || m.ExpectedUserMXID != "" || m.ExpectedDevice != "" || m.Peer != "" || m.MembershipFormat != "" || m.MemberEvent != "" {
		return false
	}
	roster, key := uint32(m.Generation>>32), uint32(m.Generation)
	if !validParticipantControlFields(m) || roster == 0 || key == 0 || key > roster || len(m.ParticipantBindings) == 0 || len(m.ParticipantBindings) > 32 {
		return false
	}
	participants, devices := map[string]bool{}, map[string]bool{}
	for _, binding := range m.ParticipantBindings {
		if binding.ParticipantID == "" || len(binding.ParticipantID) > 512 || binding.Device == "" || len(binding.Device) > 512 || len(binding.GhostMXID) < 4 || len(binding.GhostMXID) > 512 || binding.GhostMXID[0] != '@' || participants[binding.ParticipantID] || devices[binding.Device] {
			return false
		}
		participants[binding.ParticipantID], devices[binding.Device] = true, true
	}
	return true
}

// The worker must explicitly request the roster before the gateway proposes it.
// A ready message echoes the exact ordered bindings only after admission.
func validParticipantRequest(m Message) bool {
	return m.Type == "matrix_participant_request" && m.Group && m.CallID != "" && len(m.CallID) <= 256 && m.Generation == 0 && m.State == 0 && m.Timestamp == 0 && m.DurationUS == 0 && m.Orientation == 0 && !m.Muted && !m.Video && m.Reason == "" && m.RoomID == "" && m.SlotID == "" && m.LoginID == "" && m.GroupJID == "" && m.GhostMXID == "" && m.ExpectedUserMXID == "" && m.ExpectedDevice == "" && m.Peer == "" && m.MembershipFormat == "" && m.MemberEvent == "" && m.ParticipantSource == nil && len(m.ParticipantBindings) == 0 && len(m.Data) == 0 && validParticipantControlFields(m)
}
func validParticipantControlFields(m Message) bool {
	return m.BridgeBotMXID == "" && m.ExpectedSigningKey == "" && m.ExpectedIdentityKey == "" && m.MemberJoinID == "" && m.MemberJoinTS == 0 && m.Intent == "" && len(m.Targets) == 0
}
func IsParticipantRequest(m Message, callID string) bool {
	return callID != "" && m.CallID == callID && validParticipantRequest(m)
}
func MatchesParticipantReady(proposal, ready Message) bool {
	if proposal.Type != "whatsapp_participant_roster" || ready.Type != "matrix_participant_ready" || !validParticipantRoster(proposal) || !validParticipantRoster(ready) || proposal.CallID != ready.CallID || proposal.Generation != ready.Generation || len(proposal.ParticipantBindings) != len(ready.ParticipantBindings) {
		return false
	}
	for i, binding := range proposal.ParticipantBindings {
		if ready.ParticipantBindings[i] != binding {
			return false
		}
	}
	return true
}

// ValidParticipantKeyframe checks the exact source control without allowing
// media, state changes, or identity/roster fields to ride on the request.
func ValidParticipantKeyframe(m Message) bool {
	s := m.ParticipantSource
	roster, key := uint32(m.Generation>>32), uint32(m.Generation)
	if m.Type != "matrix_participant_keyframe" || s == nil || roster == 0 || key == 0 || key > roster || s.ParticipantID == "" || len(s.ParticipantID) > 512 || s.Device == "" || len(s.Device) > 512 || s.SSRC == 0 || s.RTPClockRate != 90000 || s.SampleRate != 0 || s.Discontinuity || s.ScreenShare || s.RoleRevision != 0 {
		return false
	}
	// Reuse the strict empty-control envelope check after removing only the
	// fields that belong to this request.
	m.Type = "matrix_participant_request"
	m.Generation = 0
	m.ParticipantSource = nil
	return validParticipantRequest(m)
}

// State 1/2 = microphone muted/unmuted; 3/4 = camera off/on.
// Only an exact already admitted roster binding is valid. Unknown is not a
// state transition. There is no media payload or guessed SSRC.
func ValidParticipantState(m Message) bool {
	if m.Type != "whatsapp_participant_state" || len(m.ParticipantBindings) != 1 || m.State < 1 || m.State > 6 {
		return false
	}
	if (m.State >= 5) != (m.Timestamp != 0) {
		return false
	}
	m.Type = "whatsapp_participant_roster"
	m.State = 0
	m.Timestamp = 0
	return validParticipantRoster(m)
}
