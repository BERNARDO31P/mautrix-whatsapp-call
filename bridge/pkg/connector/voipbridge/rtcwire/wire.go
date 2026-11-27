// Package rtcwire defines the local, length-prefixed protocol between the
// WhatsApp connector and a separate MatrixRTC media worker. It never exposes
// its socket on TCP; the socket directory must be private to the containers.
package rtcwire

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"time"
)

const (
	Version        = 1
	MaxPayloadSize = 2 << 20 // 2 MiB, including base64 media
	// Reserve room for the JSON envelope (including a maximally escaped
	// 256-byte call ID) before allocating or queuing outbound video.
	MaxQueuedVideoBytes = (MaxPayloadSize - 4096) / 4 * 3
	AudioSamples        = 960
	AudioBytes          = 4 * AudioSamples // float32 little-endian, mono, 16kHz
	wireSendBudget      = 2 * time.Second
)

var (
	ErrTooLarge = errors.New("rtcwire: message exceeds 2 MiB")
	ErrBadAudio = errors.New("rtcwire: invalid PCM audio frame")
)

// JSON fields are intentionally stable and versioned. Media bytes use JSON
// base64 (encoding/json's []byte format), so the protocol is simple to mirror
// from Rust. It is confined to the host's local Unix socket.
//
// Types:
//
//	incoming: Go -> worker (request to ring in Matrix)
//	outgoing: Go -> worker (Matrix user is ringing the WhatsApp ghost)
//	dial: worker -> Go (ghost session/room validated; start WhatsApp call)
//	whatsapp_answered: Go -> worker (remote WhatsApp peer accepted)
//	answer/reject/hangup: worker -> Go (MatrixRTC ready / user action)
//	whatsapp_audio / whatsapp_video: Go -> worker
//	matrix_audio / matrix_video: worker -> Go
//	matrix_video_start/stop/mute/unmute: worker -> Go
//	matrix_audio_mute/unmute: worker -> Go
//	whatsapp_video_state / whatsapp_audio_mute/unmute: Go -> worker
//	whatsapp_video_keyframe: Go -> worker (WhatsApp requested a fresh IDR)
//	ended: Go -> worker
//	error: either direction
//
// An "answer" MUST mean that the Matrix participant has joined the call and
// the end user has explicitly accepted. Mere SFU connection is not an answer.
type Message struct {
	Version             int                  `json:"version"`
	Type                string               `json:"type"`
	LoginID             string               `json:"login_id,omitempty"`
	CallID              string               `json:"call_id,omitempty"`
	RoomID              string               `json:"room_id,omitempty"`
	SlotID              string               `json:"slot_id,omitempty"`
	MembershipFormat    string               `json:"membership_format,omitempty"`
	GhostMXID           string               `json:"ghost_mxid,omitempty"`
	BridgeBotMXID       string               `json:"bridge_bot_mxid,omitempty"`
	ExpectedDevice      string               `json:"expected_device,omitempty"`
	ExpectedSigningKey  string               `json:"expected_signing_key,omitempty"`
	ExpectedIdentityKey string               `json:"expected_identity_key,omitempty"`
	MemberEvent         string               `json:"member_event,omitempty"`
	MemberJoinID        string               `json:"member_join_id,omitempty"`
	MemberJoinTS        uint64               `json:"member_join_ts,omitempty"`
	ExpectedUserMXID    string               `json:"expected_user_mxid,omitempty"`
	Peer                string               `json:"peer,omitempty"`
	Group               bool                 `json:"group,omitempty"`
	GroupJID            string               `json:"group_jid,omitempty"`
	Intent              string               `json:"intent,omitempty"`
	Video               bool                 `json:"video,omitempty"`
	Targets             []string             `json:"targets,omitempty"`
	Timestamp           uint64               `json:"timestamp,omitempty"`
	Generation          uint64               `json:"generation,omitempty"`
	State               int                  `json:"state"`
	Muted               bool                 `json:"muted,omitempty"`
	Orientation         int                  `json:"orientation,omitempty"`
	DurationUS          int64                `json:"duration_us,omitempty"`
	Reason              string               `json:"reason,omitempty"`
	Data                []byte               `json:"data,omitempty"`
	ParticipantSource   *ParticipantSource   `json:"participant_source,omitempty"`
	ParticipantBindings []ParticipantBinding `json:"participant_bindings,omitempty"`
}

// ValidDeviceKey accepts only canonical unpadded Matrix public-key encodings.
func ValidDeviceKey(key string) bool {
	if len(key) != 43 {
		return false
	}
	decoded, err := base64.RawStdEncoding.Strict().DecodeString(key)
	return err == nil && len(decoded) == 32 && base64.RawStdEncoding.EncodeToString(decoded) == key
}

func validBody(m Message) bool {
	if m.Type == "whatsapp_participant_state" {
		if !ValidParticipantState(m) {
			return false
		}
	} else if m.Type == "whatsapp_participant_roster" || m.Type == "matrix_participant_ready" {
		if !validParticipantRoster(m) {
			return false
		}
	} else if len(m.ParticipantBindings) != 0 {
		return false
	}
	participantFrame := m.Type == "whatsapp_participant_audio" || m.Type == "whatsapp_participant_video"
	participantKeyframe := m.Type == "matrix_participant_keyframe"
	if (participantFrame || participantKeyframe) != (m.ParticipantSource != nil) {
		return false
	}
	if participantFrame && !validParticipantMedia(m) {
		return false
	}
	if m.ExpectedSigningKey != "" || m.ExpectedIdentityKey != "" {
		if m.Type != "outgoing" || m.ExpectedDevice == "" || !ValidDeviceKey(m.ExpectedSigningKey) || !ValidDeviceKey(m.ExpectedIdentityKey) {
			return false
		}
	}
	if len(m.Type) > 40 || len(m.RoomID) > 512 || len(m.SlotID) > 256 || len(m.GhostMXID) > 512 || len(m.ExpectedUserMXID) > 512 || len(m.Peer) > 512 || len(m.GroupJID) > 512 || len(m.CallID) > 256 || len(m.LoginID) > 256 || len(m.ExpectedDevice) > 255 || len(m.MemberEvent) > 512 || len(m.Reason) > 4096 {
		return false
	}
	if m.BridgeBotMXID != "" && (len(m.BridgeBotMXID) > 512 || (m.Type != "incoming" && m.Type != "outgoing")) {
		return false
	}
	if m.Intent != "" && !((m.Type == "outgoing" && (m.Intent == "auto" || m.Intent == "audio" || m.Intent == "video") && !m.Video) || (m.Type == "dial" && (m.Intent == "audio" || m.Intent == "video") && m.Video == (m.Intent == "video"))) {
		return false
	}
	if m.MemberJoinID != "" {
		if len(m.MemberJoinID) > 512 || m.Type != "outgoing" || m.MemberEvent == "" || m.ExpectedDevice == "" ||
			!((m.MembershipFormat == "msc" && m.MemberJoinTS == 0) || (m.MembershipFormat == "room_state" && m.MemberJoinTS != 0)) {
			return false
		}
	} else if m.MemberJoinTS != 0 {
		return false
	}
	if m.MembershipFormat != "" && m.MembershipFormat != "msc" && m.MembershipFormat != "room_state" {
		return false
	}
	if m.Type == "whatsapp_audio_mute" && !m.Muted || m.Type == "whatsapp_audio_unmute" && m.Muted {
		return false
	}
	if m.Type == "group_check" || m.Type == "matrix_group_invite" {
		if len(m.Targets) == 0 || len(m.Targets) > 31 {
			return false
		}
		for _, target := range m.Targets {
			if target == "" || len(target) > 192 {
				return false
			}
		}
	} else if len(m.Targets) != 0 {
		return false
	}
	switch m.Type {
	case "matrix_video_screen_start":
		return m.Group && m.Generation != 0 && len(m.Data) == 0
	case "matrix_participant_request":
		return validParticipantRequest(m)
	case "whatsapp_participant_unavailable":
		m.Type = "matrix_participant_request"
		return validParticipantRequest(m)
	case "matrix_participant_keyframe":
		return ValidParticipantKeyframe(m)
	case "whatsapp_participant_epoch":
		return validParticipantEpoch(m)
	case "whatsapp_participant_audio", "whatsapp_participant_video":
		return true // validated by validParticipantMedia above
	case "whatsapp_video_state":
		switch m.State {
		case 0, 1, 3, 4, 5, 6, 8, 11:
			return len(m.Data) == 0
		default:
			return false
		}
	case "whatsapp_video", "matrix_video":
		return len(m.Data) > 3 && (bytes.HasPrefix(m.Data, []byte{0, 0, 1}) || bytes.HasPrefix(m.Data, []byte{0, 0, 0, 1})) && (m.Type != "whatsapp_video" || m.Generation > 0)
	case "whatsapp_audio", "matrix_audio":
		return readPCM(m.Data, nil) == nil
	}
	return len(m.Data) == 0
}

func writeAll(w io.Writer, data []byte) error {
	for len(data) > 0 {
		n, err := w.Write(data)
		if err != nil {
			return err
		}
		if n < 1 || n > len(data) {
			return io.ErrShortWrite
		}
		data = data[n:]
	}
	return nil
}

func encodeMessage(msg Message) ([]byte, error) {
	msg.Version = Version
	if msg.Type == "" {
		return nil, fmt.Errorf("rtcwire: missing message type")
	}
	if msg.State < 0 || msg.State > 255 {
		return nil, fmt.Errorf("rtcwire: invalid media state %d", msg.State)
	}
	if msg.Orientation < 0 || msg.Orientation > 3 {
		return nil, fmt.Errorf("rtcwire: invalid video orientation %d", msg.Orientation)
	}
	if msg.DurationUS < 0 || msg.DurationUS > int64(time.Second/time.Microsecond) {
		return nil, fmt.Errorf("rtcwire: invalid video frame duration %d us", msg.DurationUS)
	}
	if !validBody(msg) {
		return nil, fmt.Errorf("rtcwire: invalid %s payload", msg.Type)
	}
	// Reject impossible base64 sizes before allocating a JSON buffer.
	// The raw length check also prevents overflow in EncodedLen.
	if len(msg.Data) > MaxPayloadSize || base64.StdEncoding.EncodedLen(len(msg.Data)) > MaxPayloadSize {
		return nil, ErrTooLarge
	}
	body, err := json.Marshal(msg)
	if err != nil {
		return nil, err
	}
	if len(body) > MaxPayloadSize {
		return nil, ErrTooLarge
	}
	return body, nil
}

func writeMessage(w io.Writer, body []byte) error {
	var header [4]byte
	binary.BigEndian.PutUint32(header[:], uint32(len(body)))
	if err := writeAll(w, header[:]); err != nil {
		return err
	}
	return writeAll(w, body)
}

func Write(w io.Writer, msg Message) error {
	body, err := encodeMessage(msg)
	if err != nil {
		return err
	}
	return writeMessage(w, body)
}

func Read(r io.Reader) (Message, error) {
	var header [4]byte
	if _, err := io.ReadFull(r, header[:]); err != nil {
		return Message{}, err
	}
	n := binary.BigEndian.Uint32(header[:])
	if n == 0 || n > MaxPayloadSize {
		return Message{}, ErrTooLarge
	}
	body := make([]byte, n)
	if _, err := io.ReadFull(r, body); err != nil {
		return Message{}, err
	}
	var msg Message
	if err := json.Unmarshal(body, &msg); err != nil {
		return Message{}, err
	}
	if msg.Version != Version {
		return Message{}, fmt.Errorf("rtcwire: unsupported protocol version %d", msg.Version)
	}
	if msg.Type == "" {
		return Message{}, fmt.Errorf("rtcwire: missing message type")
	}
	if msg.State < 0 || msg.State > 255 {
		return Message{}, fmt.Errorf("rtcwire: invalid media state %d", msg.State)
	}
	if msg.Orientation < 0 || msg.Orientation > 3 {
		return Message{}, fmt.Errorf("rtcwire: invalid video orientation %d", msg.Orientation)
	}
	if msg.DurationUS < 0 || msg.DurationUS > int64(time.Second/time.Microsecond) {
		return Message{}, fmt.Errorf("rtcwire: invalid video frame duration %d us", msg.DurationUS)
	}
	if msg.Type == "whatsapp_video_state" || msg.Type == "whatsapp_participant_epoch" || msg.Type == "whatsapp_participant_roster" || msg.Type == "matrix_participant_ready" || msg.Type == "matrix_participant_request" || msg.Type == "matrix_participant_keyframe" || msg.Type == "whatsapp_participant_unavailable" || msg.Type == "whatsapp_participant_state" {
		var field struct {
			State *int `json:"state"`
		}
		if json.Unmarshal(body, &field) != nil || field.State == nil {
			return Message{}, fmt.Errorf("rtcwire: missing explicit state")
		}
	}
	if !validBody(msg) {
		return Message{}, fmt.Errorf("rtcwire: invalid %s payload", msg.Type)
	}
	return msg, nil
}

type Peer struct {
	r         io.Reader
	w         io.Writer
	writeGate chan struct{}
	writeErr  error
}

func NewPeer(r io.Reader, w io.Writer) *Peer {
	return &Peer{r: r, w: w, writeGate: make(chan struct{}, 1)}
}
func (p *Peer) Read() (Message, error) { return Read(p.r) }

// LocalFrameError identifies a rejected frame before any socket write.
type LocalFrameError struct{ cause error }

func (e *LocalFrameError) Error() string { return e.cause.Error() }
func (e *LocalFrameError) Unwrap() error { return e.cause }
func IsLocalFrameError(err error) bool {
	var local *LocalFrameError
	return errors.As(err, &local)
}
func (p *Peer) Send(msg Message) error {
	return p.SendContext(context.Background(), msg)
}

// SendContext permits cancellation while waiting for another complete wire frame.
// One absolute budget covers admission, encoding and socket transmission.
// Deadline-capable writers also interrupt an in-flight frame on cancellation.
// A failed frame poisons the stream because partial delivery is possible.
func (p *Peer) SendContext(ctx context.Context, msg Message) error {
	ctx, cancel := context.WithTimeout(ctx, wireSendBudget)
	defer cancel()
	if err := ctx.Err(); err != nil {
		return err
	}
	select {
	case p.writeGate <- struct{}{}:
	case <-ctx.Done():
		return ctx.Err()
	}
	defer func() { <-p.writeGate }()
	// Both select cases may have been ready. Never admit a canceled waiter.
	if err := ctx.Err(); err != nil {
		return err
	}
	if p.writeErr != nil {
		return p.writeErr
	}
	// A local validation failure cannot corrupt a stream: no bytes have
	// been transmitted yet. Keep the shared connection usable for other calls.
	body, err := encodeMessage(msg)
	if err != nil {
		return &LocalFrameError{cause: err}
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	// A stalled worker must never block the WhatsApp RTP loop forever.
	if c, ok := p.w.(interface{ SetWriteDeadline(time.Time) error }); ok {
		deadline, _ := ctx.Deadline()
		if err := c.SetWriteDeadline(deadline); err != nil {
			return err
		}
		// Wait for any started cancellation callback before resetting the
		// deadline or admitting the next writer. Otherwise a late callback
		// could interrupt a different frame.
		finished := make(chan struct{})
		stop := context.AfterFunc(ctx, func() {
			defer close(finished)
			_ = c.SetWriteDeadline(time.Now())
		})
		defer func() {
			if !stop() {
				<-finished
			}
			_ = c.SetWriteDeadline(time.Time{})
		}()
	}
	// Deadline setup and callback registration can race cancellation.
	// No bytes have been submitted yet, so keep the connection usable.
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := writeMessage(p.w, body); err != nil {
		// A failed write may have sent a partial frame. Never reuse this
		// stream, and wake the reader if it shares the connection.
		p.writeErr = err
		if closer, ok := p.w.(io.Closer); ok {
			_ = closer.Close()
		}
		return err
	}
	return nil
}

func EncodePCM(samples []float32) ([]byte, error) {
	if len(samples) != AudioSamples {
		return nil, ErrBadAudio
	}
	raw := make([]byte, AudioBytes)
	for i, value := range samples {
		if math.IsNaN(float64(value)) || math.IsInf(float64(value), 0) || value < -1 || value > 1 {
			return nil, ErrBadAudio
		}
		binary.LittleEndian.PutUint32(raw[i*4:], math.Float32bits(value))
	}
	return raw, nil
}

func DecodePCM(raw []byte) ([]float32, error) {
	if len(raw) != AudioBytes {
		return nil, ErrBadAudio
	}
	samples := make([]float32, AudioSamples)
	if err := readPCM(raw, samples); err != nil {
		return nil, err
	}
	return samples, nil
}

// readPCM validates the same samples for both wire admission and decoding.
// A nil destination validates without allocating a decoded frame.
func readPCM(raw []byte, dst []float32) error {
	if len(raw) != AudioBytes || (dst != nil && len(dst) != AudioSamples) {
		return ErrBadAudio
	}
	for i := 0; i < AudioSamples; i++ {
		value := math.Float32frombits(binary.LittleEndian.Uint32(raw[i*4:]))
		if math.IsNaN(float64(value)) || math.IsInf(float64(value), 0) || value < -1 || value > 1 {
			return ErrBadAudio
		}
		if dst != nil {
			dst[i] = value
		}
	}
	return nil
}
