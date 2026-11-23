// Package rtcwire defines the local, length-prefixed protocol between the
// WhatsApp connector and a separate MatrixRTC media worker. It never exposes
// its socket on TCP; the socket directory must be private to the containers.
package rtcwire

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"sync"
	"time"
)

const (
	Version        = 1
	MaxPayloadSize = 2 << 20 // 2 MiB, including base64 media
	AudioSamples   = 960
	AudioBytes     = 4 * AudioSamples // float32 little-endian, mono, 16kHz
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
	Version          int      `json:"version"`
	Type             string   `json:"type"`
	LoginID          string   `json:"login_id,omitempty"`
	CallID           string   `json:"call_id,omitempty"`
	RoomID           string   `json:"room_id,omitempty"`
	SlotID           string   `json:"slot_id,omitempty"`
	MembershipFormat string   `json:"membership_format,omitempty"`
	GhostMXID        string   `json:"ghost_mxid,omitempty"`
	ExpectedDevice   string   `json:"expected_device,omitempty"`
	MemberEvent      string   `json:"member_event,omitempty"`
	ExpectedUserMXID string   `json:"expected_user_mxid,omitempty"`
	Peer             string   `json:"peer,omitempty"`
	Group            bool     `json:"group,omitempty"`
	GroupJID         string   `json:"group_jid,omitempty"`
	Video            bool     `json:"video,omitempty"`
	Targets          []string `json:"targets,omitempty"`
	Timestamp        uint64   `json:"timestamp,omitempty"`
	Generation       uint64   `json:"generation,omitempty"`
	State            int      `json:"state"`
	Muted            bool     `json:"muted,omitempty"`
	Orientation      int      `json:"orientation,omitempty"`
	DurationUS       int64    `json:"duration_us,omitempty"`
	Reason           string   `json:"reason,omitempty"`
	Data             []byte   `json:"data,omitempty"`
}

func validBody(m Message) bool {
	if len(m.CallID) > 256 || len(m.LoginID) > 256 || len(m.ExpectedDevice) > 255 || len(m.MemberEvent) > 512 || len(m.Reason) > 4096 {
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
		_, err := DecodePCM(m.Data)
		return err == nil
	}
	return len(m.Data) == 0
}

func writeAll(w io.Writer, data []byte) error {
	for len(data) > 0 {
		n, err := w.Write(data)
		if err != nil {
			return err
		}
		if n < 1 {
			return io.ErrShortWrite
		}
		data = data[n:]
	}
	return nil
}

func Write(w io.Writer, msg Message) error {
	msg.Version = Version
	if msg.Type == "" {
		return fmt.Errorf("rtcwire: missing message type")
	}
	if msg.State < 0 || msg.State > 255 {
		return fmt.Errorf("rtcwire: invalid media state %d", msg.State)
	}
	if msg.Orientation < 0 || msg.Orientation > 3 {
		return fmt.Errorf("rtcwire: invalid video orientation %d", msg.Orientation)
	}
	if msg.DurationUS < 0 || msg.DurationUS > int64(time.Second/time.Microsecond) {
		return fmt.Errorf("rtcwire: invalid video frame duration %d us", msg.DurationUS)
	}
	if !validBody(msg) {
		return fmt.Errorf("rtcwire: invalid %s payload", msg.Type)
	}
	body, err := json.Marshal(msg)
	if err != nil {
		return err
	}
	if len(body) > MaxPayloadSize {
		return ErrTooLarge
	}
	var header [4]byte
	binary.BigEndian.PutUint32(header[:], uint32(len(body)))
	if err = writeAll(w, header[:]); err != nil {
		return err
	}
	return writeAll(w, body)
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
	if msg.Type == "whatsapp_video_state" {
		var field struct {
			State *int `json:"state"`
		}
		if json.Unmarshal(body, &field) != nil || field.State == nil {
			return Message{}, fmt.Errorf("rtcwire: missing video state")
		}
	}
	if !validBody(msg) {
		return Message{}, fmt.Errorf("rtcwire: invalid %s payload", msg.Type)
	}
	return msg, nil
}

type Peer struct {
	r       io.Reader
	w       io.Writer
	writeMu sync.Mutex
}

func NewPeer(r io.Reader, w io.Writer) *Peer { return &Peer{r: r, w: w} }
func (p *Peer) Read() (Message, error)       { return Read(p.r) }
func (p *Peer) Send(msg Message) error {
	p.writeMu.Lock()
	defer p.writeMu.Unlock()
	// A stalled worker must never block the WhatsApp RTP loop forever.
	if c, ok := p.w.(interface{ SetWriteDeadline(time.Time) error }); ok {
		_ = c.SetWriteDeadline(time.Now().Add(2 * time.Second))
		defer c.SetWriteDeadline(time.Time{})
	}
	return Write(p.w, msg)
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
	for i := range samples {
		value := math.Float32frombits(binary.LittleEndian.Uint32(raw[i*4:]))
		if math.IsNaN(float64(value)) || math.IsInf(float64(value), 0) || value < -1 || value > 1 {
			return nil, ErrBadAudio
		}
		samples[i] = value
	}
	return samples, nil
}
