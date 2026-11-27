package meowcaller

import (
	"context"
	"fmt"

	"github.com/purpshell/meowcaller/liveopus"
)

// AudioCodec identifies the wire audio codec negotiated for a call. WhatsApp 1:1
// audio is carried in RTP payload type 120 regardless of codec; the codec itself
// is chosen by signaling (the server's voip_settings), not the RTP payload type.
type AudioCodec int8

const (
	// AudioCodecMlow is Meta's 16 kHz MLow codec (the default).
	AudioCodecMlow AudioCodec = iota
	// AudioCodecOpus is RFC 6716 Opus.
	AudioCodecOpus
)

// String renders the codec name for logs.
func (c AudioCodec) String() string {
	switch c {
	case AudioCodecOpus:
		return "opus"
	case AudioCodecMlow:
		return "mlow"
	default:
		return "unknown"
	}
}

// The provider selects the codec, not the RTP payload type. Use the RFC 7587
// clock for standard Opus; the existing proprietary MLow path stays at 16 kHz.
// The RFC clock assumption for WhatsApp's WARP transport requires live verification.
type liveAudioConfig struct {
	codec        AudioCodec
	frameSamples int
	bitrate      int
}

func configuredLiveAudio(codec AudioCodec, frameMs, bitrate int) (liveAudioConfig, error) {
	cfg := liveAudioConfig{codec: codec, frameSamples: FrameSamples}
	if codec == AudioCodecMlow {
		return cfg, nil
	}
	if codec != AudioCodecOpus || !liveopus.Available {
		return cfg, fmt.Errorf("meowcaller: unsupported negotiated live audio codec: %s", codec)
	}
	if frameMs != 0 {
		if frameMs < 5 || frameMs > 60 || !liveopus.ValidFrameSamples(frameMs*SampleRate/1000) {
			return cfg, fmt.Errorf("meowcaller: unsupported negotiated Opus frame duration: %d ms", frameMs)
		}
		cfg.frameSamples = frameMs * SampleRate / 1000
	}
	if err := liveopus.CheckBitrate(bitrate); err != nil {
		return cfg, err
	}
	cfg.bitrate = bitrate
	return cfg, nil
}

func (c liveAudioConfig) clockRate() uint32 {
	if c.codec == AudioCodecOpus {
		return 48000
	}
	return SampleRate
}

func (c liveAudioConfig) ticksPerPacket() uint32 {
	return uint32(c.frameSamples) * (c.clockRate() / SampleRate)
}

func checkLiveAudioCodec(codec AudioCodec, cancel context.CancelCauseFunc) error {
	_, err := configuredLiveAudio(codec, 0, 0)
	if err != nil && cancel != nil {
		cancel(err)
	}
	return err
}

// A late change cannot reinterpret packets under an already initialized codec
// or silently change the RTP clock. Finish the exact owning call instead.
func checkLiveAudioChange(active liveAudioConfig, codec AudioCodec, frameMs, bitrate int, cancel context.CancelCauseFunc) {
	next, err := configuredLiveAudio(codec, frameMs, bitrate)
	if err == nil && active != next {
		err = fmt.Errorf("meowcaller: negotiated audio settings changed during media")
	}
	if err != nil {
		cancel(err)
	}
}
