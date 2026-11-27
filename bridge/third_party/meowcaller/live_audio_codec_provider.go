package meowcaller

import (
	"github.com/purpshell/meowcaller/liveopus"
	"github.com/purpshell/meowcaller/mlow"
	"github.com/rs/zerolog"
)

type liveAudioEncoder interface {
	Encode([]float32) ([]byte, error)
}

func newLiveAudioEncoder(config liveAudioConfig, log zerolog.Logger) (liveAudioEncoder, func(), error) {
	if config.codec == AudioCodecMlow {
		return mlow.NewMlowEncoder(mlow.WithLogger(log)), func() {}, nil
	}
	enc, err := liveopus.NewEncoder(config.bitrate)
	if err != nil {
		return nil, nil, err
	}
	return enc, enc.Close, nil
}

type liveOpusParticipantDecoder struct {
	log      zerolog.Logger
	decoder  *liveopus.Decoder
	failures uint64
	closed   bool
}

func (d *liveOpusParticipantDecoder) Decode(payload []byte) []float32 {
	if d.closed {
		return nil
	}
	var err error
	if d.decoder == nil {
		d.decoder, err = liveopus.NewDecoder()
	}
	var pcm []float32
	if err == nil {
		pcm, err = d.decoder.Decode(payload)
	}
	if err != nil {
		d.failures++
		if d.failures == 1 || d.failures%100 == 0 {
			d.log.Warn().Err(err).Uint64("failures", d.failures).Msg("dropping invalid negotiated Opus packet")
		}
		return nil
	}
	// libopus's float decoder may legitimately overshoot full scale.
	for i, v := range pcm {
		if v > 1 {
			pcm[i] = 1
		} else if v < -1 {
			pcm[i] = -1
		}
	}
	return pcm
}

func (d *liveOpusParticipantDecoder) Close() {
	d.closed = true
	if d.decoder != nil {
		d.decoder.Close()
		d.decoder = nil
	}
}

func newLiveParticipantDecoder(config liveAudioConfig, log zerolog.Logger) participantAudioDecoder {
	if config.codec == AudioCodecOpus {
		return &liveOpusParticipantDecoder{log: log}
	}
	return mlow.NewMlowDecoder(mlow.WithLogger(log))
}
