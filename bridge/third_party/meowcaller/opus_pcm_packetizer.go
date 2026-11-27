package meowcaller

// opusPCMPacketizer adapts the public 60 ms PCM source to the negotiated Opus
// packet interval. It retains less than one source frame between sends.
type opusPCMPacketizer struct{ pending []float32 }

func (p *opusPCMPacketizer) reset() { clear(p.pending); p.pending = nil }

func (p *opusPCMPacketizer) next(samples int, source func() []float32) []float32 {
	for len(p.pending) < samples {
		pcm := make([]float32, FrameSamples)
		copy(pcm, source())
		p.pending = append(p.pending, pcm...)
	}
	frame := append([]float32(nil), p.pending[:samples]...)
	p.pending = p.pending[samples:]
	if len(p.pending) == 0 {
		p.pending = nil
	}
	return frame
}

// Some real-time sources can invalidate PCM already read into a packetizer
// (e.g. microphone mute). Players without this extension retain their existing
// source/state generation fencing.
type opusPlayerGeneration struct {
	player               *Player
	source, state, frame uint64
}

func opusGeneration(player *Player) opusPlayerGeneration {
	g := opusPlayerGeneration{player: player}
	if player == nil {
		return g
	}
	player.mu.Lock()
	g.source, g.state = player.sourceGeneration, player.stateGeneration
	src := player.src
	player.mu.Unlock()
	if source, ok := src.(interface{ AudioFrameGeneration() uint64 }); ok {
		g.frame = source.AudioFrameGeneration()
	}
	return g
}
