package meowcaller

import (
	"io"
	"sync"
)

// PlayerState is a Player's lifecycle state.
type PlayerState int

const (
	// PlayerIdle means no source is playing (none set, stopped, or finished).
	PlayerIdle PlayerState = iota
	// PlayerPlaying means the active source is streaming into the call.
	PlayerPlaying
	// PlayerPaused means playback is suspended (silence is sent in the meantime).
	PlayerPaused
)

// Player streams an AudioSource into a Call — the discord.js AudioPlayer analogue.
// Attach it with Call.Subscribe (or Call.Play as a shortcut); while Playing, the call
// pulls frames from the source on the codec's 60 ms cadence. When the source reaches
// io.EOF the player returns to PlayerIdle and fires OnFinish (queue the next source
// there for gapless playback).
type Player struct {
	mu               sync.Mutex
	src              AudioSource
	state            PlayerState
	onFinish         func()
	sourceGeneration uint64
	stateGeneration  uint64
}

// NewPlayer returns an idle Player.
func NewPlayer() *Player {
	return &Player{state: PlayerIdle}
}

// Play sets src as the active source and starts playback, replacing (and closing) any
// current source.
func (p *Player) Play(src AudioSource) {
	p.mu.Lock()
	old := p.src
	p.src = src
	p.sourceGeneration++
	p.stateGeneration++
	p.state = PlayerPlaying
	p.mu.Unlock()
	if old != nil {
		_ = old.Close()
	}
}

// Pause suspends playback; the call sends silence until Resume.
func (p *Player) Pause() {
	p.mu.Lock()
	if p.state == PlayerPlaying {
		p.state = PlayerPaused
		p.stateGeneration++
	}
	p.mu.Unlock()
}

// Resume continues a paused player.
func (p *Player) Resume() {
	p.mu.Lock()
	if p.state == PlayerPaused {
		p.state = PlayerPlaying
		p.stateGeneration++
	}
	p.mu.Unlock()
}

// Stop halts playback and closes the active source.
func (p *Player) Stop() {
	p.mu.Lock()
	old := p.src
	p.src = nil
	p.sourceGeneration++
	p.stateGeneration++
	p.state = PlayerIdle
	p.mu.Unlock()
	if old != nil {
		_ = old.Close()
	}
}

// State returns the current PlayerState.
func (p *Player) State() PlayerState {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.state
}

// OnFinish registers a callback fired when the active source is exhausted (the player
// transitions to PlayerIdle). Replaces any previous callback.
func (p *Player) OnFinish(fn func()) {
	p.mu.Lock()
	p.onFinish = fn
	p.mu.Unlock()
}

// nextFrame is pulled by the engine's send loop each frame interval. It returns the
// next source frame while Playing, or nil to send silence (idle/paused/between
// sources). On source EOF it goes Idle and fires OnFinish (outside the lock).
func (p *Player) nextFrame() []float32 {
	p.mu.Lock()
	if p.state != PlayerPlaying || p.src == nil {
		p.mu.Unlock()
		return nil
	}
	src := p.src
	sourceGeneration, stateGeneration := p.sourceGeneration, p.stateGeneration
	p.mu.Unlock()

	frame, err := src.ReadFrame()
	p.mu.Lock()
	if p.sourceGeneration != sourceGeneration {
		// Play or Stop already released ownership of this source. Neither its
		// late frame nor its EOF belongs to the new playback generation.
		p.mu.Unlock()
		return nil
	}
	admitFrame := p.state == PlayerPlaying && p.stateGeneration == stateGeneration
	if err == nil {
		p.mu.Unlock()
		if admitFrame {
			return frame
		}
		return nil
	}
	// Only the owner closes an exhausted source and invokes OnFinish.
	p.src = nil
	p.sourceGeneration++
	p.stateGeneration++
	p.state = PlayerIdle
	finish := p.onFinish
	p.mu.Unlock()
	_ = src.Close()
	if finish != nil {
		finish()
	}
	if err != io.EOF || !admitFrame {
		return nil
	}
	return frame // may be a final padded frame or nil
}
