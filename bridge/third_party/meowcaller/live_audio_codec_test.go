package meowcaller

import (
	"context"
	"strings"
	"testing"

	"github.com/purpshell/meowcaller/liveopus"
)

func TestLiveAudioCodecAdmission(t *testing.T) {
	ctx, cancel := context.WithCancelCause(context.Background())
	defer cancel(nil)
	if err := checkLiveAudioCodec(AudioCodecMlow, cancel); err != nil || ctx.Err() != nil {
		t.Fatalf("MLow rejected: %v", err)
	}
	err := checkLiveAudioCodec(AudioCodecOpus, cancel)
	if liveopus.Available {
		if err != nil || ctx.Err() != nil {
			t.Fatalf("available Opus rejected: %v", err)
		}
		config, err := configuredLiveAudio(AudioCodecOpus, 20, 24000)
		if err != nil || config.frameSamples != 320 || config.ticksPerPacket() != 960 || config.clockRate() != 48000 {
			t.Fatalf("wrong Opus config: %+v / %v", config, err)
		}
		checkLiveAudioChange(config, AudioCodecOpus, 20, 24000, cancel)
		if ctx.Err() != nil {
			t.Fatal("unchanged configuration cancelled media")
		}
		checkLiveAudioChange(config, AudioCodecMlow, 60, 0, cancel)
		if ctx.Err() == nil {
			t.Fatal("late codec change did not cancel media")
		}
		if _, err := configuredLiveAudio(AudioCodecOpus, 30, 0); err == nil {
			t.Fatal("invalid duration accepted")
		}
	} else if err == nil || !strings.Contains(err.Error(), "opus") || context.Cause(ctx) != err {
		t.Fatalf("unavailable Opus not rejected: %v / %v", err, context.Cause(ctx))
	}
	if err := checkLiveAudioCodec(AudioCodec(99), nil); err == nil || !strings.Contains(err.Error(), "unknown") {
		t.Fatalf("unknown accepted: %v", err)
	}
}
