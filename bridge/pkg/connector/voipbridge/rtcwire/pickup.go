package rtcwire

import (
	"context"
	"fmt"
	"io"
)

// StreamFrames preserves the last successfully read frame before EOF. Closing
// the result channel signals failure without cancelling the consumer ahead of
// any queued terminal decision. The caller must interrupt a blocked read when
// ctx ends (the gateway closes its socket).
func StreamFrames(ctx context.Context, read func() (Message, error)) <-chan Message {
	frames := make(chan Message, 1)
	go func() {
		defer close(frames)
		for ctx.Err() == nil {
			msg, err := read()
			if err != nil {
				return
			}
			select {
			case frames <- msg:
			case <-ctx.Done():
				return
			}
		}
	}()
	return frames
}

// PickupBuffer holds only ordered, bounded controls received after Matrix
// answers but before the provider accepts. Real-time media has no value until
// acceptance, and retaining it would back up the private socket writer.
type PickupBuffer struct {
	Controls     []Message
	DroppedVideo bool
}

const maxPickupControls = 32

// AwaitPickup drains worker frames while the provider pickup is pending. The
// caller owns the outer call lifetime and must terminate the exact call on
// error, including an ambiguous provider submission. The buffered completion
// channel lets a canceled provider return without retaining the reader.
func AwaitPickup(ctx context.Context, callID string, frames <-chan Message, answer func(context.Context) error) (PickupBuffer, error) {
	var pending PickupBuffer
	if err := ctx.Err(); err != nil {
		return pending, err
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- answer(ctx) }()
	for {
		if err := ctx.Err(); err != nil {
			return PickupBuffer{}, err
		}
		select {
		case <-ctx.Done():
			return PickupBuffer{}, ctx.Err()
		case err := <-done:
			if err != nil {
				return PickupBuffer{}, err
			}
			if err := ctx.Err(); err != nil {
				return PickupBuffer{}, err
			}
			return pending, nil
		case msg, ok := <-frames:
			if !ok {
				return PickupBuffer{}, io.EOF
			}
			if msg.CallID != callID {
				return PickupBuffer{}, fmt.Errorf("call ID mismatch during RTC pickup")
			}
			switch msg.Type {
			case "matrix_audio":
			case "matrix_video":
				pending.DroppedVideo = true
			case "matrix_video_resync", "matrix_video_start", "matrix_video_screen_start", "matrix_video_stop", "matrix_video_mute", "matrix_video_unmute", "matrix_audio_mute", "matrix_audio_unmute":
				if len(pending.Controls) >= maxPickupControls {
					return PickupBuffer{}, fmt.Errorf("RTC pickup control queue full")
				}
				pending.Controls = append(pending.Controls, msg)
			case "hangup", "reject":
				return PickupBuffer{}, fmt.Errorf("Matrix participant ended the call during WhatsApp pickup")
			case "error":
				return PickupBuffer{}, fmt.Errorf("RTC worker: %s", msg.Reason)
			default:
				return PickupBuffer{}, fmt.Errorf("unexpected RTC worker event %q during pickup", msg.Type)
			}
		}
	}
}
