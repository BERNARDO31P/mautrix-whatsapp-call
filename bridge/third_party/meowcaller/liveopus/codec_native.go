//go:build liveopus && cgo

package liveopus

/*
#cgo LDFLAGS: -lopus -lm
#include <stdint.h>
// Stable public libopus ABI (opus.h, opus_defines.h).
// The production build uses the verified upstream static archive.
typedef struct OpusEncoder OpusEncoder;
typedef struct OpusDecoder OpusDecoder;
extern OpusEncoder *opus_encoder_create(int32_t, int, int, int *);
extern void opus_encoder_destroy(OpusEncoder *);
extern int32_t opus_encode_float(OpusEncoder *, const float *, int, unsigned char *, int32_t);
extern int opus_encoder_ctl(OpusEncoder *, int, ...);
extern OpusDecoder *opus_decoder_create(int32_t, int, int *);
extern void opus_decoder_destroy(OpusDecoder *);
extern int opus_decode_float(OpusDecoder *, const unsigned char *, int32_t, float *, int, int);
extern const char *opus_strerror(int);
extern const char *opus_get_version_string(void);
static int set_bitrate(OpusEncoder *encoder, int bitrate) {
    return opus_encoder_ctl(encoder, 4002, bitrate); // OPUS_SET_BITRATE_REQUEST
}
*/
import "C"

import (
	"fmt"
	"math"
	"runtime"
	"sync"
	"unsafe"
)

const Available = true

func Version() string { return C.GoString(C.opus_get_version_string()) }

type Encoder struct {
	mu  sync.Mutex
	ptr *C.OpusEncoder
}

type Decoder struct {
	mu  sync.Mutex
	ptr *C.OpusDecoder
}

func opusError(operation string, code C.int) error {
	return fmt.Errorf("%s: %s (%d)", operation, C.GoString(C.opus_strerror(code)), int(code))
}

func NewEncoder(bitrate int) (*Encoder, error) {
	if err := CheckBitrate(bitrate); err != nil {
		return nil, err
	}
	var code C.int
	ptr := C.opus_encoder_create(SampleRate, 1, 2048, &code) // OPUS_APPLICATION_VOIP
	if ptr == nil || code != 0 {
		if ptr != nil {
			C.opus_encoder_destroy(ptr)
		}
		return nil, opusError("create Opus encoder", code)
	}
	enc := &Encoder{ptr: ptr}
	if bitrate != 0 {
		if code = C.set_bitrate(ptr, C.int(bitrate)); code != 0 {
			enc.Close()
			return nil, opusError("set Opus bitrate", code)
		}
	}
	runtime.SetFinalizer(enc, (*Encoder).Close)
	return enc, nil
}

func (e *Encoder) Encode(pcm []float32) ([]byte, error) {
	if !ValidFrameSamples(len(pcm)) {
		return nil, fmt.Errorf("invalid Opus PCM length: %d", len(pcm))
	}
	for _, sample := range pcm {
		if math.IsNaN(float64(sample)) || math.IsInf(float64(sample), 0) || sample < -1 || sample > 1 {
			return nil, fmt.Errorf("invalid Opus PCM sample")
		}
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.ptr == nil {
		return nil, fmt.Errorf("Opus encoder closed")
	}
	out := make([]byte, 1275)
	n := C.opus_encode_float(e.ptr, (*C.float)(unsafe.Pointer(&pcm[0])), C.int(len(pcm)), (*C.uchar)(unsafe.Pointer(&out[0])), C.int32_t(len(out)))
	if n <= 0 {
		return nil, opusError("encode Opus packet", C.int(n))
	}
	return out[:int(n)], nil
}

func (e *Encoder) Close() {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.ptr != nil {
		C.opus_encoder_destroy(e.ptr)
		e.ptr = nil
	}
	runtime.SetFinalizer(e, nil)
}

func NewDecoder() (*Decoder, error) {
	var code C.int
	ptr := C.opus_decoder_create(SampleRate, 1, &code)
	if ptr == nil || code != 0 {
		if ptr != nil {
			C.opus_decoder_destroy(ptr)
		}
		return nil, opusError("create Opus decoder", code)
	}
	dec := &Decoder{ptr: ptr}
	runtime.SetFinalizer(dec, (*Decoder).Close)
	return dec, nil
}

func (d *Decoder) Decode(packet []byte) ([]float32, error) {
	if len(packet) == 0 || len(packet) > MaxPacketBytes {
		return nil, fmt.Errorf("invalid Opus packet length: %d", len(packet))
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.ptr == nil {
		return nil, fmt.Errorf("Opus decoder closed")
	}
	out := make([]float32, MaxDecodeSamples)
	n := C.opus_decode_float(d.ptr, (*C.uchar)(unsafe.Pointer(&packet[0])), C.int32_t(len(packet)), (*C.float)(unsafe.Pointer(&out[0])), C.int(len(out)), 0)
	if n <= 0 || int(n) > len(out) {
		return nil, opusError("decode Opus packet", C.int(n))
	}
	return out[:int(n)], nil
}

func (d *Decoder) Close() {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.ptr != nil {
		C.opus_decoder_destroy(d.ptr)
		d.ptr = nil
	}
	runtime.SetFinalizer(d, nil)
}
