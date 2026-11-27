// Package liveopus provides the explicitly negotiated RFC Opus path. It is
// separate from the existing MLow implementation.
package liveopus

import "fmt"

const SampleRate = 16000
const MaxDecodeSamples = SampleRate * 120 / 1000
const MaxPacketBytes = 1275 * 3 // bounded receive packet; one 60 ms encode <= 1275

func ValidFrameSamples(samples int) bool {
	switch samples {
	case 40, 80, 160, 320, 640, 960:
		return true
	default:
		return false
	}
}

func CheckBitrate(bitrate int) error {
	if bitrate != 0 && (bitrate < 500 || bitrate > 512000) {
		return fmt.Errorf("invalid Opus bitrate: %d", bitrate)
	}
	return nil
}
