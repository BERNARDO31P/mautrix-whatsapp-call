package stun

import (
	"encoding/binary"
	"testing"
)

func TestCompleteRelaySTUNFrame(t *testing.T) {
	valid := []byte{0, 1, 0, 8, 0x21, 0x12, 0xa4, 0x42, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 0, 8, 0, 1, 9, 0, 0, 0}
	if !validStunFrame(valid) || !validStunFrame(append([]byte{0, 1, 0, 0}, valid[4:20]...)) {
		t.Fatal("complete frames rejected")
	}
	for i := 0; i < len(valid); i++ {
		if validStunFrame(valid[:i]) {
			t.Fatalf("accepted truncation at %d", i)
		}
	}
	for _, tc := range []struct {
		name  string
		index int
		value byte
	}{
		{"wrong version", 0, 0x80}, {"wrong cookie", 4, 0}, {"unaligned body", 3, 7},
		{"too short declared body", 3, 4}, {"overlong attribute", 23, 5},
	} {
		t.Run(tc.name, func(t *testing.T) {
			bad := append([]byte(nil), valid...)
			bad[tc.index] = tc.value
			if validStunFrame(bad) {
				t.Fatal("accepted malformed frame")
			}
		})
	}
	bad := append(append([]byte(nil), valid...), 0, 0, 0, 0)
	if validStunFrame(bad) {
		t.Fatal("accepted undeclared tail")
	}
	binary.BigEndian.PutUint16(bad[2:4], 12)
	if !validStunFrame(bad) {
		t.Fatal("rejected complete second zero-length TLV")
	}
}
