package stun

import "encoding/binary"

// DataChannel delivers one complete message at a time. Never interpret a
// truncated attribute, extra bytes, or a non-STUN cookie as a control frame.
func validStunFrame(data []byte) bool {
	if len(data) < 20 || data[0]&0xc0 != 0 || binary.BigEndian.Uint32(data[4:8]) != 0x2112a442 {
		return false
	}
	body := int(binary.BigEndian.Uint16(data[2:4]))
	if body%4 != 0 || body != len(data)-20 {
		return false
	}
	for off := 20; off < len(data); {
		if len(data)-off < 4 {
			return false
		}
		n := int(binary.BigEndian.Uint16(data[off+2 : off+4]))
		padded := (n + 3) &^ 3
		if padded > len(data)-off-4 {
			return false
		}
		off += 4 + padded
	}
	return true
}
