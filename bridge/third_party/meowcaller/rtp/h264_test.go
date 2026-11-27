package rtp

// Ported from WaCalls (jotadev66, MIT) feat/video-calls:
// https://github.com/JotaDev66/WaCalls/blob/2d6a1f666426049a89ef9541414e771acdcf8a16/internal/voip/transport/h264_packet_test.go

import (
	"bytes"
	"testing"
)

func depacketizeAll(d *H264Depacketizer, pkts [][]byte) [][]byte {
	var out [][]byte
	for _, p := range pkts {
		out = append(out, d.Depacketize(p)...)
	}
	return out
}

func TestH264SingleNALURoundtrip(t *testing.T) {
	nalu := []byte{0x41, 0x9a, 0x00, 0x11, 0x22}
	pkts := PackageH264NALU(nalu)
	if len(pkts) != 1 {
		t.Fatalf("small NALU should be one packet, got %d", len(pkts))
	}
	var d H264Depacketizer
	got := depacketizeAll(&d, pkts)
	if len(got) != 1 || !bytes.Equal(got[0], nalu) {
		t.Fatalf("roundtrip mismatch: got %x", got)
	}
}

func TestH264FUARoundtrip(t *testing.T) {
	nalu := make([]byte, 5000)
	nalu[0] = 0x65
	for i := 1; i < len(nalu); i++ {
		nalu[i] = byte(i)
	}
	pkts := PackageH264NALU(nalu)
	if len(pkts) < 2 {
		t.Fatalf("large NALU should fragment, got %d packets", len(pkts))
	}
	for _, p := range pkts {
		if p[0]&0x1F != h264FuaType {
			t.Fatalf("fragment is not FU-A: %x", p[0])
		}
	}
	var d H264Depacketizer
	got := depacketizeAll(&d, pkts)
	if len(got) != 1 || !bytes.Equal(got[0], nalu) {
		t.Fatalf("FU-A reassembly mismatch: got %d NALUs", len(got))
	}
}

func TestH264AccessUnitAssemblerWaitsForIDRAfterPacketGap(t *testing.T) {
	idr := make([]byte, 2400)
	idr[0] = 0x65
	for i := 1; i < len(idr); i++ {
		idr[i] = byte(i)
	}
	idrPackets := PackageH264NALU(idr)
	if len(idrPackets) != 4 {
		t.Fatalf("IDR packet count = %d, want 4", len(idrPackets))
	}

	var assembler H264AccessUnitAssembler
	if got, complete, recovery := assembler.Push(100, 0, false, idrPackets[0]); complete || recovery || got != nil {
		t.Fatalf("first fragment = complete:%t recovery:%t bytes:%d", complete, recovery, len(got))
	}
	if got, complete, recovery := assembler.Push(102, 0, false, idrPackets[2]); complete || !recovery || got != nil {
		t.Fatalf("gapped fragment = complete:%t recovery:%t bytes:%d", complete, recovery, len(got))
	}
	if got, complete, recovery := assembler.Push(103, 0, true, idrPackets[3]); complete || recovery || got != nil {
		t.Fatalf("damaged IDR = complete:%t recovery:%t bytes:%d", complete, recovery, len(got))
	}
	if got, complete, recovery := assembler.Push(103, 0, true, []byte{0x41, 0x01}); complete || recovery || got != nil {
		t.Fatalf("reordered packet while recovering = complete:%t recovery:%t bytes:%d", complete, recovery, len(got))
	}

	delta := []byte{0x41, 0x9a, 0x01}
	if got, complete, recovery := assembler.Push(104, 0, true, delta); complete || recovery || got != nil {
		t.Fatalf("delta after loss = complete:%t recovery:%t bytes:%d", complete, recovery, len(got))
	}

	recovery := []byte{0x65, 0x88, 0x84}
	got, complete, request := assembler.Push(105, 0, true, recovery)
	want := append([]byte{0, 0, 0, 1}, recovery...)
	if !complete || request || !bytes.Equal(got, want) {
		t.Fatalf("recovery access unit = (%t, %t, %x), want (true, false, %x)", complete, request, got, want)
	}
}

func TestH264AccessUnitAssemblerBoundsMarkerFreeStreamAndRequestsRecoveryOnce(t *testing.T) {
	// Source of truth: https://github.com/JotaDev66/WaCalls/blob/2d6a1f666426049a89ef9541414e771acdcf8a16/internal/voip/transport/h264_packet.go#L138-L210
	var assembler H264AccessUnitAssembler
	payload := make([]byte, 64<<10)
	payload[0] = 0x41
	recoveryRequests := 0
	var sequence uint16
	for total := 0; total < maxAccessUnitBytes*2; total += len(payload) {
		_, complete, recovery := assembler.Push(sequence, 0, false, payload)
		sequence++
		if complete {
			t.Fatal("marker-free stream emitted an access unit")
		}
		if recovery {
			recoveryRequests++
		}
		if len(assembler.accessUnit) > maxAccessUnitBytes {
			t.Fatalf("access unit grew to %d bytes", len(assembler.accessUnit))
		}
	}
	if recoveryRequests != 1 {
		t.Fatalf("recovery requests = %d, want 1", recoveryRequests)
	}

	if got, complete, recovery := assembler.Push(sequence, 0, true, []byte{0x41, 1}); complete || recovery || got != nil {
		t.Fatalf("delta while recovering = complete:%t recovery:%t bytes:%d", complete, recovery, len(got))
	}
	sequence++
	idr := []byte{0x65, 1, 2}
	got, complete, recovery := assembler.Push(sequence, 0, true, idr)
	if !complete || recovery || !bytes.Equal(got, append([]byte{0, 0, 0, 1}, idr...)) {
		t.Fatalf("IDR recovery = complete:%t recovery:%t bytes:%x", complete, recovery, got)
	}
}

func TestH264AccessUnitAssemblerBoundsFragmentedNALU(t *testing.T) {
	// Source of truth: https://github.com/JotaDev66/WaCalls/blob/2d6a1f666426049a89ef9541414e771acdcf8a16/internal/voip/transport/h264_packet.go#L138-L210
	var assembler H264AccessUnitAssembler
	start := append([]byte{0x7c, 0x85}, make([]byte, 64<<10)...)
	if _, complete, recovery := assembler.Push(1, 0, false, start); complete || recovery {
		t.Fatalf("FU start = complete:%t recovery:%t", complete, recovery)
	}
	continuation := append([]byte{0x7c, 0x05}, make([]byte, 64<<10)...)
	recoveryRequests := 0
	for sequence := uint16(2); sequence < 80; sequence++ {
		_, complete, recovery := assembler.Push(sequence, 0, false, continuation)
		if complete {
			t.Fatal("unterminated FU emitted an access unit")
		}
		if recovery {
			recoveryRequests++
		}
		if len(assembler.depacketizer.fuBuf) > maxFuReassemblyBytes {
			t.Fatalf("FU buffer grew to %d bytes", len(assembler.depacketizer.fuBuf))
		}
	}
	if recoveryRequests != 1 {
		t.Fatalf("fragment recovery requests = %d, want 1", recoveryRequests)
	}
}

func TestH264STAPARoundtrip(t *testing.T) {
	sps := []byte{0x67, 0x42, 0x00, 0x1f}
	pps := []byte{0x68, 0xce, 0x3c, 0x80}
	stap := PackageH264STAPA([][]byte{sps, pps})
	if stap == nil || stap[0]&0x1F != h264StapAType {
		t.Fatalf("expected STAP-A aggregate")
	}
	var d H264Depacketizer
	got := d.Depacketize(stap)
	if len(got) != 2 || !bytes.Equal(got[0], sps) || !bytes.Equal(got[1], pps) {
		t.Fatalf("STAP-A split mismatch: got %d NALUs", len(got))
	}
}

func TestSplitAnnexB(t *testing.T) {
	n1 := []byte{0x67, 0x01, 0x02}
	n2 := []byte{0x68, 0x03}
	n3 := []byte{0x65, 0x04, 0x05, 0x06}
	var stream []byte
	stream = append(stream, 0, 0, 0, 1)
	stream = append(stream, n1...)
	stream = append(stream, 0, 0, 1)
	stream = append(stream, n2...)
	stream = append(stream, 0, 0, 0, 1)
	stream = append(stream, n3...)

	nalus := SplitAnnexB(stream)
	if len(nalus) != 3 || !bytes.Equal(nalus[0], n1) || !bytes.Equal(nalus[1], n2) || !bytes.Equal(nalus[2], n3) {
		t.Fatalf("AnnexB split mismatch: got %d NALUs %x", len(nalus), nalus)
	}
}

func TestH264AssemblerDiscardsDamagedAccessUnit(t *testing.T) {
	cases := []struct {
		name     string
		payloads [][]byte
	}{
		{"unfinished fragment at marker", [][]byte{{0x7c, 0x85, 1}}},
		{"malformed aggregate after IDR", [][]byte{{0x78, 0, 2, 0x65, 1, 0, 3, 0x41}}},
		{"aggregate trailing byte", [][]byte{{0x78, 0, 2, 0x65, 1, 0}}},
		{"fragment type changed", [][]byte{{0x7c, 0x85, 1}, {0x7c, 0x41, 2}}},
		{"fragment NRI changed", [][]byte{{0x7c, 0x85, 1}, {0x5c, 0x45, 2}}},
		{"fragment replaced by IDR", [][]byte{{0x7c, 0x85, 1}, {0x65, 2}}},
		{"orphan fragment", [][]byte{{0x7c, 0x45, 2}}},
		{"empty packet", [][]byte{nil}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var a H264AccessUnitAssembler
			seq := uint16(1)
			// A completed NAL must not survive damage to another NAL in the same AU.
			a.Push(seq, 0, false, []byte{0x65, 7})
			seq++
			requests := 0
			for i, packet := range tc.payloads {
				got, complete, recover := a.Push(seq, 0, i == len(tc.payloads)-1, packet)
				seq++
				if complete || got != nil {
					t.Fatal("damaged frame emitted")
				}
				if recover {
					requests++
				}
			}
			if requests != 1 {
				t.Fatalf("recovery requests = %d", requests)
			}
			if got, complete, _ := a.Push(seq, 0, true, []byte{0x41, 1}); got != nil || complete {
				t.Fatal("dependent frame emitted")
			}
			if got, complete, _ := a.Push(seq+1, 0, true, []byte{0x65, 2}); !complete || !bytes.Equal(got, []byte{0, 0, 0, 1, 0x65, 2}) {
				t.Fatal("clean IDR did not recover")
			}
		})
	}
}

func TestH264AssemblerGapDoesNotEmitSuffixIDR(t *testing.T) {
	var a H264AccessUnitAssembler
	a.Push(1, 0, false, []byte{0x67, 1})
	if got, complete, request := a.Push(3, 0, true, []byte{0x65, 2}); got != nil || complete || !request {
		t.Fatal("partial access unit emitted after gap")
	}
	if _, complete, _ := a.Push(4, 0, true, []byte{0x65, 3}); !complete {
		t.Fatal("subsequent IDR did not recover")
	}
}

func TestH264TimestampBoundaryDiscardsUnmarkedFrame(t *testing.T) {
	for _, tc := range []struct {
		name string
		old  []byte
	}{
		{"complete NAL without marker", []byte{0x65, 7}},
		{"unfinished FU", []byte{0x7c, 0x85, 7}},
		{"damaged frame without marker", []byte{0x7c, 0x05, 7}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var a H264AccessUnitAssembler
			a.Push(10, 100, false, tc.old)
			got, complete, request := a.Push(11, 200, true, []byte{0x65, 8})
			if !complete || request || !bytes.Equal(got, []byte{0, 0, 0, 1, 0x65, 8}) {
				t.Fatalf("new IDR = %x, complete=%t request=%t", got, complete, request)
			}
		})
	}
}

func TestH264TimestampBoundaryRequiresIDR(t *testing.T) {
	var a H264AccessUnitAssembler
	a.Push(65535, 0xffffffff, false, []byte{0x65, 7})
	if got, complete, request := a.Push(0, 0, true, []byte{0x41, 8}); got != nil || complete || !request {
		t.Fatalf("wrapped timestamp delta = %x, complete=%t request=%t", got, complete, request)
	}
	if got, complete, request := a.Push(1, 100, false, []byte{0x67, 9}); got != nil || complete || request {
		t.Fatal("unexpected SPS emission")
	}
	got, complete, request := a.Push(2, 100, true, []byte{0x65, 10})
	if !complete || request || !bytes.Equal(got, []byte{0, 0, 0, 1, 0x67, 9, 0, 0, 0, 1, 0x65, 10}) {
		t.Fatalf("recovery = %x %t %t", got, complete, request)
	}
}

func TestH264TimestampBoundaryWithGapRejectsPossiblyIncompleteNewFrame(t *testing.T) {
	var a H264AccessUnitAssembler
	a.Push(10, 100, false, []byte{0x41, 7})
	if got, complete, request := a.Push(12, 200, true, []byte{0x65, 8}); got != nil || complete || !request {
		t.Fatalf("gap = %x %t %t", got, complete, request)
	}
	if got, complete, request := a.Push(13, 300, true, []byte{0x65, 9}); !complete || request || !bytes.Equal(got, []byte{0, 0, 0, 1, 0x65, 9}) {
		t.Fatalf("recovery = %x %t %t", got, complete, request)
	}
}

func TestH264TimestampChangeAfterMarkerDoesNotRequireIDR(t *testing.T) {
	var a H264AccessUnitAssembler
	a.Push(10, 200, true, []byte{0x65, 7})
	if got, complete, request := a.Push(11, 100, true, []byte{0x41, 8}); !complete || request || !bytes.Equal(got, []byte{0, 0, 0, 1, 0x41, 8}) {
		t.Fatalf("normal boundary = %x %t %t", got, complete, request)
	}
}
