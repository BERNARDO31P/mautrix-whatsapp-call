package relay

import (
	"net"
	"testing"
	"time"

	"github.com/pion/datachannel"
)

// TestClassifyFirstByte pins every branch of the first-byte relay packet demux
// against the reference's inline assertions.
func TestClassifyFirstByte(t *testing.T) {
	cases := []struct {
		data []byte
		want RelayPacketKind
	}{
		{[]byte{0x00, 0x01}, RelayPacketStun},
		{[]byte{0x00, 0x03}, RelayPacketStun},
		{[]byte{0x80, 0xc8}, RelayPacketRtcp},
		{[]byte{0x81, 0xc8}, RelayPacketRtcp},
		{[]byte{0x91, 0xce}, RelayPacketRtcp},
		{[]byte{0x90, 0x78}, RelayPacketRtp},
		{[]byte{0x90, 0xe1}, RelayPacketRtp},
		{[]byte{0xff, 0xff}, RelayPacketOther},
		{[]byte{0x00}, RelayPacketOther},
	}
	for _, c := range cases {
		if got := ClassifyRelayPacket(c.data); got != c.want {
			t.Errorf("ClassifyRelayPacket(%x) = %d, want %d", c.data, got, c.want)
		}
	}
}

func TestCountedPacketConn(t *testing.T) {
	a, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close()
	b, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()
	ca := &countedPacketConn{PacketConn: a}
	cb := &countedPacketConn{PacketConn: b}
	if err := cb.SetReadDeadline(time.Now().Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	if _, err := ca.WriteTo([]byte{1, 2}, b.LocalAddr()); err != nil {
		t.Fatal(err)
	}
	if _, _, err := cb.ReadFrom(make([]byte, 8)); err != nil {
		t.Fatal(err)
	}
	if ca.sent.Load() != 1 || ca.received.Load() != 0 || cb.sent.Load() != 0 || cb.received.Load() != 1 {
		t.Fatal("incorrect UDP packet counts")
	}
	if _, err := ca.WriteTo([]byte{1}, &net.TCPAddr{}); err == nil {
		t.Fatal("expected invalid address error")
	}
	if ca.sent.Load() != 1 {
		t.Fatal("failed write counted as sent")
	}
}

func TestRelayHandshakePacketDiagnostics(t *testing.T) {
	listen := func() *net.UDPConn {
		t.Helper()
		c, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = c.Close() })
		return c
	}
	local, peer, other := listen(), listen(), listen()
	c := &countedPacketConn{PacketConn: local, peer: peer.LocalAddr().(*net.UDPAddr)}
	if err := c.SetReadDeadline(time.Now().Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	record := func(kind byte) []byte {
		p := make([]byte, 14)
		p[0], p[1], p[2], p[12] = kind, 0xfe, 0xfd, 1
		return p
	}
	for _, item := range []struct {
		source *net.UDPConn
		data   []byte
	}{
		{peer, record(22)}, {peer, record(21)}, {peer, []byte{22, 0xfe, 0xfd}},
		{peer, record(22)[:13]}, {other, record(22)},
	} {
		if _, err := item.source.WriteToUDP(item.data, local.LocalAddr().(*net.UDPAddr)); err != nil {
			t.Fatal(err)
		}
		if _, _, err := c.ReadFrom(make([]byte, 64)); err != nil {
			t.Fatal(err)
		}
	}
	if c.received.Load() != 5 || c.peerReceived.Load() != 4 || c.foreignReceived.Load() != 1 || c.peerHandshake.Load() != 1 || c.peerAlert.Load() != 1 || c.peerOther.Load() != 2 {
		t.Fatal("incorrect peer or DTLS diagnostic counts")
	}
}

func TestDTLSRecordMetadata(t *testing.T) {
	valid := []byte{22, 0xfe, 0xfd, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 42}
	if got := dtlsRecordMetadata(valid); got != 0x16fefd {
		t.Fatalf("unexpected DTLS metadata: %x", got)
	}
	valid[13] = 99
	if got := dtlsRecordMetadata(valid); got != 0x16fefd {
		t.Fatal("handshake body must not influence metadata")
	}
	for _, invalid := range [][]byte{nil, valid[:12], valid[:13], {22, 3, 3, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}} {
		if got := dtlsRecordMetadata(invalid); got != 0 {
			t.Fatalf("invalid record classified: %x", got)
		}
	}
}

func TestMediaDataChannelUsesRealtimeReliability(t *testing.T) {
	cfg := mediaDataChannelConfig()
	if !cfg.Negotiated || cfg.ChannelType != datachannel.ChannelTypePartialReliableRexmitUnordered || cfg.ReliabilityParameter != 0 || cfg.Label != DataChannelLabel || cfg.LoggerFactory == nil {
		t.Fatalf("unexpected media data channel config: %+v", cfg)
	}
}
