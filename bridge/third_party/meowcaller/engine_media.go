package meowcaller

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"io"
	"math"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/purpshell/meowcaller/diag"
	"github.com/purpshell/meowcaller/relay"
	"github.com/purpshell/meowcaller/rtp"
	"github.com/purpshell/meowcaller/srtp"
	"github.com/purpshell/meowcaller/stun"
	"github.com/rs/zerolog"
)

// The live-relay media loop: connect+allocate to the elected relay, then run the
// per-frame send/recv loop. Outbound pulls frames from the Call's Player (silence when
// idle), encodes via the selected codec + ProtectAudio, and sends to the relay; inbound classifies
// relay packets, unprotects+decodes RTP, and writes to the Call's sink.

const (
	groupRegistryInstallMaxAttempts = 32
	groupRegistryInstallBackoff     = 5 * time.Millisecond
	videoPLIInterval                = 300 * time.Millisecond
)

type videoReceiveState struct {
	participantEpoch ParticipantMediaEpoch
	role             GroupVideoRole
	requestKeyframe  atomic.Bool
	assembler        rtp.H264AccessUnitAssembler
	orientation      int
}

// A newly attached receiver has no decoder history, even if the relay is
// already forwarding an established video stream.
func newVideoReceiveState() *videoReceiveState {
	s := &videoReceiveState{orientation: -1}
	s.assembler.RequireKeyframe()
	return s
}

// A discarded access unit may have been a reference frame. Do not resume
// forwarding dependent frames to a consumer with missing decoder history.
func (s *videoReceiveState) noteVideoDelivery(delivered bool) {
	if !delivered {
		s.assembler.RequireKeyframe()
	}
}

// takeVideoPLI reserves a feedback attempt without consuming a downstream
// request when throttled. Assembly recovery remains pending until an IDR.
// Only the receive loop accesses the assembler and lastSent.
func (s *videoReceiveState) takeVideoPLI(lastSent map[uint32]time.Time, ssrc uint32, now time.Time) (send, explicit bool) {
	if !s.assembler.NeedsKeyframe() && !s.requestKeyframe.Load() {
		return false, false
	}
	if !shouldSendVideoPLI(lastSent, ssrc, now) {
		return false, false
	}
	return true, s.requestKeyframe.Swap(false)
}

// maybeStartMedia launches the media loop for callID once both the callKey and the relay
// endpoint are known. It is idempotent — the loop starts exactly once per call.
func (e *engine) maybeStartMedia(callID string) {
	e.maybeStartMediaMatching(callID, nil)
}

func (e *engine) maybeStartMediaMatching(callID string, expected *engineCall) {
	e.mu.Lock()
	m := e.calls[callID]
	if m == nil || m.started || (expected != nil && m != expected) || (m.linkCall && !m.linkAdmitted) {
		e.mu.Unlock()
		return
	}
	if m.group && (!m.hasGroupEpoch || len(m.groupRawEpoch) != 32 ||
		m.groupUpdate == nil || m.groupUpdate.Relay == nil) {
		e.c.log.Debug().
			Str("call_id", callID).
			Bool("has_group_epoch", m.hasGroupEpoch).
			Int("group_epoch_bytes", len(m.groupRawEpoch)).
			Bool("has_group_update", m.groupUpdate != nil).
			Bool("has_group_relay", m.groupUpdate != nil && m.groupUpdate.Relay != nil).
			Msg("group media prerequisites incomplete")
		e.mu.Unlock()
		return
	}
	if !m.group && len(mediaRelayCandidates(m.relay, m.direction == CallDirectionIncoming, false)) == 0 {
		e.c.log.Debug().Str("call_id", callID).Msg("waiting for usable direct relay allocation")
		e.mu.Unlock()
		return
	}
	if !m.group && len(m.callKey) == 0 {
		e.mu.Unlock()
		return
	}
	rd := m.relay
	if m.group {
		groupRelay, err := groupRelayData(*m.groupUpdate, m.direction == CallDirectionIncoming)
		if err != nil {
			e.mu.Unlock()
			e.c.log.Debug().Err(err).Str("call_id", callID).Msg("group media is waiting for an authoritative relay")
			return
		}
		rd = groupRelay
	}
	if rd == nil {
		e.mu.Unlock()
		return
	}
	m.started = true
	mctx, cancel := context.WithCancel(context.Background())
	m.cancel = cancel
	call := m.call
	var callKey []byte
	if m.group {
		callKey = append([]byte(nil), m.groupRawEpoch...)
	} else {
		callKey = append([]byte(nil), m.callKey...)
	}
	selfLID, peerLID := m.selfLID, m.peerLID
	inbound := m.direction == CallDirectionIncoming
	e.mu.Unlock()

	if call != nil {
		call.advancePhase(CallPhaseConnecting)
	}
	e.c.log.Info().Str("call_id", callID).Msg("starting media")
	go func() {
		defer clear(callKey)
		if err := e.runMedia(mctx, callID, call, callKey, selfLID, peerLID, rd, inbound); err != nil {
			e.c.log.Warn().Err(err).Str("call_id", callID).Msg("media ended")
			if mctx.Err() == nil {
				e.finishCallIfCurrent(call, "media_failed")
			}
		}
	}()
}

// connectAndAllocate opens the relay DataChannel and sends the STUN allocate, returning
// the channel and the allocate bytes (re-sent by the keepalive).
//
// NOT VALIDATED: live-relay only.
// Check allocation prerequisites before spending a transport attempt.
func usableRelayEndpoint(rd *relayData, ep *relayEndpoint) bool {
	if rd == nil || ep == nil || len(rd.relayKeyASCII) == 0 ||
		uint64(ep.tokenID) >= uint64(len(rd.relayTokens)) || len(rd.relayTokens[ep.tokenID]) == 0 || len(ep.addresses) == 0 {
		return false
	}
	addr := ep.addresses[0]
	ip := net.ParseIP(addr.ipv4)
	return addr.port != 0 && ip != nil && ip.To4() != nil && !ip.IsUnspecified() && !ip.IsMulticast()
}

func mediaRelayCandidates(rd *relayData, inbound, group bool) []*relayEndpoint {
	if rd == nil {
		return nil
	}
	preferred := getMediaRelayEndpoint(rd, inbound)
	if preferred == nil {
		return nil
	}
	if group {
		// Group subscription updates are bound to the elected relay.
		return []*relayEndpoint{preferred}
	}
	candidates := make([]*relayEndpoint, 0, 3)
	// The parser can receive repeated te2 entries. Identical transport and
	// allocation inputs must not consume multiple slots in the retry budget.
	// Distinct tokens at the same address remain independent allocations.
	type candidateKey struct {
		ip      string
		port    uint16
		tokenID uint32
	}
	seen := make(map[candidateKey]bool, 3)
	add := func(ep *relayEndpoint) {
		if !usableRelayEndpoint(rd, ep) {
			return
		}
		addr := ep.addresses[0]
		key := candidateKey{net.ParseIP(addr.ipv4).To4().String(), addr.port, ep.tokenID}
		if !seen[key] {
			seen[key] = true
			candidates = append(candidates, ep)
		}
	}
	add(preferred)
	// Keep the elected relay class. An inbound FNA relay cannot be replaced
	// by a non-FNA endpoint that does not carry the caller's media.
	for i := range rd.endpoints {
		ep := &rd.endpoints[i]
		if ep == preferred || (inbound && preferred.isFNA && !ep.isFNA) || (!inbound && !preferred.isFNA && ep.isFNA) {
			continue
		}
		add(ep)
		if len(candidates) == 3 {
			break
		}
	}
	return candidates
}

// relayInitialSetup retains the early direct relay packets for the ordinary
// media reader. A received packet is not itself proof of allocation or media.
type relayInitialSetup struct {
	channel      *relay.RelayMediaChannel
	allocation   []byte
	firstPackets [][]byte
	pingTx       *[12]byte
}

func (e *engine) connectAndAllocate(ctx context.Context, rd *relayData, streamSsrcs [9]uint32, inbound, group bool) (relayInitialSetup, error) {
	candidates := mediaRelayCandidates(rd, inbound, group)
	return tryRelayCandidates(ctx, candidates, func(ep *relayEndpoint, timeout time.Duration) (relayInitialSetup, error) {
		return e.connectAndAllocateEndpoint(ctx, rd, streamSsrcs, ep, group, timeout)
	}, func(attempt int, ep *relayEndpoint, err error) {
		e.c.log.Warn().Err(err).Int("attempt", attempt).Int("candidates", len(candidates)).Str("relay_name", ep.relayName).Msg("relay setup attempt failed")
	}, func(setup relayInitialSetup) {
		if setup.channel != nil {
			_ = setup.channel.Abort()
		}
	})
}

func (e *engine) connectAndAllocateEndpoint(ctx context.Context, rd *relayData, streamSsrcs [9]uint32, ep *relayEndpoint, group bool, timeout time.Duration) (relayInitialSetup, error) {
	log := e.c.log
	if !usableRelayEndpoint(rd, ep) {
		return relayInitialSetup{}, fmt.Errorf("relay endpoint or allocation credentials are incomplete")
	}
	addr := &net.UDPAddr{IP: net.ParseIP(ep.addresses[0].ipv4), Port: int(ep.addresses[0].port)}
	log.Info().Str("relay_name", ep.relayName).Str("addr", addr.String()).Msg("connecting media transport to relay")
	e.c.diag.Emit("relay", map[string]any{
		"event": "endpoint", "relay_name": ep.relayName,
		"ipv4": ep.addresses[0].ipv4, "port": ep.addresses[0].port, "token_id": ep.tokenID,
	})
	// The allocation always uses the signalled address, not the alternate transport port.
	endpointXor, ok := stun.EncodeXorRelayEndpoint(ep.addresses[0].ipv4, ep.addresses[0].port, log)
	if !ok {
		return relayInitialSetup{}, fmt.Errorf("bad endpoint XOR")
	}
	e.c.diag.Emit("relay", map[string]any{
		"event": "keying", "token_id": ep.tokenID, "token_count": len(rd.relayTokens),
		"relay_key_bytes": len(rd.relayKeyASCII), "token_bytes": len(rd.relayTokens[ep.tokenID]),
	})
	connectCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	setup, _, err := dialDirectRelayPorts(connectCtx, ep.addresses[0].port, group, func(dialCtx context.Context, port uint16) (relayInitialSetup, error) {
		target := &net.UDPAddr{IP: addr.IP, Port: int(port)}
		log.Debug().Str("addr", target.String()).Uint16("advertised_port", ep.addresses[0].port).Msg("dialing relay transport port")
		ch, err := awaitRelayConnection(dialCtx, func() (*relay.RelayMediaChannel, error) {
			return relay.ConnectRelayMediaContext(dialCtx, target, relay.WithLogger(log))
		}, func(ch *relay.RelayMediaChannel) {
			if ch != nil {
				_ = ch.Abort()
			}
		})
		if err != nil {
			return relayInitialSetup{}, err
		}
		keep := false
		defer func() {
			if !keep {
				_ = ch.Abort()
			}
		}()
		stopAllocate := ch.WatchCancellation(dialCtx)
		defer stopAllocate()
		if err := relayContextError(dialCtx); err != nil {
			return relayInitialSetup{}, err
		}
		log.Info().Str("relay_name", ep.relayName).Uint16("transport_port", port).Uint16("advertised_port", ep.addresses[0].port).Msg("relay DataChannel open")
		var tx [12]byte
		if _, err := rand.Read(tx[:]); err != nil {
			return relayInitialSetup{}, fmt.Errorf("generate relay allocation transaction ID: %w", err)
		}
		allocation := stun.BuildWasmStunAllocateRequestWithStreamSsrcs(tx, rd.relayTokens[ep.tokenID], endpointXor, streamSsrcs, rd.relayKeyASCII, log)
		if _, err := ch.Send(allocation); err != nil {
			return relayInitialSetup{}, fmt.Errorf("allocate send: %w", err)
		}
		log.Info().Int("bytes", len(allocation)).Msg("sent STUN allocate")
		e.c.diag.Emit("stun", map[string]any{"event": "allocate_sent", "bytes": len(allocation), "tx_id_hex": hex.EncodeToString(tx[:]), "stream_ssrcs": streamSsrcs})
		result := relayInitialSetup{channel: ch, allocation: allocation}
		if !group {
			var ptx [12]byte
			if _, err := rand.Read(ptx[:]); err != nil {
				return relayInitialSetup{}, fmt.Errorf("generate relay consent transaction ID: %w", err)
			}
			ping := stun.BuildWhatsappPing(ptx, log)
			if _, err := ch.Send(ping[:]); err != nil {
				return relayInitialSetup{}, fmt.Errorf("initial relay consent ping: %w", err)
			}
			e.c.diag.Emit("stun", map[string]any{"event": "consent_ping_sent", "tx_id_hex": hex.EncodeToString(ptx[:]), "ping_hex": hex.EncodeToString(ping[:])})
			result.pingTx = &ptx
			packets, err := awaitRelayPackets(dialCtx, ch.Recv, ch.WatchCancellation, func(packet []byte) (bool, error) {
				// A matching allocation result is decisive. A binding request or
				// early media must reach the normal reader without waiting for an
				// acknowledgement that the relay might not send first.
				if mt, ok := stun.StunMessageType(packet); ok {
					switch mt {
					case stun.MsgAllocateSuccess, stun.MsgAllocateError:
						if got, ok := stun.StunTransactionID(packet); ok && bytes.Equal(got, tx[:]) {
							if mt == stun.MsgAllocateSuccess {
								return true, nil
							}
							e.c.diag.Emit("stun", map[string]any{"event": "allocation_response", "success": false, "transport_port": port})
							if code, ok := stun.ParseStunErrorCode(packet, log); ok {
								return false, fmt.Errorf("direct relay allocation rejected: STUN error %d", code)
							}
							return false, fmt.Errorf("direct relay allocation rejected")
						}
					case stun.MsgBindingRequest:
						return true, nil
					}
					return false, nil
				}
				kind := relay.ClassifyRelayPacket(packet)
				return kind == relay.RelayPacketRtp || kind == relay.RelayPacketRtcp, nil
			})
			if err != nil {
				e.c.diag.Emit("stun", map[string]any{"event": "allocation_preflight_failed", "transport_port": port, "error": err.Error()})
				return relayInitialSetup{}, fmt.Errorf("direct relay allocation receive: %w", err)
			}
			result.firstPackets = packets
		}
		if !stopAllocate() || dialCtx.Err() != nil {
			return relayInitialSetup{}, relayContextError(dialCtx)
		}
		ch.BindLifetime(ctx)
		keep = true
		return result, nil
	})
	if err != nil {
		return relayInitialSetup{}, fmt.Errorf("relay setup: %w", err)
	}
	return setup, nil
}

// runMedia runs the per-frame media loop over the relay DataChannel: the Player's frames
// (or silence) → negotiated codec → E2E-SRTP protect → DataChannel, and DataChannel → classify →
// unprotect → negotiated decoder → the Call's sink. A 1 Hz allocate+ping keepalive holds the
// relay's consent freshness; the relay's binding-requests are answered with
// binding-success. The working recipe is preserved exactly: a consent ping (0x0801) goes
// out with the allocate at t+0, BEFORE any RTP; no STUN binding-requests are ever sent.
//
// NOT VALIDATED: live-relay only.
func (e *engine) runMedia(ctx context.Context, callID string, call *Call, callKey []byte, selfLID, peerLID string, rd *relayData, inbound bool) error {
	ctx, cancelMedia := context.WithCancelCause(ctx)
	defer cancelMedia(nil)
	// Bind the codec check and late-signaling cancellation under the same owner lock.
	// Bind codec, packet duration and RTP clock to this media generation.
	e.mu.Lock()
	owner := e.mediaCallLocked(callID, call)
	if owner == nil {
		e.mu.Unlock()
		return fmt.Errorf("meowcaller: call replaced before codec admission")
	}
	group := owner.group
	audioConfig, codecErr := configuredLiveAudio(owner.codec, owner.audioFrameMs, owner.audioBitrate)
	if codecErr == nil {
		owner.mediaCodecFailure = func(codec AudioCodec, frameMs, bitrate int) {
			checkLiveAudioChange(audioConfig, codec, frameMs, bitrate, cancelMedia)
		}
	}
	e.mu.Unlock()
	if codecErr != nil {
		return codecErr
	}
	defer func() {
		e.mu.Lock()
		if e.mediaCallLocked(callID, call) == owner {
			owner.mediaCodecFailure = nil
		}
		e.mu.Unlock()
	}()
	log := e.c.log.With().Str("call_id", callID).Logger()
	enc, closeEncoder, err := newLiveAudioEncoder(audioConfig, log)
	if err != nil {
		return err
	}
	// Until the sender starts, startup owns the encoder. Once started, the
	// sender closes it itself; a caller-provided AudioSource may block.
	encoderOwnedBySender := false
	defer func() {
		if !encoderOwnedBySender {
			closeEncoder()
		}
	}()
	if ctx.Err() != nil {
		return context.Cause(ctx)
	}
	selfParticipantID := rtp.FormatE2ESrtpParticipantID(selfLID)
	ssrc, err := rtp.DeriveWasmParticipantSsrc(callID, selfParticipantID, 0, log)
	if err != nil {
		return err
	}
	videoSelfSsrc, err := rtp.DeriveWasmParticipantSsrc(callID, selfParticipantID, rtp.VideoSlotWord, log)
	if err != nil {
		return err
	}
	appDataSelfSsrc, err := rtp.DeriveWasmParticipantSsrc(callID, selfParticipantID, rtp.AppDataSlotWord, log)
	if err != nil {
		return err
	}
	hbhFECTXSSRC, err := rtp.DeriveWasmParticipantSsrc(callID, selfParticipantID, rtp.HBHFECTXSlotWord, log)
	if err != nil {
		return err
	}
	hbhFECRXSSRC, err := rtp.DeriveWasmParticipantSsrc(callID, selfParticipantID, rtp.HBHFECRXSlotWord, log)
	if err != nil {
		return err
	}
	streamSsrcs, err := rtp.DeriveWasmRelayStreamSsrcs(callID, selfParticipantID, log)
	if err != nil {
		return err
	}
	streamSsrcs, err = prepareWasmRelayStreamSSRCs(streamSsrcs, appDataSelfSsrc, rand.Reader)
	if err != nil {
		return err
	}
	setup, err := e.connectAndAllocate(ctx, rd, streamSsrcs, inbound, group)
	if err != nil {
		return err
	}
	ch, allocate := setup.channel, setup.allocation
	// Stop protocol I/O before joining senders. Closing only the UDP socket
	// does not guarantee an SCTP/DataChannel writer has been released.
	stopRelay := sync.OnceFunc(func() {
		cancelMedia(nil)
		_ = ch.Abort()
	})
	defer stopRelay()
	allocateState := newGroupRelayAllocateStateWithHBHFEC(
		allocate,
		rd.relayKeyASCII,
		[2]uint32{hbhFECTXSSRC, hbhFECRXSSRC},
	)

	allocationStarted := time.Now()
	var consent relayConsentTransactions
	// Send a consent ping (0x0801) immediately, together with the allocate and BEFORE any
	// RTP. The relay won't forward the peer's media until consent (ping → pong) is
	// established; RTP sent before the first ping is dropped and the relay never bridges.
	if setup.pingTx != nil {
		consent.remember(*setup.pingTx, time.Now())
	} else {
		var ptx [12]byte
		if _, err := rand.Read(ptx[:]); err != nil {
			return fmt.Errorf("generate relay consent transaction ID: %w", err)
		}
		consent.remember(ptx, time.Now())
		initPing := stun.BuildWhatsappPing(ptx, log)
		if _, err := ch.Send(initPing[:]); err != nil {
			return fmt.Errorf("initial relay consent ping: %w", err)
		}
		e.c.diag.Emit("stun", map[string]any{
			"event": "consent_ping_sent", "tx_id_hex": hex.EncodeToString(ptx[:]),
			"ping_hex": hex.EncodeToString(initPing[:]),
		})
	}

	log.Info().
		Str("self_lid", selfLID).
		Str("peer_lid", peerLID).
		Str("ssrc", fmt.Sprintf("0x%08x", ssrc)).
		Str("video_ssrc", fmt.Sprintf("0x%08x", videoSelfSsrc)).
		Str("app_data_ssrc", fmt.Sprintf("0x%08x", appDataSelfSsrc)).
		Msg("media session")
	e.c.diag.Emit("ssrc", map[string]any{
		"call_id": callID, "ssrc": ssrc, "video_ssrc": videoSelfSsrc, "app_data_ssrc": appDataSelfSsrc,
		"stream_ssrcs": streamSsrcs, "self_lid": selfLID,
		"participant_id": selfParticipantID,
	})

	e.mu.Lock()
	m := e.mediaCallLocked(callID, call)
	if m == nil {
		e.mu.Unlock()
		return fmt.Errorf("meowcaller: call replaced during media startup")
	}
	audioReceivers := (*participantReceiveRegistry)(nil)
	var groupMode atomic.Bool
	if m != nil {
		audioReceivers = m.groupReceivers
		groupMode.Store(m.group)
	}
	e.mu.Unlock()
	if audioReceivers == nil {
		audioReceivers, err = newParticipantReceiveRegistry(
			callID,
			callKey,
			selfLID,
			peerLID,
			func() participantAudioDecoder {
				return newLiveParticipantDecoder(audioConfig, log)
			},
			WithLogger(log),
		)
		if err != nil {
			return err
		}
		unpublishedReceivers := audioReceivers
		defer func() {
			if unpublishedReceivers != nil {
				unpublishedReceivers.clear()
			}
		}()
		if groupMode.Load() {
			installed := false
			for attempt := 0; attempt < groupRegistryInstallMaxAttempts; attempt++ {
				if err = ctx.Err(); err != nil {
					return err
				}
				e.mu.Lock()
				current := e.mediaCallLocked(callID, call)
				if current == nil || !current.group || current.groupUpdate == nil ||
					!current.hasGroupEpoch || len(current.groupRawEpoch) != 32 {
					e.mu.Unlock()
					return fmt.Errorf("meowcaller: group media prerequisites changed during startup")
				}
				update := cloneGroupCallUpdate(*current.groupUpdate)
				rawEpoch := append([]byte(nil), current.groupRawEpoch...)
				transactionID := current.groupEpochTxID
				e.mu.Unlock()

				if err = audioReceivers.ApplyGroupUpdate(update); err == nil {
					err = audioReceivers.ApplyGroupRawEpoch(transactionID, rawEpoch)
				}
				clear(rawEpoch)
				if err != nil {
					return fmt.Errorf("initialize group participant media: %w", err)
				}

				e.mu.Lock()
				current = e.mediaCallLocked(callID, call)
				if current == nil || !current.group {
					e.mu.Unlock()
					return fmt.Errorf("meowcaller: group call ended during media startup")
				}
				if current.groupReceivers != nil {
					discarded := audioReceivers
					audioReceivers = current.groupReceivers
					e.mu.Unlock()
					discarded.clear()
					unpublishedReceivers = nil
					installed = true
					break
				}
				if current.groupUpdate != nil &&
					current.groupUpdate.TransactionID <= update.TransactionID &&
					current.groupEpochTxID <= transactionID {
					current.groupReceivers = audioReceivers
					e.mu.Unlock()
					unpublishedReceivers = nil
					installed = true
					break
				}
				e.mu.Unlock()
				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-time.After(groupRegistryInstallBackoff):
				}
			}
			if !installed {
				return fmt.Errorf(
					"meowcaller: group media roster changed during %d startup attempts",
					groupRegistryInstallMaxAttempts,
				)
			}
		} else {
			e.mu.Lock()
			current := e.mediaCallLocked(callID, call)
			if current == nil {
				e.mu.Unlock()
				return fmt.Errorf("meowcaller: direct call ended during media startup")
			}
			if current.groupReceivers == nil {
				current.groupReceivers = audioReceivers
				e.mu.Unlock()
			} else {
				discarded := audioReceivers
				audioReceivers = current.groupReceivers
				e.mu.Unlock()
				discarded.clear()
			}
			unpublishedReceivers = nil
		}
	}
	// Only the selected/published registry may fence this call. Unpublished
	// startup candidates can be discarded without invalidating a live registry.
	audioReceivers.setMediaEpochObserver(call.recordParticipantMediaEpoch)
	audioPlayout := newAudioPlayoutBuffer()
	audioPlayout.clockScale = audioConfig.clockRate() / SampleRate
	var audioPlayoutMu sync.Mutex
	var groupMixing atomic.Bool
	defer func() {
		// Stop background media I/O before flushing the playout buffer.
		cancelMedia(nil)
		if groupMixing.Load() {
			return
		}
		audioPlayoutMu.Lock()
		defer audioPlayoutMu.Unlock()
		_, sink := callPlayerSink(call)
		_ = audioPlayout.Flush(sink)
	}()
	audioMixer := newParticipantAudioMixer()
	var audioSinkFramer participantAudioSinkFramer
	runMixedPlayout := func() {
		ticker := time.NewTicker(time.Duration(participantAudioMixChunkSamples) * time.Second / SampleRate)
		defer ticker.Stop()
		playoutStarted := false
		var writeFailures uint64
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
			}
			if ctx.Err() != nil {
				return
			}
			if !groupMixing.Load() {
				continue
			}
			_, sink := callPlayerSink(call)
			if sink == nil {
				continue
			}
			chunk, ok := audioMixer.MixChunk()
			if !ok {
				continue
			}
			frame, ok := audioSinkFramer.Push(chunk)
			if !ok {
				continue
			}
			if err := sink.WriteFrame(frame); err != nil {
				if writeFailures++; writeFailures == 1 {
					log.Warn().Err(err).Msg("failed to write mixed WhatsApp audio")
				}
				continue
			}
			if !playoutStarted {
				playoutStarted = true
				log.Info().
					Int("prefill_ms", participantAudioMixerPrefillSamples*1000/SampleRate).
					Int("chunk_ms", participantAudioMixChunkSamples*1000/SampleRate).
					Msg("started participant-mixed inbound audio playout")
			}
		}
	}
	txPipe, err := NewMediaPipeline(callKey, selfLID, peerLID, ssrc, audioConfig.ticksPerPacket(), WithLogger(log))
	if err != nil {
		return err
	}
	if err = audioReceivers.attachSendPipeline(txPipe); err != nil {
		return fmt.Errorf("attach audio RTP sender: %w", err)
	}
	audioRtcp, err := newMediaSrtcpSender(callKey, selfLID, ssrc, false)
	if err != nil {
		return err
	}
	if err = audioReceivers.attachSRTCPSender(audioRtcp); err != nil {
		return fmt.Errorf("attach audio SRTCP sender: %w", err)
	}
	// The derived E2E-SRTP keys live inside MediaPipeline; record the derivation INPUTS
	// (callKey + participant-ID info strings) so a reference can re-derive and compare.
	e.c.diag.Emit("srtp", map[string]any{
		"event": "media_keys_input", "call_id": callID, "ssrc": ssrc,
		"self_participant_id": selfParticipantID,
		"peer_participant_id": rtp.FormatE2ESrtpParticipantID(peerLID),
		"call_key_bytes":      len(callKey),
	})
	e.c.diag.Emit("meta", map[string]any{
		"event": "media_start", "call_id": callID, "self_lid": selfLID,
		"peer_lid": peerLID, "ssrc": ssrc,
	})

	// Observe actual responses independently of packet arrival. The direct-call
	// allocation is fixed; group subscriptions may replace their transaction.
	var allocationWatch relayAllocationWatch
	runAllocationWatch := func() {
		runRelayAllocationWatch(ctx, allocationStarted, func(after time.Duration) {
			status := allocationWatch.status()
			if status == relayAllocationAcknowledged || status == relayAllocationRejected || (group && status != relayAllocationSilent) {
				return
			}
			log.Warn().Dur("after", after).Str("allocation_status", status).Msg("relay allocation not acknowledged")
			e.c.diag.Emit("stun", map[string]any{"event": "allocation_watchdog", "status": status, "elapsed_ms": after.Milliseconds()})
		})
	}

	// Keepalive: re-send the Allocate AND a WhatsApp ping (0x0801) ~1 Hz. This matches the
	// working capture exactly — allocate+ping every second, NO STUN binding-requests at
	// all; the relay answers allocate-success + pong and bridges the peer's media.
	// Binding-requests instead flip the relay into ICE-consent mode and the bridge never
	// forms.
	runRelayKeepalive := func() {
		t := time.NewTicker(time.Second)
		defer t.Stop()
		var tickCount uint64
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
			}
			if ctx.Err() != nil {
				return
			}
			var tx [12]byte
			if _, err := rand.Read(tx[:]); err != nil {
				failMediaTask(cancelMedia, "relay consent transaction ID", err)
				return
			}
			ping := stun.BuildWhatsappPing(tx, log)
			allocateSent := false
			e.mu.Lock()
			currentCall := e.mediaCallLocked(callID, call)
			if currentCall != nil && currentCall.group {
				groupMode.Store(true)
			}
			e.mu.Unlock()
			if groupMode.Load() {
				e.mu.Lock()
				current := e.mediaCallLocked(callID, call)
				var update *groupCallUpdate
				if current != nil && current.groupUpdate != nil {
					cloned := cloneGroupCallUpdate(*current.groupUpdate)
					update = &cloned
				}
				e.mu.Unlock()
				if update != nil && update.Relay != nil {
					var relayTx [12]byte
					if _, err := rand.Read(relayTx[:]); err == nil {
						endpoint := getMediaRelayEndpoint(rd, inbound)
						var refreshErr error
						allocateSent, refreshErr = allocateState.ApplyWithSubscriptions(
							endpoint,
							update.Relay,
							update.TransactionID,
							streamSsrcs,
							appDataSelfSsrc,
							connectedRemoteParticipantPIDs(*update, audioReceivers.selfID),
							relayTx,
							func(packet []byte) error {
								_, sendErr := ch.Send(packet)
								return sendErr
							},
						)
						if refreshErr != nil {
							log.Warn().Err(refreshErr).Str("call_id", callID).Msg("failed to refresh group relay allocation")
						}
					}
				}
				activeParticipantIDs := audioReceivers.ActiveParticipantIDs()
				audioMixer.Retain(activeParticipantIDs)
				if shouldStartParticipantMixing(activeParticipantIDs) && !groupMixing.Load() {
					audioPlayoutMu.Lock()
					_, sink := callPlayerSink(call)
					if err := audioPlayout.Drain(sink); err != nil {
						log.Warn().Err(err).Msg("failed to drain direct audio before group playout")
					}
					groupMixing.Store(true)
					audioPlayoutMu.Unlock()
				}
			}
			if !allocateSent {
				if err := allocateState.SendCurrent(func(packet []byte) error {
					_, sendErr := ch.Send(packet)
					return sendErr
				}); err != nil {
					failMediaTask(cancelMedia, "relay allocation keepalive", err)
					return
				}
			}
			consent.remember(tx, time.Now())
			if _, err := ch.Send(ping[:]); err != nil {
				failMediaTask(cancelMedia, "relay consent keepalive", err)
				return
			}
			tickCount++
			e.c.diag.Emit("stun", map[string]any{
				"event": "keepalive", "tick": tickCount,
				"tx_id_hex": hex.EncodeToString(tx[:]), "ping_hex": hex.EncodeToString(ping[:]),
			})
		}
	}

	// Send loop: frame-paced from connect, NOT gated on the Player. WhatsApp starts media
	// on relay connection and the relay learns our SSRC from our FIRST RTP — it won't
	// bridge the peer's media until it sees our stream. So we send silence frames until the
	// Player has real audio (nextFrame() == nil means send silence).
	frameInterval := time.Duration(audioConfig.frameSamples) * time.Second / SampleRate
	runAudioSender := func() {
		defer closeEncoder()
		silence := make([]float32, FrameSamples)
		var packetizer opusPCMPacketizer
		var packetGeneration opusPlayerGeneration
		ticker := time.NewTicker(frameInterval)
		defer ticker.Stop()
		var txCount uint64
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
			}
			if ctx.Err() != nil {
				return
			}
			frame := silence
			player, _ := callPlayerSink(call)
			if audioConfig.codec == AudioCodecOpus {
				generation := opusGeneration(player)
				if packetGeneration != generation {
					packetizer.reset()
					packetGeneration = generation
				}
				frame = packetizer.next(audioConfig.frameSamples, func() []float32 {
					if player == nil {
						return nil
					}
					return player.nextFrame()
				})
				if currentPlayer, _ := callPlayerSink(call); opusGeneration(currentPlayer) != packetGeneration {
					packetizer.reset()
					packetGeneration = opusGeneration(currentPlayer)
					frame = make([]float32, audioConfig.frameSamples)
				}
			} else if player != nil {
				if f := player.nextFrame(); f != nil {
					frame = f
				}
			}
			if ctx.Err() != nil {
				return
			}
			payload, err := enc.Encode(frame)
			if err != nil {
				if audioConfig.codec == AudioCodecOpus {
					failMediaTask(cancelMedia, "Opus encoder", err)
					return
				}
				continue
			}
			packet, err := txPipe.ProtectAudio(payload)
			if err != nil {
				continue
			}
			if audioConfig.codec == AudioCodecOpus {
				if currentPlayer, _ := callPlayerSink(call); opusGeneration(currentPlayer) != packetGeneration {
					packetizer.reset()
					packetGeneration = opusGeneration(currentPlayer)
					continue // RTP sequence/time already advanced; no stale PCM submission
				}
			}
			e.c.diag.Emit("media_out", map[string]any{
				"frame": txCount, "frame_samples": len(frame), "pcm_rms": rmsFloat32(frame),
				"payload_len": len(payload), "packet_len": len(packet),
			})
			if _, err := sendMediaTaskPacket(ctx, cancelMedia, "audio RTP sender", ch.Send, packet); err != nil {
				return
			}
			if txCount++; txCount == 1 {
				log.Info().Int("bytes", len(packet)).Msg("first RTP sent to relay, outbound media flowing")
				e.c.diag.Emit("meta", map[string]any{"event": "first_rtp_sent", "call_id": callID, "bytes": len(packet)})
			}
		}
	}

	// Receive: DataChannel → classify. RTP → unprotect → decode → sink. A non-RTP STUN
	// binding request gets a binding-success reply (ICE consent freshness, RFC 7675);
	// without it the relay drops the binding and the peer's call fails.
	// Video receive uses participant-scoped WARP pipelines keyed on each video SSRC.
	//
	// NOT VALIDATED: no live video-RTP vector; assumes video shares the audio E2E keys and
	// WARP framing, and that the relay bridges the video SSRC.
	rekeyPeer := func(answeringPeer string) error {
		return audioReceivers.RekeyFallback(answeringPeer)
	}
	e.mu.Lock()
	currentPeer := peerLID
	if m := e.mediaCallLocked(callID, call); m != nil {
		m.rekeyPeer = rekeyPeer
		currentPeer = m.peerLID
	} else {
		e.mu.Unlock()
		return fmt.Errorf("meowcaller: call ended before media rekey registration")
	}
	e.mu.Unlock()
	defer func() {
		e.mu.Lock()
		if m := e.mediaCallLocked(callID, call); m != nil {
			m.rekeyPeer = nil
		}
		e.mu.Unlock()
	}()
	if currentPeer != "" && currentPeer != peerLID {
		if err := rekeyPeer(currentPeer); err != nil {
			return fmt.Errorf("rekey media to answering device: %w", err)
		}
		peerLID = currentPeer
	}
	videoReceiveStates := make(map[*participantAudioReceiver]*videoReceiveState)
	appDataReceivers := make(map[*participantAudioReceiver]*appDataReceiver)
	lastVideoPLI := make(map[uint32]time.Time)
	var rosterGeneration uint64
	hasRosterGeneration := false
	var videoWirePacket, videoWireFrame uint64

	// Video send: a second WARP pipeline on our video SSRC, registered on the call so
	// Call.SendVideoFrame can push encoded H.264 to the relay. Cleared when the loop exits.
	txVideoPipe, err := NewMediaPipeline(callKey, selfLID, peerLID, videoSelfSsrc, FrameSamples, WithLogger(log))
	if err != nil {
		return err
	}
	if err = audioReceivers.attachSendPipeline(txVideoPipe); err != nil {
		return fmt.Errorf("attach video RTP sender: %w", err)
	}
	videoRtcp, err := newMediaSrtcpSender(callKey, selfLID, videoSelfSsrc, true)
	if err != nil {
		return err
	}
	if err = audioReceivers.attachSRTCPSender(videoRtcp); err != nil {
		return fmt.Errorf("attach video SRTCP sender: %w", err)
	}
	vsender := &videoSender{
		pipe: txVideoPipe, stream: rtp.NewVideoRtpStream(videoSelfSsrc, defaultVideoRtpStepSamples),
		ch: ch, ssrc: videoSelfSsrc, callID: callID, keyframeRequired: true,
		log: log, diag: e.c.diag,
		mediaContext: ctx, cancelMedia: cancelMedia,
	}
	txAppDataPipe, err := NewMediaPipeline(callKey, selfLID, peerLID, appDataSelfSsrc, FrameSamples, WithLogger(log))
	if err != nil {
		return err
	}
	if err = audioReceivers.attachSendPipeline(txAppDataPipe); err != nil {
		return fmt.Errorf("attach app-data RTP sender: %w", err)
	}
	appSender := newAppDataSender(txAppDataPipe, appDataSelfSsrc, func(packet []byte) (int, error) {
		if header, ok := rtp.ParseRtpHeader(packet); ok {
			e.c.diag.Emit("app_data", map[string]any{
				"event": "out", "ssrc": header.Ssrc, "seq": header.SequenceNumber,
				"ts": header.Timestamp, "pt": header.PayloadType, "bytes": len(packet),
			})
		}
		return sendMediaTaskPacket(ctx, cancelMedia, "app-data RTP sender", ch.Send, packet)
	})
	e.mu.Lock()
	if m := e.mediaCallLocked(callID, call); m != nil {
		vsender.active = m.localVideo
		vsender.sendGated = m.videoGate
		m.videoTx = vsender
		m.appDataTx = appSender
	} else {
		e.mu.Unlock()
		appSender.close()
		return fmt.Errorf("meowcaller: call ended before media sender registration")
	}
	e.mu.Unlock()
	defer func() {
		// Interrupt blocked sender I/O before waiting for either sender lock.
		cancelMedia(nil)
		appSender.close()
		vsender.mu.Lock()
		vsender.ch = nil
		vsender.mu.Unlock()
		e.mu.Lock()
		if m := e.mediaCallLocked(callID, call); m != nil {
			m.videoTx = nil
			m.appDataTx = nil
		}
		e.mu.Unlock()
	}()

	var allocationConfirmed, allocationRejected, consentConfirmed bool
	var audioReception rtp.RtcpReceptionStatsSet
	var videoReception rtp.RtcpReceptionStatsSet
	if groupMode.Load() {
		e.mu.Lock()
		current := e.mediaCallLocked(callID, call)
		var update *groupCallUpdate
		if current != nil && current.groupUpdate != nil {
			cloned := cloneGroupCallUpdate(*current.groupUpdate)
			update = &cloned
		}
		e.mu.Unlock()
		if update == nil || update.Relay == nil {
			return fmt.Errorf("meowcaller: group media started without an authoritative relay snapshot")
		}
		var relayTx [12]byte
		if _, err := rand.Read(relayTx[:]); err != nil {
			return fmt.Errorf("generate group relay transaction ID: %w", err)
		}
		_, err = allocateState.ApplyWithSubscriptions(
			getMediaRelayEndpoint(rd, inbound),
			update.Relay,
			update.TransactionID,
			streamSsrcs,
			appDataSelfSsrc,
			connectedRemoteParticipantPIDs(*update, audioReceivers.selfID),
			relayTx,
			func(packet []byte) error {
				_, sendErr := ch.Send(packet)
				return sendErr
			},
		)
		if err != nil {
			return fmt.Errorf("send initial group relay allocation: %w", err)
		}
		audioReception.Retain(audioReceivers.ActiveAudioSSRCs())
		videoReception.Retain(audioReceivers.ActiveVideoSSRCs())
		activeParticipantIDs := audioReceivers.ActiveParticipantIDs()
		audioMixer.Retain(activeParticipantIDs)
		groupMixing.Store(shouldStartParticipantMixing(activeParticipantIDs))
	}

	// Do not start background media I/O until all fallible startup steps,
	// including the authoritative group allocation and sender registration,
	// have completed. A failed startup must not leak partially started tasks.
	if err := relayContextError(ctx); err != nil {
		return err
	}
	// Join the internal watchdog and report sender before releasing media
	// resources. Do not join arbitrary AudioSource/AudioSink callbacks: the
	// public interfaces allow blocking implementations.
	var internalTasks sync.WaitGroup
	defer func() {
		stopRelay() // release protocol writers before waiting for their exit
		internalTasks.Wait()
	}()
	startInternalTask := func(run func()) {
		internalTasks.Add(1)
		go func() {
			defer internalTasks.Done()
			run()
		}()
	}
	go runMixedPlayout()
	startInternalTask(runAllocationWatch)
	go runRelayKeepalive()
	encoderOwnedBySender = true
	go runAudioSender()

	// WhatsApp associates the RTP streams with an SRTCP session. Periodic compound
	// SR+SDES packets are required for the caller's video to start flowing to the
	// answerer, and give the peer a target for PLI/FIR recovery feedback.
	startInternalTask(func() {
		ticker := time.NewTicker(1500 * time.Millisecond)
		defer ticker.Stop()
		var sent uint64
		for {
			select {
			case <-ctx.Done():
				return
			case now := <-ticker.C:
				if ctx.Err() != nil {
					return
				}
				nowMs := uint64(now.UnixMilli())
				e.mu.Lock()
				current := e.mediaCallLocked(callID, call)
				if current != nil && current.group {
					groupMode.Store(true)
				}
				e.mu.Unlock()
				if groupMode.Load() {
					audioReception.Retain(audioReceivers.ActiveAudioSSRCs())
					videoReception.Retain(audioReceivers.ActiveVideoSSRCs())
				}
				audioReports := audioReception.Reports(nowMs)
				_, err := sendMediaSrtcpReceptionReports(
					audioRtcp,
					txPipe.SenderStats(),
					nowMs,
					audioReports,
					groupMode.Load() && audioReceivers.HasCommittedGroupUpdate(),
					func(packet []byte) error {
						_, sendErr := ch.Send(packet)
						return sendErr
					},
				)
				if err != nil {
					failMediaTask(cancelMedia, "audio SRTCP sender", err)
					return
				}
				videoStats := txVideoPipe.SenderStats()
				if videoStats.PacketsSent > 0 {
					videoReports := videoReception.Reports(nowMs)
					_, err = sendMediaSrtcpReceptionReports(
						videoRtcp,
						videoStats,
						nowMs,
						videoReports,
						groupMode.Load() && audioReceivers.HasCommittedGroupUpdate(),
						func(packet []byte) error {
							_, sendErr := ch.Send(packet)
							return sendErr
						},
					)
					if err != nil {
						failMediaTask(cancelMedia, "video SRTCP sender", err)
						return
					}
				}
				sent++
				if sent == 1 {
					log.Info().Msg("started periodic SRTCP sender reports")
				}
				e.c.diag.Emit("rtcp", map[string]any{
					"event": "sender_reports", "tick": sent,
					"audio_packets": txPipe.SenderStats().PacketsSent,
					"video_packets": videoStats.PacketsSent,
				})
			}
		}
	})

	buf := make([]byte, 1500)
	var rtpIn, rtpSeen, unprotectFail, rtpInspect, vidIn, appDataIn, appDataUnprotectFail, videoUnprotectFail, videoFrameIn, videoSinkMissing, rtcpIn, rtcpAuthFail, groupForwardingInvalid uint64
	for {
		if ctx.Err() != nil {
			return context.Cause(ctx)
		}
		var n int
		var err error
		if len(setup.firstPackets) != 0 {
			n = copy(buf, setup.firstPackets[0])
			setup.firstPackets[0] = nil
			setup.firstPackets = setup.firstPackets[1:]
		} else {
			n, err = ch.Recv(buf)
		}
		if err != nil {
			if ctx.Err() != nil {
				return context.Cause(ctx)
			}
			return fmt.Errorf("relay recv: %w", err)
		}
		if ctx.Err() != nil {
			return context.Cause(ctx)
		}
		currentRosterGeneration, activeReceivers := audioReceivers.ActiveReceiverSnapshot()
		if !hasRosterGeneration || currentRosterGeneration != rosterGeneration {
			for receiver, state := range videoReceiveStates {
				if _, active := activeReceivers[receiver]; active {
					continue
				}
				state.assembler = rtp.H264AccessUnitAssembler{}
				delete(lastVideoPLI, receiver.videoSSRC)
				delete(videoReceiveStates, receiver)
			}
			for receiver := range appDataReceivers {
				if _, active := activeReceivers[receiver]; !active {
					delete(appDataReceivers, receiver)
				}
			}
			rosterGeneration = currentRosterGeneration
			hasRosterGeneration = true
		}
		allocationWatch.received.Store(true)
		rawPkt := buf[:n]
		pkt, groupWrapped, groupValid := relay.UnwrapGroupForwardingPacket(rawPkt)
		if !groupValid {
			if groupForwardingInvalid++; groupForwardingInvalid == 1 {
				log.Warn().Int("packet_bytes", n).Msg("invalid group-forwarding relay packet")
			}
			continue
		}
		packetKind := relay.ClassifyRelayPacket(pkt)
		isRTP := packetKind == relay.RelayPacketRtp
		e.c.diag.Emit("relay", map[string]any{
			"event": "packet_in", "bytes": n, "is_rtp": isRTP,
			"group_wrapped": groupWrapped, "group_header_bytes": n - len(pkt),
		})
		switch packetKind {
		case relay.RelayPacketRtcp:
			senderSsrc, ok := rtp.ParseRtcpSenderSsrc(pkt)
			if !ok {
				continue
			}
			plain, index, ok := audioReceivers.UnprotectSRTCP(senderSsrc, pkt)
			if !ok {
				if rtcpAuthFail++; rtcpAuthFail == 1 {
					log.Warn().Uint32("ssrc", senderSsrc).Msg("peer SRTCP failed authentication")
				}
				e.c.diag.Emit("rtcp", map[string]any{
					"event": "auth_failed", "ssrc": senderSsrc, "bytes": n,
				})
				continue
			}
			if rtcpIn++; rtcpIn == 1 {
				log.Info().Uint32("ssrc", senderSsrc).Uint32("index", index).Msg("first authenticated peer SRTCP received")
			}
			keyframe := rtp.RtcpRequestsKeyframe(plain, videoSelfSsrc)
			if keyframe {
				vsender.requestKeyframe()
				if call != nil {
					call.requestVideoKeyframe()
				}
				log.Debug().Uint32("video_ssrc", videoSelfSsrc).Msg("peer requested a video keyframe")
			}
			if reportSsrc, ntpSeconds, ntpFraction, ok := rtp.ParseSenderReportTiming(plain); ok {
				nowMs := uint64(time.Now().UnixMilli())
				audioReception.ObserveSenderReport(reportSsrc, ntpSeconds, ntpFraction, nowMs)
				videoReception.ObserveSenderReport(reportSsrc, ntpSeconds, ntpFraction, nowMs)
			}
			e.c.diag.Emit("rtcp", map[string]any{
				"event": "in", "ssrc": senderSsrc, "index": index,
				"plain_bytes": len(plain), "requests_keyframe": keyframe,
			})
			continue
		case relay.RelayPacketStun:
			mt, isStun := stun.StunMessageType(pkt)
			if !isStun {
				continue
			}
			switch mt {
			case stun.MsgAllocateSuccess, stun.MsgAllocateError:
				if allocateState.matchesResponse(pkt) {
					// An acknowledgement is not evidence of peer media readiness.
					e.c.diag.Emit("stun", map[string]any{"event": "allocation_response", "success": mt == stun.MsgAllocateSuccess})
					if mt == stun.MsgAllocateSuccess {
						allocationWatch.acknowledged.Store(true)
					} else {
						allocationWatch.rejected.Store(true)
					}
					if mt == stun.MsgAllocateSuccess && !allocationConfirmed {
						log.Info().Msg("relay allocation acknowledged")
						allocationConfirmed = true
					} else if mt == stun.MsgAllocateError {
						code, hasCode := stun.ParseStunErrorCode(pkt, log)
						if !allocationRejected {
							event := log.Warn()
							if hasCode {
								event = event.Uint16("stun_error_code", code)
							}
							event.Msg("relay rejected allocation")
							allocationRejected = true
						}
						// Preflight can hand off on an early binding request or media.
						// Do not leave a direct call connecting if the matching
						// allocation is explicitly rejected afterwards.
						if err := initialRelayRejection(groupMode.Load(), allocationConfirmed, code, hasCode); err != nil {
							return err
						}
					}
				}
			case stun.MsgWhatsappPong:
				tx, ok := stun.StunTransactionID(pkt)
				if ok && consent.consume([12]byte(tx), time.Now()) {
					e.c.diag.Emit("stun", map[string]any{"event": "consent_pong_matched"})
					if !consentConfirmed {
						log.Info().Msg("relay consent acknowledged")
						consentConfirmed = true
					}
				}
			case stun.MsgBindingRequest:
				resp, answered, err := allocateState.SendBindingSuccess(pkt, func(packet []byte) error {
					_, sendErr := ch.Send(packet)
					return sendErr
				})
				if err != nil {
					return fmt.Errorf("relay send binding-success: %w", err)
				}
				if answered {
					e.c.diag.Emit("stun", map[string]any{
						"event": "binding_request_answered", "response_bytes": len(resp),
					})
				}
			}
			continue
		case relay.RelayPacketOther:
			continue
		}
		if rtpSeen++; rtpSeen == 1 {
			log.Info().Int("bytes", n).Msg("first RTP-classified packet from relay, relay is bridging the peer's media")
		}
		vh, vok := rtp.ParseRtpHeader(pkt)
		if vok {
			if rtpInspect < 20 || vh.PayloadType == rtp.RtpPayloadTypeH264 || vh.PayloadType == rtp.RtpPayloadTypeAppData {
				log.Debug().
					Uint8("payload_type", vh.PayloadType).
					Uint32("ssrc", vh.Ssrc).
					Uint16("seq", vh.SequenceNumber).
					Uint32("timestamp", vh.Timestamp).
					Bool("marker", vh.Marker).
					Int("bytes", n).
					Msg("relay RTP packet summary")
			}
			rtpInspect++
		}
		if !vok {
			continue
		}
		kind := classifyMediaPayload(vh)
		if kind == mediaPayloadAppData {
			media, ok := audioReceivers.UnprotectAppData(pkt)
			if !ok {
				if appDataUnprotectFail++; appDataUnprotectFail == 1 {
					log.Warn().Uint32("ssrc", vh.Ssrc).Uint16("seq", vh.SequenceNumber).Msg("app-data RTP arrived but failed to unprotect")
				}
				e.c.diag.Emit("app_data", map[string]any{"event": "unprotect_failed", "ssrc": vh.Ssrc, "seq": vh.SequenceNumber})
				continue
			}
			receiver := appDataReceivers[media.receiver]
			if receiver == nil {
				receiver = &appDataReceiver{}
				appDataReceivers[media.receiver] = receiver
			}
			handled, err := handleGroupAppDataReaction(call, receiver, media)
			if err != nil {
				log.Warn().Err(err).Uint32("ssrc", media.Header.Ssrc).Uint16("seq", media.Header.SequenceNumber).Msg("invalid RTC app-data payload")
				e.c.diag.Emit("app_data", map[string]any{
					"event": "decode_failed", "ssrc": media.Header.Ssrc, "seq": media.Header.SequenceNumber,
					"error": err.Error(),
				})
				continue
			}
			e.c.diag.Emit("app_data", map[string]any{
				"event": "in", "ssrc": media.Header.Ssrc, "seq": media.Header.SequenceNumber,
				"ts": media.Header.Timestamp, "handled": handled,
				"participant_id": media.ParticipantID,
			})
			if handled {
				if appDataIn++; appDataIn == 1 {
					log.Info().Uint32("ssrc", media.Header.Ssrc).Msg("first RTC call reaction received")
				}
			}
			continue
		}
		// Demux H.264 (PT 97) to video and emit Annex-B access units on the marker bit.
		// Source of truth: https://github.com/JotaDev66/WaCalls/blob/2d6a1f666426049a89ef9541414e771acdcf8a16/internal/voip/call/callmanager_video.go#L86-L126
		if kind == mediaPayloadVideo {
			media, vunok := audioReceivers.UnprotectVideo(pkt)
			if !vunok {
				if videoUnprotectFail++; videoUnprotectFail == 1 {
					log.Warn().
						Uint32("ssrc", vh.Ssrc).
						Uint16("seq", vh.SequenceNumber).
						Msg("video RTP arrived but failed to unprotect")
				}
				e.c.diag.Emit("video", map[string]any{"event": "unprotect_failed", "ssrc": vh.Ssrc, "seq": vh.SequenceNumber})
				continue
			}
			vh = media.Header
			role := GroupVideoRole{}
			if call != nil && media.Epoch.Valid {
				role = call.videoRole(media.Epoch, media.UserJID, media.DeviceJID, media.PID, media.HasPID)
			}
			videoState := videoReceiveStates[media.receiver]
			if videoState == nil || (media.Epoch.Valid && (videoState.participantEpoch != media.Epoch || videoState.role != role)) {
				// Never complete an access unit assembled across different
				// authenticated roster/key epochs. Resume at a fresh keyframe.
				videoState = newVideoReceiveState()
				videoState.participantEpoch = media.Epoch
				videoState.role = role
				videoReceiveStates[media.receiver] = videoState
				if media.Epoch.Valid {
					videoState.requestKeyframe.Store(true)
				}
			}
			videoReception.Observe(vh.Ssrc, vh.SequenceNumber, vh.Timestamp, uint64(time.Now().UnixMilli()), 90000)
			if vh.VideoExtension != nil {
				orientation := vh.VideoExtension.DisplayOrientation()
				if orientation != videoState.orientation {
					videoState.orientation = orientation
					if sink, ok := callVideoSink(call).(VideoOrientationSink); ok {
						sink.SetOrientation(orientation)
					}
					log.Debug().
						Int("orientation", orientation).
						Uint8("media_frame_info", vh.VideoExtension.MediaFrameInfo).
						Msg("updated peer video orientation from RTP")
					e.c.diag.Emit("video", map[string]any{
						"event": "orientation", "orientation": orientation,
						"media_frame_info": vh.VideoExtension.MediaFrameInfo,
					})
				}
			}
			if videoWirePacket < videoWirePacketLimit {
				headerLen, _ := rtp.RtpHeaderByteLength(pkt)
				_, extension, _ := rtp.RtpExtensionProfileAndData(pkt)
				e.c.diag.Emit("video_wire", map[string]any{
					"event": "packet", "direction": "in", "call_id": callID,
					"packet": videoWirePacket, "frame": videoWireFrame,
					"ssrc": vh.Ssrc, "seq": vh.SequenceNumber, "rtp_ts": vh.Timestamp,
					"marker": vh.Marker, "header_hex": hex.EncodeToString(pkt[:headerLen]),
					"extension_hex": hex.EncodeToString(extension),
					"payload_bytes": len(media.Payload), "protected_bytes": len(pkt),
					"participant_id": media.ParticipantID,
				})
			}
			videoWirePacket++
			frame, complete, _ := videoState.assembler.Push(
				vh.SequenceNumber,
				vh.Timestamp,
				vh.Marker,
				media.Payload,
			)
			if send, explicit := videoState.takeVideoPLI(lastVideoPLI, vh.Ssrc, time.Now()); send {
				packet, feedbackErr := videoRtcp.pictureLossIndication(vh.Ssrc)
				if feedbackErr == nil {
					_, feedbackErr = ch.Send(packet)
				}
				if feedbackErr != nil {
					if explicit {
						videoState.requestKeyframe.Store(true)
					}
					log.Warn().Err(feedbackErr).Uint32("ssrc", vh.Ssrc).Msg("failed to request video keyframe after RTP loss")
				}
			}
			if complete {
				if videoWireFrame < videoWireFrameLimit {
					e.c.diag.Emit("video_wire", map[string]any{
						"event": "access_unit", "direction": "in", "call_id": callID,
						"frame": videoWireFrame, "ssrc": vh.Ssrc, "rtp_ts": vh.Timestamp,
						"idr": rtp.AUHasIDR(frame), "bytes": len(frame),
					})
				}
				videoWireFrame++
				e.c.diag.Emit("video", map[string]any{"event": "frame", "ssrc": vh.Ssrc, "bytes": len(frame)})
				deliveredParticipantFrame := false
				if call != nil {
					deliveredParticipantFrame = call.dispatchParticipantVideoFrame(ParticipantVideoFrame{
						Epoch:         media.Epoch,
						ParticipantID: media.ParticipantID,
						Sender:        media.UserJID,
						Device:        media.DeviceJID,
						PID:           media.PID,
						HasPID:        media.HasPID,
						SSRC:          vh.Ssrc,
						Timestamp:     vh.Timestamp, RequestKeyframe: func() { videoState.requestKeyframe.Store(true) },
						Orientation: videoState.orientation,
						ScreenShare: role.ScreenShare, RoleRevision: role.Revision,
						AccessUnit: frame,
					})
				}
				delivered := deliveredParticipantFrame
				if sink := callVideoSink(call); sink != nil {
					if err := sink.WriteVideo(frame); err != nil {
						log.Warn().Err(err).Uint32("ssrc", vh.Ssrc).Int("bytes", len(frame)).Msg("failed to write WhatsApp video frame to sink")
					} else {
						if videoFrameIn == 0 {
							log.Info().Uint32("ssrc", vh.Ssrc).Int("bytes", len(frame)).Msg("first WhatsApp video frame written to sink")
						}
						videoFrameIn++
						delivered = true
					}
				} else if !deliveredParticipantFrame {
					if videoSinkMissing == 0 {
						log.Warn().Uint32("ssrc", vh.Ssrc).Int("bytes", len(frame)).Msg("WhatsApp video frame arrived with no sink attached")
					}
					videoSinkMissing++
				}
				videoState.noteVideoDelivery(delivered)
			}
			if vidIn++; vidIn == 1 {
				log.Info().Uint32("ssrc", vh.Ssrc).Msg("first video RTP demuxed from relay (NOT VALIDATED)")
				e.c.diag.Emit("meta", map[string]any{"event": "first_video_rtp_in", "call_id": callID, "ssrc": vh.Ssrc})
			}
			continue
		}
		if kind != mediaPayloadAudio {
			log.Debug().Uint8("payload_type", vh.PayloadType).Uint32("ssrc", vh.Ssrc).Msg("dropping unknown RTP payload")
			e.c.diag.Emit("rtp", map[string]any{
				"event": "unknown_payload", "pt": vh.PayloadType, "ssrc": vh.Ssrc,
				"seq": vh.SequenceNumber, "ts": vh.Timestamp,
			})
			continue
		}
		audio, ok := audioReceivers.DecodeAudio(pkt)
		if !ok {
			if unprotectFail++; unprotectFail == 1 {
				log.Warn().Uint32("ssrc", vh.Ssrc).Int("bytes", n).Msg("audio RTP failed participant authentication or decoding")
			}
			e.c.diag.Emit("srtp", map[string]any{"event": "audio_rejected", "ssrc": vh.Ssrc, "bytes": n})
			continue
		}
		audioReception.Observe(audio.SSRC, vh.SequenceNumber, audio.Timestamp, uint64(time.Now().UnixMilli()), audioConfig.clockRate())
		e.c.diag.Emit("rtp", map[string]any{
			"event": "in", "ssrc": audio.SSRC, "seq": vh.SequenceNumber,
			"ts": audio.Timestamp, "pt": vh.PayloadType, "marker": vh.Marker,
			"participant_id": audio.ParticipantID, "pid": audio.PID, "has_pid": audio.HasPID,
		})
		e.c.diag.Emit("srtp", map[string]any{
			"event": "frame_authenticated", "ssrc": audio.SSRC, "seq": vh.SequenceNumber,
			"participant_id": audio.ParticipantID, "pid": audio.PID, "has_pid": audio.HasPID,
		})
		e.c.diag.Emit("media_in", map[string]any{
			"seq": vh.SequenceNumber, "samples": len(audio.PCM),
			"pcm_rms": rmsFloat32(audio.PCM), "participant_id": audio.ParticipantID,
		})
		// Preserve authenticated participant identity and audio before aggregate playout.
		// With no listener, the existing mixer and direct sink remain unchanged.
		if call != nil {
			call.dispatchParticipantAudioFrame(audio, audioConfig.clockRate())
		}
		mixedMode := groupMixing.Load()
		playoutLocked := false
		if !mixedMode {
			audioPlayoutMu.Lock()
			playoutLocked = true
			mixedMode = groupMixing.Load()
		}
		if mixedMode {
			audioMixer.Add(audio.ParticipantID, audio.PCM)
		} else {
			_, sink := callPlayerSink(call)
			playoutStarted, playoutErr := audioPlayout.Push(audio.Timestamp, audio.PCM, sink)
			if playoutErr != nil {
				log.Warn().Err(playoutErr).Msg("failed to write timestamp-aligned WhatsApp audio")
			}
			if playoutStarted {
				log.Info().Int("prefill_ms", audioPlayoutPrefillSamples*1000/SampleRate).Msg("started timestamp-aligned inbound audio playout")
			}
		}
		if playoutLocked {
			audioPlayoutMu.Unlock()
		}
		if rtpIn++; rtpIn == 1 {
			log.Info().Msg("first RTP decoded from relay, inbound audio flowing")
			e.c.diag.Emit("meta", map[string]any{"event": "first_rtp_in", "call_id": callID})
			if call != nil {
				call.setPhase(CallPhaseActive)
				if fn := call.onReadyFn(); fn != nil {
					fn()
				}
			}
		}
	}
}

func prepareWasmRelayStreamSSRCs(
	streamSSRCs [9]uint32,
	appDataSSRC uint32,
	random io.Reader,
) ([9]uint32, error) {
	// Source of truth: https://github.com/purpshell/meowcaller/blob/0911f20e97b858506a55ee6aa4f6a1ad73f19798/datasheets/group-video-reactions.md#L51-L65
	if random == nil {
		return [9]uint32{}, fmt.Errorf("meowcaller: relay stream SSRC random source is nil")
	}
	used := make(map[uint32]struct{}, len(streamSSRCs)+1)
	for _, ssrc := range streamSSRCs[:6] {
		if ssrc != 0 {
			used[ssrc] = struct{}{}
		}
	}
	if appDataSSRC != 0 {
		used[appDataSSRC] = struct{}{}
	}
	for i := 6; i < len(streamSSRCs); i++ {
		var selected bool
		for range 64 {
			var encoded [4]byte
			if _, err := io.ReadFull(random, encoded[:]); err != nil {
				return [9]uint32{}, fmt.Errorf("meowcaller: generate auxiliary video SSRC: %w", err)
			}
			candidate := binary.LittleEndian.Uint32(encoded[:])
			if candidate == 0 {
				continue
			}
			if _, exists := used[candidate]; exists {
				continue
			}
			streamSSRCs[i] = candidate
			used[candidate] = struct{}{}
			selected = true
			break
		}
		if !selected {
			return [9]uint32{}, fmt.Errorf("meowcaller: generate unique auxiliary video SSRC")
		}
	}
	return streamSSRCs, nil
}

func sendMediaSrtcpReceptionReports(
	sender *mediaSrtcpSender,
	stats rtp.RtcpSenderStats,
	nowMs uint64,
	reports []*rtp.RtcpReceptionReport,
	groupMedia bool,
	send func([]byte) error,
) (int, error) {
	// Source of truth: https://github.com/purpshell/meowcaller/blob/bab582d4e799292478ccba2f8a86f2164d4737c3/datasheets/group-media-rtcp-feedback.md#L148-L156
	if sender == nil || send == nil {
		return 0, fmt.Errorf("meowcaller: SRTCP sender or send callback is nil")
	}
	if !groupMedia || len(reports) == 0 {
		var report *rtp.RtcpReceptionReport
		if len(reports) > 0 {
			report = reports[0]
		}
		packet, err := sender.senderReport(stats, nowMs, report)
		if err != nil {
			return 0, err
		}
		return 1, send(packet)
	}
	sent := 0
	for _, report := range reports {
		packet, err := sender.groupSenderReport(stats, nowMs, report)
		if err != nil {
			return sent, err
		}
		if err = send(packet); err != nil {
			return sent, err
		}
		sent++
	}
	return sent, nil
}

func handleGroupAppDataReaction(
	call *Call,
	receiver *appDataReceiver,
	media unprotectedParticipantMedia,
) (bool, error) {
	// Source of truth: https://github.com/purpshell/meowcaller/blob/36d54857c74e45ccb08f6444a32d2afa13f20be9/datasheets/group-video-reactions.md#L10-L19
	reaction, ok, err := receiver.receive(media.Payload)
	if err != nil || !ok || call == nil {
		return false, err
	}
	if fn := call.onReactionFn(); fn != nil {
		fn(CallReaction{
			Emoji: reaction.emoji, ParticipantID: media.ParticipantID,
			Sender: media.UserJID, Device: media.DeviceJID,
			PID: media.PID, HasPID: media.HasPID,
			Removed: reaction.emoji == "",
		})
	}
	return true, nil
}

// callPlayerSink returns a Call's current Player and sink, tolerating a nil Call (an
// outbound call may never have had one attached).
func callPlayerSink(call *Call) (*Player, AudioSink) {
	if call == nil {
		return nil, nil
	}
	return call.playerAndSink()
}

// callVideoSink returns a Call's inbound-video sink, tolerating a nil Call.
func callVideoSink(call *Call) VideoSink {
	if call == nil {
		return nil
	}
	return call.videoSinkRef()
}

const defaultVideoRtpStepSamples = 90000 / 30

const (
	videoWirePacketLimit = 120
	videoWireFrameLimit  = 30
)

func videoRtpDurationSamples(duration time.Duration) uint32 {
	if duration <= 0 {
		return defaultVideoRtpStepSamples
	}
	samples := uint32((duration.Nanoseconds()*90000 + int64(time.Second)/2) / int64(time.Second))
	if samples == 0 {
		return defaultVideoRtpStepSamples
	}
	return samples
}

// videoSender packetizes encoded H.264 access units (Annex-B) into PT-97 RTP, E2E-SRTP
// protects them with the video pipeline, and sends them to the relay. The send path is
// fed encoded H.264 (e.g. from the VideoBridge / WebCodecs), not raw frames.
//
// NOT VALIDATED: the video send media path is unproven.
type videoSender struct {
	mediaContext     context.Context
	cancelMedia      context.CancelCauseFunc
	mu               sync.Mutex
	pipe             *MediaPipeline
	stream           *rtp.VideoRtpStream
	ch               *relay.RelayMediaChannel
	ssrc             uint32
	callID           string
	frame            uint64
	logged           bool
	active           bool
	sendGated        bool
	keyframeRequired bool
	log              zerolog.Logger
	diag             *diag.Recorder
}

type mediaSrtcpSender struct {
	retired bool
	mu      sync.Mutex
	keys    srtp.E2eSrtpKeys
	ssrc    uint32
	cname   [rtp.WhatsappRtcpCnameLen]byte
	profile bool
	index   uint32
}

type mediaSrtcpReceiver struct {
	retired bool
	mu      sync.Mutex
	keys    srtp.E2eSrtpKeys
}

func newMediaSrtcpReceiver(callKey []byte, peerLID string) (*mediaSrtcpReceiver, error) {
	keys, err := srtp.DeriveE2eSrtcpKeys(callKey, rtp.FormatE2ESrtpParticipantID(peerLID))
	if err != nil {
		return nil, err
	}
	return &mediaSrtcpReceiver{keys: keys}, nil
}

func (r *mediaSrtcpReceiver) rekey(callKey []byte, peerLID string) error {
	keys, err := srtp.DeriveE2eSrtcpKeys(callKey, rtp.FormatE2ESrtpParticipantID(peerLID))
	if err != nil {
		return err
	}
	return r.installKeys(keys)
}

func (r *mediaSrtcpReceiver) retire() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.retired = true
	r.keys = srtp.E2eSrtpKeys{}
}

func (r *mediaSrtcpReceiver) installKeys(keys srtp.E2eSrtpKeys) error {
	// Source of truth: https://github.com/oxidezap/whatsapp-rust/blob/41095d4e6ba4610e054e9ede3af1d5e88a83faee/wacore/src/voip/e2e_srtp.rs#L34-L70
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.retired {
		return errMediaKeysRetired
	}
	r.keys = keys
	return nil
}

func (r *mediaSrtcpReceiver) unprotect(senderSSRC uint32, packet []byte) ([]byte, uint32, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.retired {
		return nil, 0, false
	}
	return srtp.UnprotectSrtcp(&r.keys, senderSSRC, packet)
}

func newMediaSrtcpSender(callKey []byte, selfLID string, ssrc uint32, profile bool) (*mediaSrtcpSender, error) {
	keys, err := srtp.DeriveE2eSrtcpKeys(callKey, rtp.FormatE2ESrtpParticipantID(selfLID))
	if err != nil {
		return nil, err
	}
	var entropy [12]byte
	_, _ = rand.Read(entropy[:])
	return &mediaSrtcpSender{
		keys: keys, ssrc: ssrc, cname: rtp.BuildWhatsappRtcpCname(entropy),
		profile: profile, index: 1,
	}, nil
}

func (s *mediaSrtcpSender) retire() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.retired = true
	s.keys = srtp.E2eSrtpKeys{}
}

func (s *mediaSrtcpSender) installKeys(keys srtp.E2eSrtpKeys) error {
	// Source of truth: https://github.com/oxidezap/whatsapp-rust/blob/41095d4e6ba4610e054e9ede3af1d5e88a83faee/wacore/src/voip/e2e_srtp.rs#L34-L70
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.retired {
		return errMediaKeysRetired
	}
	s.keys = keys
	return nil
}

func (s *mediaSrtcpSender) senderReport(stats rtp.RtcpSenderStats, nowMs uint64, report *rtp.RtcpReceptionReport) ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.retired {
		return nil, errMediaKeysRetired
	}
	plain := rtp.BuildSenderReportWithSdesAndReception(s.ssrc, &stats, nowMs, &s.cname, report, s.profile)
	packet, err := srtp.ProtectSrtcp(&s.keys, s.ssrc, s.index, plain)
	if err == nil {
		s.index++
	}
	return packet, err
}

func (s *mediaSrtcpSender) groupSenderReport(stats rtp.RtcpSenderStats, nowMs uint64, report *rtp.RtcpReceptionReport) ([]byte, error) {
	// Source of truth: https://github.com/purpshell/meowcaller/blob/6e202a6d6ec5a9384bae6ccbe621966edeee6592/datasheets/group-media-rtcp-feedback.md#L53-L75
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.retired {
		return nil, errMediaKeysRetired
	}
	plain := rtp.BuildGroupSenderReport(s.ssrc, &stats, nowMs, report, rtp.RTCPGroupReportExtension{})
	packet, err := srtp.ProtectSrtcp(&s.keys, s.ssrc, s.index, plain)
	if err == nil {
		s.index++
	}
	return packet, err
}

func shouldSendVideoPLI(lastSent map[uint32]time.Time, mediaSSRC uint32, now time.Time) bool {
	// Source of truth: https://github.com/oxidezap/whatsapp-rust/blob/41095d4e6ba4610e054e9ede3af1d5e88a83faee/wacore/src/voip/e2e_srtp.rs#L34-L70
	if lastSent == nil {
		return false
	}
	if previous, ok := lastSent[mediaSSRC]; ok && now.Sub(previous) < videoPLIInterval {
		return false
	}
	lastSent[mediaSSRC] = now
	return true
}

func (s *mediaSrtcpSender) pictureLossIndication(mediaSSRC uint32) ([]byte, error) {
	// Source of truth: https://github.com/oxidezap/whatsapp-rust/blob/41095d4e6ba4610e054e9ede3af1d5e88a83faee/wacore/src/voip/e2e_srtp.rs#L34-L70
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.retired {
		return nil, errMediaKeysRetired
	}
	plain := rtp.BuildPictureLossIndication(s.ssrc, mediaSSRC, s.profile)
	packet, err := srtp.ProtectSrtcp(&s.keys, s.ssrc, s.index, plain[:])
	if err == nil {
		s.index++
	}
	return packet, err
}

func (vs *videoSender) protectAccessUnit(au []byte, duration time.Duration) [][]byte {
	vs.mu.Lock()
	defer vs.mu.Unlock()
	return vs.protectAccessUnitLocked(au, duration)
}

func (vs *videoSender) protectAccessUnitLocked(au []byte, duration time.Duration) [][]byte {
	if !vs.active || vs.sendGated {
		return nil
	}
	idr := rtp.AUHasIDR(au)
	if vs.keyframeRequired && !idr {
		return nil
	}
	nalus := rtp.SplitAnnexB(au)
	var packedAccessUnit []byte
	for _, n := range nalus {
		if len(n) == 0 || n[0]&0x1f == 9 {
			continue
		}
		if len(packedAccessUnit) > 0 {
			packedAccessUnit = append(packedAccessUnit, 0, 0, 0, 1)
		}
		packedAccessUnit = append(packedAccessUnit, n...)
	}
	if len(packedAccessUnit) == 0 {
		return nil
	}
	// WhatsApp fragments the complete Annex-B access unit as one RTP NAL unit.
	payloads := rtp.PackageH264NALU(packedAccessUnit)
	captureWire := vs.frame < videoWireFrameLimit
	if captureWire {
		vs.diag.Emit("video_wire", map[string]any{
			"event": "access_unit", "direction": "out", "call_id": vs.callID,
			"frame": vs.frame, "ssrc": vs.ssrc, "idr": idr, "bytes": len(au),
			"duration_ms": duration.Milliseconds(),
		})
	}
	vs.stream.SetTimestampStride(videoRtpDurationSamples(duration))
	mediaFrameInfo := rtp.VideoMediaFrameInfoDelta
	if idr {
		mediaFrameInfo = rtp.VideoMediaFrameInfoIDR
	}
	packets := make([][]byte, 0, len(payloads))
	for i, payload := range payloads {
		header := vs.stream.NextPacket(i == len(payloads)-1, mediaFrameInfo)
		packet, err := vs.pipe.ProtectRTP(&header, payload)
		if err != nil {
			// Never submit an incomplete access unit or clear the recovery gate.
			vs.keyframeRequired = true
			return nil
		}
		packets = append(packets, packet)
		if captureWire {
			headerBytes := rtp.EncodeRtpHeader(&header)
			_, extension, _ := rtp.RtpExtensionProfileAndData(headerBytes)
			vs.diag.Emit("video_wire", map[string]any{
				"event": "packet", "direction": "out", "call_id": vs.callID,
				"frame": vs.frame, "packet": i, "ssrc": header.Ssrc,
				"seq": header.SequenceNumber, "rtp_ts": header.Timestamp, "marker": header.Marker,
				"header_hex":    hex.EncodeToString(headerBytes),
				"extension_hex": hex.EncodeToString(extension),
				"payload_bytes": len(payload), "protected_bytes": len(packet),
			})
		}
	}
	if len(packets) > 0 && idr {
		vs.keyframeRequired = false
	}
	return packets
}

func (vs *videoSender) enable(sendGated bool) {
	vs.mu.Lock()
	needsRecovery := !vs.active || (vs.sendGated && !sendGated)
	vs.active = true
	vs.sendGated = sendGated
	vs.keyframeRequired = vs.keyframeRequired || needsRecovery
	vs.mu.Unlock()
}

func (vs *videoSender) disable() {
	vs.mu.Lock()
	vs.active = false
	vs.sendGated = false
	vs.keyframeRequired = true
	vs.mu.Unlock()
}

func (vs *videoSender) requestKeyframe() {
	vs.mu.Lock()
	vs.keyframeRequired = true
	vs.mu.Unlock()
}

// send fragments one Annex-B access unit into PT-97 RTP packets (marker on the last) and
// sends them to the relay.
func (vs *videoSender) send(au []byte, duration time.Duration) error {
	if vs == nil {
		return ErrVideoFrameDropped
	}
	vs.mu.Lock()
	defer vs.mu.Unlock()
	if vs.ch == nil || len(au) == 0 {
		vs.keyframeRequired = true
		return ErrVideoFrameDropped
	}
	packets := vs.protectAccessUnitLocked(au, duration)
	if len(packets) == 0 {
		vs.keyframeRequired = true
		return ErrVideoFrameDropped
	}
	sent := 0
	wireBytes := 0
	for _, pkt := range packets {
		if _, err := sendMediaTaskPacket(vs.mediaContext, vs.cancelMedia, "video RTP sender", vs.ch.Send, pkt); err != nil {
			vs.keyframeRequired = true
			return err
		}
		sent++
		wireBytes += len(pkt)
	}
	frame := vs.frame
	if sent > 0 {
		vs.frame++
		if frame < 10 || frame%30 == 0 {
			vs.diag.Emit("video_out", map[string]any{
				"event": "frame", "call_id": vs.callID, "frame": frame,
				"ssrc": vs.ssrc, "rtp_ts": vs.pipe.SenderStats().RtpTimestamp,
				"packets": sent, "access_unit_bytes": len(au),
				"wire_bytes": wireBytes, "duration_ms": duration.Milliseconds(),
				"duration_samples": videoRtpDurationSamples(duration),
			})
		}
	}
	if sent > 0 && !vs.logged {
		vs.logged = true
		vs.log.Info().
			Int("packets", sent).
			Uint32("ssrc", vs.ssrc).
			Msg("first video RTP sent to relay, outbound video flowing")
	}
	return nil
}

// rmsFloat32 returns the root-mean-square level of a PCM frame, a cheap loudness
// metric for the media diagnostic streams (avoids inlining raw float32 PCM).
func rmsFloat32(f []float32) float64 {
	if len(f) == 0 {
		return 0
	}
	var sum float64
	for _, s := range f {
		sum += float64(s) * float64(s)
	}
	return math.Sqrt(sum / float64(len(f)))
}
