package rtcwire

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"io"
	"math"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"
)

// These tests use the actual compiled Rust executable, never a mock server.
// The release test uses an empty private state directory and no credentials.
// The staging test has a separate explicit opt-in and fixture contract.
type workerProcess struct {
	cmd    *exec.Cmd
	done   chan error
	socket string
	log    *os.File
}

func workerEnv(state, socket string, enabled bool) []string {
	calls := "0"
	if enabled {
		calls = "1"
	}
	return []string{"PATH=" + os.Getenv("PATH"), "HOME=" + state,
		"RTC_WORKER_STATE_DIR=" + state, "RTC_WORKER_SOCKET=" + socket,
		"RTC_WORKER_SOCKET_MODE=0600", "RTC_WORKER_ENABLE_CALLS=" + calls}
}

func startWorker(t *testing.T, binaryPath, state, socket string, env []string) *workerProcess {
	t.Helper()
	if !filepath.IsAbs(binaryPath) || !filepath.IsAbs(state) || !filepath.IsAbs(socket) {
		t.Fatal("worker paths must be absolute")
	}
	log, err := os.CreateTemp(filepath.Dir(socket), "worker-log-*")
	if err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command(binaryPath)
	cmd.SysProcAttr = &syscall.SysProcAttr{Pdeathsig: syscall.SIGTERM}
	cmd.Env = env
	cmd.Stdout, cmd.Stderr = log, log
	if err = cmd.Start(); err != nil {
		log.Close()
		t.Fatal(err)
	}
	p := &workerProcess{cmd: cmd, done: make(chan error, 1), socket: socket, log: log}
	go func() { p.done <- cmd.Wait(); close(p.done) }()
	t.Cleanup(func() {
		select {
		case <-p.done:
		default:
			_ = p.cmd.Process.Kill()
			<-p.done
		}
		log.Close()
		if t.Failed() {
			if raw, err := os.ReadFile(log.Name()); err == nil {
				if len(raw) > 8192 {
					raw = raw[len(raw)-8192:]
				}
				t.Logf("worker log tail: %s", raw)
			}
		}
	})
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		select {
		case err = <-p.done:
			t.Fatalf("worker exited during startup: %v", err)
		default:
		}
		c, err := net.DialTimeout("unix", socket, 100*time.Millisecond)
		if err == nil {
			_ = c.SetDeadline(time.Now().Add(time.Second))
			if Write(c, Message{Type: "ping", CallID: "health"}) == nil {
				reply, readErr := Read(c)
				c.Close()
				if readErr == nil && reply.Type == "pong" && reply.CallID == "health" {
					return p
				}
			} else {
				c.Close()
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatal("worker never completed a real Go/Rust health exchange")
	return nil
}

func (p *workerProcess) connect(t *testing.T, timeout time.Duration) net.Conn {
	t.Helper()
	c, err := net.DialTimeout("unix", p.socket, 2*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if err = c.SetDeadline(time.Now().Add(timeout)); err != nil {
		c.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() { c.Close() })
	return c
}
func (p *workerProcess) stop(t *testing.T) {
	t.Helper()
	if err := p.cmd.Process.Signal(syscall.SIGTERM); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-p.done:
		if err != nil {
			t.Fatalf("worker shutdown failed: %v", err)
		}
	case <-time.After(50 * time.Second):
		t.Fatal("worker shutdown deadline exceeded")
	}
	if _, err := os.Lstat(p.socket); !os.IsNotExist(err) {
		t.Fatalf("socket not retired on shutdown: %v", err)
	}
}
func readCall(t *testing.T, c net.Conn, id string) Message {
	t.Helper()
	m, err := Read(c)
	if err != nil {
		t.Fatal(err)
	}
	if m.CallID != id {
		t.Fatalf("wrong call identity: %q", m.CallID)
	}
	return m
}
func writeCall(t *testing.T, c net.Conn, m Message) {
	t.Helper()
	if err := Write(c, m); err != nil {
		t.Fatal(err)
	}
}
func fixtureCall(direction, id, format string) Message {
	m := Message{Type: direction, CallID: id, LoginID: "disposable-fixture", RoomID: "!fixture:example.invalid", SlotID: "m.call#room", MembershipFormat: format, GhostMXID: "@ghost:example.invalid", ExpectedUserMXID: "@peer:example.invalid", Peer: "123@s.whatsapp.net"}
	if direction == "outgoing" {
		m.ExpectedDevice = "DEVICE"
		m.ExpectedSigningKey = strings.Repeat("A", 43)
		m.ExpectedIdentityKey = strings.Repeat("A", 43)
		m.Intent = "audio"
	}
	return m
}
func expectClosed(t *testing.T, c net.Conn) {
	t.Helper()
	_ = c.SetReadDeadline(time.Now().Add(5 * time.Second))
	_, err := Read(c)
	if !errors.Is(err, io.EOF) && !errors.Is(err, net.ErrClosed) && !errors.Is(err, syscall.ECONNRESET) {
		t.Fatalf("expected socket closure, got %v", err)
	}
}

func TestRTCWorkerRelease(t *testing.T) {
	binaryPath := os.Getenv("RTC_TEST_RELEASE_WORKER")
	if binaryPath == "" {
		t.Skip("release worker binary not supplied")
	}
	// Keep the Unix-domain socket well below Linux sun_path's length limit.
	root, err := os.MkdirTemp("", "rtc-release-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(root) })
	socket := filepath.Join(root, "w.sock")
	state := filepath.Join(root, "state")
	for _, enabled := range []bool{false, true, false} {
		p := startWorker(t, binaryPath, state, socket, workerEnv(state, socket, enabled))
		for _, format := range []string{"msc", "room_state"} {
			for _, direction := range []string{"incoming", "outgoing"} {
				// Repeat exact ID: rejected admission must not register a replay.
				for repeat := 0; repeat < 2; repeat++ {
					c := p.connect(t, 5*time.Second)
					req := fixtureCall(direction, "repeated-"+direction+"-"+format, format)
					writeCall(t, c, req)
					got := readCall(t, c, req.CallID)
					reason := "RTC calls disabled by configuration"
					if enabled {
						reason = "MATRIXRTC_SERVER_NAME is required"
					}
					if got.Type != "error" || !strings.Contains(got.Reason, reason) {
						t.Fatalf("unexpected pre-admission result: %q %q", got.Type, got.Reason)
					}
					expectClosed(t, c)
					c.Close()
				}
			}
		}
		// Reject invalid first message and malformed body without killing the listener.
		bad := p.connect(t, 5*time.Second)
		writeCall(t, bad, Message{Type: "answer", CallID: "invalid-first"})
		expectClosed(t, bad)
		bad.Close()
		bad = p.connect(t, 5*time.Second)
		var h [4]byte
		binary.BigEndian.PutUint32(h[:], uint32(MaxPayloadSize+1))
		if _, err := bad.Write(h[:]); err != nil {
			t.Fatal(err)
		}
		expectClosed(t, bad)
		bad.Close()
		c := p.connect(t, 5*time.Second)
		writeCall(t, c, Message{Type: "ping", CallID: "health"})
		if m := readCall(t, c, "health"); m.Type != "pong" {
			t.Fatalf("worker lost health: %s", m.Type)
		}
		expectClosed(t, c)
		c.Close()
		// Shutdown must close a client which is still awaiting its first frame.
		pending := p.connect(t, 5*time.Second)
		p.stop(t)
		expectClosed(t, pending)
		pending.Close()
		entries, err := os.ReadDir(filepath.Join(state, "seen-calls"))
		if err != nil || len(entries) != 0 {
			t.Fatalf("failed admissions changed replay store: %v (%d entries)", err, len(entries))
		}
	}
}

// Energy ratio at the expected frequency, with an absolute level floor so
// silence and near-zero decoder noise cannot satisfy the assertion.
func toneRatio(pcm []float32, hz float64) float64 {
	if len(pcm) == 0 {
		return 0
	}
	var re, im, energy float64
	for i, s := range pcm {
		v := float64(s)
		a := 2 * math.Pi * hz * float64(i) / 16000
		re += v * math.Cos(a)
		im += v * math.Sin(a)
		energy += v * v
	}
	if energy/float64(len(pcm)) < 1e-5 {
		return 0
	}
	return 2 * (re*re + im*im) / (float64(len(pcm)) * energy)
}
func TestWorkerHarnessToneDiscrimination(t *testing.T) {
	pcm := make([]float32, 32000)
	if toneRatio(pcm, 660) != 0 {
		t.Fatal("silence accepted")
	}
	for i := range pcm {
		pcm[i] = float32(.25 * math.Sin(2*math.Pi*660*float64(i)/16000))
	}
	if toneRatio(pcm, 660) < .99 || toneRatio(pcm, 440) > .01 {
		t.Fatal("frequency discrimination failed")
	}
}

type stagingFixture struct {
	Request    Message `json:"request"`
	End        string  `json:"end"` // gateway, peer, or cancel (before answer)
	ControlDir string  `json:"control_dir"`
}

// TestRTCWorkerStaging is driven by the SDK peer in e2e_call/worker_bridge.rs.
// It emulates only the WhatsApp-side wire boundary, not the WhatsApp provider.
// The worker, Matrix SDK, membership/key exchange, SFU and PCM paths are real.
func TestRTCWorkerStaging(t *testing.T) {
	path := os.Getenv("RTC_TEST_STAGING_FIXTURE")
	if path == "" {
		t.Skip("explicit disposable staging fixture not supplied")
	}
	if os.Getenv("RTC_WORKER_E2E_DISPOSABLE") != "1" {
		t.Fatal("disposable staging opt-in required")
	}
	state := os.Getenv("RTC_WORKER_E2E_STATE")
	if resolved, e := filepath.EvalSymlinks(state); e != nil || resolved != state {
		t.Fatal("staging state must be an existing canonical directory")
	}
	if !filepath.IsAbs(state) || !strings.HasPrefix(filepath.Base(state), "rtc-worker-e2e-") {
		t.Fatal("dedicated staging state directory required")
	}
	marker, err := os.ReadFile(filepath.Join(state, ".matrixrtc-e2e-disposable"))
	if err != nil || !bytes.Equal(marker, []byte("disposable\n")) {
		t.Fatal("disposable staging marker required")
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var f stagingFixture
	if err = json.Unmarshal(raw, &f); err != nil {
		t.Fatal(err)
	}
	if !filepath.IsAbs(f.ControlDir) || !strings.HasPrefix(filepath.Base(f.ControlDir), "rtc-worker-control-") {
		t.Fatal("dedicated control directory required")
	}
	if f.Request.Type != "incoming" && f.Request.Type != "outgoing" {
		t.Fatal("unsupported fixture direction")
	}
	if f.End != "gateway" && f.End != "peer" && f.End != "cancel" {
		t.Fatal("unsupported terminal case")
	}
	server := os.Getenv("RTC_WORKER_E2E_SERVER")
	if server == "" || !strings.HasSuffix(f.Request.GhostMXID, ":"+server) || !strings.HasSuffix(f.Request.ExpectedUserMXID, ":"+server) {
		t.Fatal("fixture identity must belong to explicit staging server")
	}
	root, err := os.MkdirTemp("", "rtc-stage-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(root) })
	socket := filepath.Join(root, "w.sock")
	env := append(workerEnv(state, socket, true), "MATRIXRTC_SERVER_NAME="+server, "MATRIX_HOMESERVER=https://"+server)
	// Do not inherit production worker socket, state, tokens or configuration.
	pass := os.Getenv("RTC_WORKER_E2E_PASSPHRASE_FILE")
	if !filepath.IsAbs(pass) {
		t.Fatal("explicit staging passphrase file required")
	}
	env = append(env, "RTC_WORKER_STORE_PASSPHRASE_FILE="+pass)
	p := startWorker(t, os.Getenv("RTC_TEST_RELEASE_WORKER"), state, socket, env)
	c := p.connect(t, 150*time.Second)
	req := f.Request
	writeCall(t, c, req)
	if req.Type == "outgoing" {
		m := readCall(t, c, req.CallID)
		if m.Type != "dial" || m.Intent != "audio" || m.Video {
			t.Fatalf("expected audio dial, got %q: %s", m.Type, m.Reason)
		}
		if f.End == "cancel" {
			writeCall(t, c, Message{Type: "reject", CallID: req.CallID, Reason: "fixture decline"})
			expectStagingClosure(t, c, req.CallID)
			p.stop(t)
			return
		}
		writeCall(t, c, Message{Type: "whatsapp_answered", CallID: req.CallID})
	} else if f.End == "cancel" {
		// The peer signals that the worker's ringing membership was observed.
		waitHarnessFile(t, filepath.Join(f.ControlDir, "ringing"), 75*time.Second)
		writeCall(t, c, Message{Type: "ended", CallID: req.CallID})
		// There must be no answer or media before closure.
		expectStagingClosure(t, c, req.CallID)
		p.stop(t)
		return
	}
	for {
		m := readCall(t, c, req.CallID)
		switch m.Type {
		case "answer":
			goto answered
		case "matrix_audio_mute", "matrix_audio_unmute", "matrix_video_start", "matrix_video_stop", "matrix_video_mute", "matrix_video_unmute":
		default:
			t.Fatalf("unexpected pre-answer event %s: %s", m.Type, m.Reason)
		}
	}
answered:
	// Pace the synthetic provider's 440 Hz tone, concurrently record 660 Hz
	// from the real Matrix peer. One reader, one writer, bounded deadlines.
	_ = c.SetDeadline(time.Now().Add(75 * time.Second))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sendErr := make(chan error, 1)
	go func() {
		ticker := time.NewTicker(60 * time.Millisecond)
		defer ticker.Stop()
		pos := 0
		for {
			select {
			case <-ctx.Done():
				sendErr <- nil
				return
			case <-ticker.C:
			}
			pcm := make([]float32, AudioSamples)
			for i := range pcm {
				pcm[i] = float32(.25 * math.Sin(2*math.Pi*440*float64(pos+i)/16000))
			}
			pos += len(pcm)
			data, e := EncodePCM(pcm)
			if e == nil {
				e = Write(c, Message{Type: "whatsapp_audio", CallID: req.CallID, Data: data})
			}
			if e != nil {
				sendErr <- e
				return
			}
		}
	}()
	// Ensure the producer is settled before any terminal signal on the same wire.
	settled := false
	defer func() {
		if !settled {
			cancel()
			c.Close()
			<-sendErr
		}
	}()
	pcm := make([]float32, 0, AudioSamples*35)
	toneOK := false
	for !toneOK {
		m := readCall(t, c, req.CallID)
		switch m.Type {
		case "matrix_audio":
			samples, e := DecodePCM(m.Data)
			if e != nil {
				t.Fatal(e)
			}
			pcm = append(pcm, samples...)
			if len(pcm) >= AudioSamples*35 {
				toneOK = toneRatio(pcm, 660) > .45
				pcm = pcm[:0]
			}
		case "matrix_audio_mute", "matrix_audio_unmute", "matrix_video_start", "matrix_video_stop", "matrix_video_mute", "matrix_video_unmute":
		default:
			t.Fatalf("unexpected active event %s: %s", m.Type, m.Reason)
		}
	}
	if err = writeHarnessMark(filepath.Join(f.ControlDir, "gateway-media-ok")); err != nil {
		t.Fatal(err)
	}
	// Drain the socket while the peer verifies its tone; blocking here could
	// backpressure the real writer and turn a good call into a false failure.
	for !harnessFileReady(t, filepath.Join(f.ControlDir, "peer-media-ok")) {
		m := readCall(t, c, req.CallID)
		if !activeHarnessEvent(m.Type) {
			t.Fatalf("unexpected event while verifying peer tone: %s %s", m.Type, m.Reason)
		}
	}
	cancel()
	if err = <-sendErr; err != nil {
		settled = true
		t.Fatal(err)
	}
	settled = true
	if f.End == "peer" {
		if err = writeHarnessMark(filepath.Join(f.ControlDir, "ready-for-peer-end")); err != nil {
			t.Fatal(err)
		}
	}
	if f.End == "gateway" {
		writeCall(t, c, Message{Type: "ended", CallID: req.CallID})
	}
	deadline := time.Now().Add(45 * time.Second)
	_ = c.SetReadDeadline(deadline)
	terminal := false
	for {
		m, e := Read(c)
		if errors.Is(e, io.EOF) || errors.Is(e, syscall.ECONNRESET) {
			break
		}
		if e != nil {
			t.Fatal(e)
		}
		if m.CallID != req.CallID {
			t.Fatal("cross-call event")
		}
		if terminal && m.Type == "matrix_audio" {
			t.Fatal("media after terminal")
		}
		switch m.Type {
		case "hangup":
			if f.End != "peer" || terminal {
				t.Fatal("unexpected duplicate terminal")
			}
			terminal = true
		case "matrix_audio", "matrix_audio_mute", "matrix_audio_unmute", "matrix_video_start", "matrix_video_stop", "matrix_video_mute", "matrix_video_unmute":
		default:
			t.Fatalf("unexpected terminal event: %s %s", m.Type, m.Reason)
		}
	}
	if f.End == "peer" && !terminal {
		t.Fatal("peer departure did not produce hangup")
	}
	p.stop(t)
}

func activeHarnessEvent(kind string) bool {
	switch kind {
	case "matrix_audio", "matrix_audio_mute", "matrix_audio_unmute", "matrix_video_start", "matrix_video_stop", "matrix_video_mute", "matrix_video_unmute":
		return true
	}
	return false
}
func expectStagingClosure(t *testing.T, c net.Conn, id string) {
	t.Helper()
	_ = c.SetReadDeadline(time.Now().Add(45 * time.Second))
	for {
		m, e := Read(c)
		if errors.Is(e, io.EOF) || errors.Is(e, syscall.ECONNRESET) {
			return
		}
		if e != nil {
			t.Fatal(e)
		}
		if m.CallID != id || !activeHarnessEvent(m.Type) || m.Type == "matrix_audio" {
			t.Fatalf("unexpected pre-answer cancellation event: %s %s", m.Type, m.Reason)
		}
	}
}
func writeHarnessMark(path string) error {
	tmp, err := os.CreateTemp(filepath.Dir(path), ".rendezvous-*")
	if err != nil {
		return err
	}
	defer os.Remove(tmp.Name())
	if _, err = tmp.Write([]byte("PASS\n")); err != nil {
		tmp.Close()
		return err
	}
	if err = tmp.Close(); err != nil {
		return err
	}
	return os.Rename(tmp.Name(), path)
}
func harnessFileReady(t *testing.T, path string) bool {
	t.Helper()
	raw, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		return false
	}
	if err != nil {
		t.Fatal(err)
	}
	if string(raw) != "PASS\n" {
		t.Fatal("invalid staging rendezvous")
	}
	return true
}
func waitHarnessFile(t *testing.T, path string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		raw, err := os.ReadFile(path)
		if err == nil {
			if string(raw) != "PASS\n" {
				t.Fatal("invalid staging rendezvous")
			}
			return
		}
		if !os.IsNotExist(err) {
			t.Fatal(err)
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("staging rendezvous timed out: %s", filepath.Base(path))
}
