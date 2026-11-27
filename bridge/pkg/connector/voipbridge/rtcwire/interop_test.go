package rtcwire

import (
	"bytes"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

// The same independent semantic corpus is consumed by the actual Rust codec.
// No calls, accounts, external network or production credentials are used.
func interopCorpus(t *testing.T) []Message {
	t.Helper()
	data, err := os.ReadFile("testdata/interop.json")
	if err != nil {
		t.Fatal(err)
	}
	var expected []Message
	if err = json.Unmarshal(data, &expected); err != nil {
		t.Fatal(err)
	}
	if len(expected) < 20 {
		t.Fatalf("incomplete corpus: %d frames", len(expected))
	}
	return expected
}

func writeInteropCorpus(t *testing.T, w io.Writer, expected []Message) {
	t.Helper()
	for i, msg := range expected {
		if err := Write(w, msg); err != nil {
			t.Fatalf("frame %d (%s): %v", i, msg.Type, err)
		}
	}
}

func checkInteropCorpus(t *testing.T, r io.Reader, expected []Message) {
	t.Helper()
	for i, want := range expected {
		got, err := Read(r)
		if err != nil {
			t.Fatalf("frame %d (%s): %v", i, want.Type, err)
		}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("frame %d (%s): semantic mismatch", i, want.Type)
		}
	}
	if _, err := Read(r); err != io.EOF {
		t.Fatalf("expected exact EOF, got %v", err)
	}
}

func TestRTCWireInteropCorpus(t *testing.T) {
	expected := interopCorpus(t)
	var encoded bytes.Buffer
	writeInteropCorpus(t, &encoded, expected)
	checkInteropCorpus(t, &encoded, expected)
}

func TestRTCWireInteropExport(t *testing.T) {
	dir := os.Getenv("RTC_WIRE_INTEROP_DIR")
	if dir == "" {
		t.Skip("run build/test-wire-interop.sh for cross-language exchange")
	}
	expected := interopCorpus(t)
	f, err := os.OpenFile(filepath.Join(dir, "go.frames"), os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	writeInteropCorpus(t, f, expected)
	if err = f.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestRTCWireInteropImport(t *testing.T) {
	dir := os.Getenv("RTC_WIRE_INTEROP_DIR")
	if dir == "" {
		t.Skip("run build/test-wire-interop.sh for cross-language exchange")
	}
	f, err := os.Open(filepath.Join(dir, "rust.frames"))
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	checkInteropCorpus(t, f, interopCorpus(t))
	// The runner checks this marker to ensure a filter typo cannot skip the test.
	if err := os.WriteFile(filepath.Join(dir, "go-import.ok"), []byte("PASS\n"), 0600); err != nil {
		t.Fatal(err)
	}
}
