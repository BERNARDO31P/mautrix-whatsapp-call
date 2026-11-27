package connector

import (
	"net"
	"os"
	"path/filepath"
	"runtime"
	"testing"
)

func TestIncomingRTCSocketPresent(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "rtc.sock")
	if incomingRTCSocketPresent("") || incomingRTCSocketPresent(path) || incomingRTCSocketPresent(dir) {
		t.Fatal("admitted missing configuration or non-socket")
	}
	if err := os.WriteFile(path, nil, 0600); err != nil {
		t.Fatal(err)
	}
	if incomingRTCSocketPresent(path) {
		t.Fatal("admitted ordinary file")
	}
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	if runtime.GOOS == "windows" {
		t.Skip("worker Unix socket is deployed on Linux")
	}
	listener, err := net.Listen("unix", path)
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	if !incomingRTCSocketPresent(path) {
		t.Fatal("rejected existing socket")
	}
	if err := listener.Close(); err != nil {
		t.Fatal(err)
	}
	if incomingRTCSocketPresent(path) {
		t.Fatal("admitted removed socket")
	}
}
