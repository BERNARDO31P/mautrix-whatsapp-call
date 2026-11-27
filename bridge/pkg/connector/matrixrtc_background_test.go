package connector

import (
	"context"
	"maunium.net/go/mautrix/bridgev2"
	"strings"
	"testing"
)

// The foreground admission check must precede even dereferencing the client.
// The nil fixture would otherwise return ErrNotLoggedIn; no socket, login or
// bridge fixtures are necessary to verify this early rejection.
func TestRTCForegroundLoginRejectsBackgroundSync(t *testing.T) {
	wa := &WhatsAppClient{rtcForegroundCalls: true}
	err := wa.ConnectBackground(context.Background(), nil)
	if err == nil || !strings.Contains(err.Error(), "foreground calling login") {
		t.Fatalf("expected foreground ownership refusal, got %v", err)
	}
	if wa.offlineSyncWaiter.Load() != nil {
		t.Fatal("background sync installed a waiter on a foreground login")
	}
}

func TestRTCNonForegroundLoginKeepsOriginalNotLoggedInResult(t *testing.T) {
	wa := &WhatsAppClient{}
	if err := wa.ConnectBackground(context.Background(), nil); err != bridgev2.ErrNotLoggedIn {
		t.Fatalf("expected original not-logged-in result, got %v", err)
	}
}
