package connector

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestRTCCallLinkToken(t *testing.T) {
	for _, raw := range []string{"Token_123-xyz", "https://call.whatsapp.com/video/Token_123-xyz"} {
		if token, ok := rtcCallLinkToken(raw, "video"); !ok || token != "Token_123-xyz" {
			t.Fatal("valid link rejected")
		}
	}
	for _, raw := range []string{"", "https://call.whatsapp.com/audio/Token", "https://call.whatsapp.com.evil/video/Token", "https://user@call.whatsapp.com/video/Token", "https://call.whatsapp.com/video/Token?secret=1", "https://call.whatsapp.com/video/Token#fragment", "https://call.whatsapp.com/video/%2fToken", "https://call.whatsapp.com/video/Token/", "Token\n", " Token", strings.Repeat("x", 257)} {
		if _, ok := rtcCallLinkToken(raw, "video"); ok {
			t.Errorf("invalid link accepted: %q", raw)
		}
	}
}

func TestRTCCallLinkRejectsInvalidRequestsBeforeProvider(t *testing.T) {
	for _, body := range []string{`{}`, `null`, `[]`, `{"media":"VIDEO","link":"Token"}`, `{"media":"video","link":null}`, `{"media":"video","link":"Token","link":"Other"}`, `{"media":"video","link":"Token","extra":true}`, `{"media":"video","link":"Token"} {}`, `{"media":"video"}`, `{"media":"video","link":"` + strings.Repeat("x", 3000) + `"}`} {
		w := httptest.NewRecorder()
		r := httptest.NewRequest(http.MethodPost, "/preview", strings.NewReader(body))
		serveRTCCallLink(w, r, true, func(context.Context, rtcCallLinkRequest) (rtcCallLinkResult, error) {
			t.Fatal("provider invoked for invalid request")
			return rtcCallLinkResult{}, nil
		})
		if w.Code != 400 || w.Header().Get("Cache-Control") != "no-store" {
			t.Errorf("status %d for invalid request", w.Code)
		}
	}
	w := httptest.NewRecorder()
	serveRTCCallLink(w, httptest.NewRequest(http.MethodPost, "/create", strings.NewReader(`{"media":"audio","link":""}`)), false, func(context.Context, rtcCallLinkRequest) (rtcCallLinkResult, error) {
		t.Fatal("provider invoked")
		return rtcCallLinkResult{}, nil
	})
	if w.Code != 400 {
		t.Fatal(w.Code)
	}
}

func TestRTCCallLinkPreviewNeverEchoesToken(t *testing.T) {
	w := httptest.NewRecorder()
	calls := 0
	serveRTCCallLink(w, httptest.NewRequest(http.MethodPost, "/preview", strings.NewReader(`{"media":"video","link":"https://call.whatsapp.com/video/SecretToken"}`)), true, func(ctx context.Context, req rtcCallLinkRequest) (rtcCallLinkResult, error) {
		calls++
		if _, ok := ctx.Deadline(); !ok || req.Link != "SecretToken" || req.Media != "video" {
			t.Fatal("invalid provider request")
		}
		approval, admin := true, false
		return rtcCallLinkResult{Media: "video", ApprovalRequired: &approval, IsAdmin: &admin}, nil
	})
	if calls != 1 || w.Code != 200 || strings.Contains(w.Body.String(), "SecretToken") || !strings.Contains(w.Body.String(), `"approval_required":true`) || !strings.Contains(w.Body.String(), `"is_admin":false`) {
		t.Fatal(w.Code, w.Body.String())
	}
}

func TestRTCCallLinkCreateOnlyReturnsCanonicalResult(t *testing.T) {
	for _, test := range []struct {
		result rtcCallLinkResult
		want   int
	}{
		{rtcCallLinkResult{Media: "audio", URL: "https://call.whatsapp.com/audio/SecretToken"}, 200},
		{rtcCallLinkResult{Media: "video", URL: "https://call.whatsapp.com/video/SecretToken"}, 502},
		{rtcCallLinkResult{Media: "audio", URL: "SecretToken"}, 502},
		{rtcCallLinkResult{Media: "audio", URL: "https://evil.example/SecretToken"}, 502},
	} {
		w := httptest.NewRecorder()
		serveRTCCallLink(w, httptest.NewRequest(http.MethodPost, "/create", strings.NewReader(`{"media":"audio"}`)), false, func(context.Context, rtcCallLinkRequest) (rtcCallLinkResult, error) { return test.result, nil })
		if w.Code != test.want || w.Header().Get("Cache-Control") != "no-store" || (test.want != 200 && strings.Contains(w.Body.String(), "SecretToken")) {
			t.Fatal(w.Code, w.Body.String())
		}
	}
}

func TestRTCCallLinkNoAutomaticRetryOrErrorLeak(t *testing.T) {
	for _, test := range []struct {
		err  error
		want int
	}{
		{errors.New("provider failure: SecretToken"), 502}, {errRTCCallLinkUnavailable, 503}, {errRTCCallLinkBusy, 409}, {context.Canceled, 504}, {context.DeadlineExceeded, 504},
	} {
		calls := 0
		w := httptest.NewRecorder()
		serveRTCCallLink(w, httptest.NewRequest(http.MethodPost, "/create", strings.NewReader(`{"media":"audio"}`)), false, func(context.Context, rtcCallLinkRequest) (rtcCallLinkResult, error) {
			calls++
			return rtcCallLinkResult{}, test.err
		})
		if calls != 1 || w.Code != test.want || strings.Contains(w.Body.String(), "SecretToken") {
			t.Fatal(calls, w.Code, w.Body.String())
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	w := httptest.NewRecorder()
	serveRTCCallLink(w, httptest.NewRequest(http.MethodPost, "/create", strings.NewReader(`{"media":"audio"}`)).WithContext(ctx), false, func(context.Context, rtcCallLinkRequest) (rtcCallLinkResult, error) {
		t.Fatal("cancelled operation invoked")
		return rtcCallLinkResult{}, nil
	})
	if w.Code != 504 {
		t.Fatal(w.Code)
	}
}

func TestRTCCallLinkRejectsIncompletePreview(t *testing.T) {
	w := httptest.NewRecorder()
	serveRTCCallLink(w, httptest.NewRequest(http.MethodPost, "/preview", strings.NewReader(`{"media":"audio","link":"SecretToken"}`)), true, func(context.Context, rtcCallLinkRequest) (rtcCallLinkResult, error) {
		return rtcCallLinkResult{Media: "audio"}, nil
	})
	if w.Code != 502 || strings.Contains(w.Body.String(), "SecretToken") {
		t.Fatal(w.Code, w.Body.String())
	}
}
