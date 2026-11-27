package connector

import (
	"fmt"
	waBinary "go.mau.fi/whatsmeow/binary"
	waLog "go.mau.fi/whatsmeow/util/log"
	"strings"
	"testing"
)

type rtcCaptureLinkLogger struct{ lines *[]string }

func (l rtcCaptureLinkLogger) Sub(string) waLog.Logger { return l }
func (l rtcCaptureLinkLogger) Debugf(msg string, args ...any) {
	*l.lines = append(*l.lines, fmt.Sprintf(msg, args...))
}
func (l rtcCaptureLinkLogger) Infof(msg string, args ...any)  { l.Debugf(msg, args...) }
func (l rtcCaptureLinkLogger) Warnf(msg string, args ...any)  { l.Debugf(msg, args...) }
func (l rtcCaptureLinkLogger) Errorf(msg string, args ...any) { l.Debugf(msg, args...) }

func TestRTCCallLinkRawLoggerRedaction(t *testing.T) {
	var lines []string
	l := rtcCallLinkLogger{rtcCaptureLinkLogger{&lines}}.Sub("Send")
	call := waBinary.Node{Tag: "call", Content: []waBinary.Node{{Tag: "link_query", Attrs: waBinary.Attrs{"token": "SecretToken"}}}}
	ack := waBinary.Node{Tag: "ack", Attrs: waBinary.Attrs{"type": "link_create"}, Content: []waBinary.Node{{Tag: "link_create", Attrs: waBinary.Attrs{"token": "SecretToken"}}}}
	ordinary := waBinary.Node{Tag: "message", Attrs: waBinary.Attrs{"id": "OrdinaryMessage"}}
	l.Debugf("%s", &call)
	l.Warnf("%v", ack)
	l.Infof("%s", &ordinary)
	l.Errorf("unrelated %s", "diagnostic")
	if len(lines) != 4 || lines[0] != rtcCallLinkRedactedLog || lines[1] != rtcCallLinkRedactedLog || !strings.Contains(lines[2], "OrdinaryMessage") || lines[3] != "unrelated diagnostic" {
		t.Fatal(lines)
	}
}
