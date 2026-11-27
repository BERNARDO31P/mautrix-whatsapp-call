package connector

import (
	waBinary "go.mau.fi/whatsmeow/binary"
	waLog "go.mau.fi/whatsmeow/util/log"
)

// The pinned whatsmeow client logs raw decoded nodes at DEBUG on both send
// and receive. Suppress only token-bearing call-link protocol nodes; do not
// alter ordinary messaging, other call logs, or the configured log level.
type rtcCallLinkLogger struct{ waLog.Logger }

func (l rtcCallLinkLogger) Sub(module string) waLog.Logger {
	return rtcCallLinkLogger{l.Logger.Sub(module)}
}

func isRTCCallLinkLogNode(node *waBinary.Node) bool {
	if node == nil {
		return false
	}
	switch node.Tag {
	case "link_create", "link_query", "link_join", "waiting_room", "waiting_room_update":
		return true
	case "call", "ack":
		if typ, ok := node.Attrs["type"].(string); ok && (typ == "link_create" || typ == "link_query" || typ == "link_join") {
			return true
		}
		for _, child := range node.GetChildren() {
			if isRTCCallLinkLogNode(&child) {
				return true
			}
		}
	}
	return false
}

func rtcCallLinkLogArgs(args []any) bool {
	for _, arg := range args {
		switch node := arg.(type) {
		case *waBinary.Node:
			if isRTCCallLinkLogNode(node) {
				return true
			}
		case waBinary.Node:
			if isRTCCallLinkLogNode(&node) {
				return true
			}
		}
	}
	return false
}

const rtcCallLinkRedactedLog = "WhatsApp call-link protocol node (redacted)"

func (l rtcCallLinkLogger) Debugf(msg string, args ...any) {
	if rtcCallLinkLogArgs(args) {
		l.Logger.Debugf(rtcCallLinkRedactedLog)
		return
	}
	l.Logger.Debugf(msg, args...)
}
func (l rtcCallLinkLogger) Infof(msg string, args ...any) {
	if rtcCallLinkLogArgs(args) {
		l.Logger.Infof(rtcCallLinkRedactedLog)
		return
	}
	l.Logger.Infof(msg, args...)
}
func (l rtcCallLinkLogger) Warnf(msg string, args ...any) {
	if rtcCallLinkLogArgs(args) {
		l.Logger.Warnf(rtcCallLinkRedactedLog)
		return
	}
	l.Logger.Warnf(msg, args...)
}
func (l rtcCallLinkLogger) Errorf(msg string, args ...any) {
	if rtcCallLinkLogArgs(args) {
		l.Logger.Errorf(rtcCallLinkRedactedLog)
		return
	}
	l.Logger.Errorf(msg, args...)
}
