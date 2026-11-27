package connector

import (
	"reflect"
	"testing"
)

func TestRTCAdHocTargets(t *testing.T) {
	resolve := func(user string) (string, bool) {
		v, ok := map[string]string{"alice": "1@lid", "alias": "1@lid", "bob": "2@lid"}[user]
		return v, ok
	}
	for _, tc := range []struct {
		name        string
		group, room bool
		users       []string
		want        bool
	}{
		{"exact", true, false, []string{"alice", "bob"}, true},
		{"direct", false, false, []string{"alice", "bob"}, false},
		{"broadcast", true, true, []string{"alice", "bob"}, false},
		{"empty", true, false, nil, false},
		{"single", true, false, []string{"alice"}, false},
		{"unknown", true, false, []string{"alice", "bob", "unknown"}, false},
		{"duplicate", true, false, []string{"alice", "alice", "bob"}, false},
		{"alias", true, false, []string{"alice", "alias", "bob"}, false},
		{"too many", true, false, make([]string, 32), false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, ok := rtcAdHocTargets(tc.group, tc.room, tc.users, resolve)
			if ok != tc.want {
				t.Fatalf("accepted=%v, want %v", ok, tc.want)
			}
			if !ok && got != nil {
				t.Fatal("partial targets returned")
			}
			if ok && !reflect.DeepEqual(got, []string{"1@lid", "2@lid"}) {
				t.Fatalf("targets=%v", got)
			}
		})
	}
}
