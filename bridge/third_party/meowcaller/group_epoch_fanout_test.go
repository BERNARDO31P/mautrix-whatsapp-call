package meowcaller

import (
	"context"
	"errors"
	"testing"
)

func TestGroupEpochFanoutCommitBoundary(t *testing.T) {
	failure := errors.New("send failed")
	secondFailure := errors.New("another send failed")
	commitFailure := errors.New("commit failed")
	for _, tc := range []struct {
		name                string
		count               int
		failures            map[int]error
		cancelBefore        bool
		cancelAfter         int
		commitError         error
		expectedError       error
		expectedSecondError error
		sends               int
		commits             int
	}{
		{name: "all submitted", count: 3, cancelAfter: -1, sends: 3, commits: 1},
		{name: "first failed", count: 3, cancelAfter: -1, failures: map[int]error{0: failure}, expectedError: failure, sends: 3},
		{name: "last failed", count: 3, cancelAfter: -1, failures: map[int]error{2: failure}, expectedError: failure, sends: 3},
		{name: "multiple failures", count: 3, cancelAfter: -1, failures: map[int]error{0: failure, 2: secondFailure}, expectedError: failure, expectedSecondError: secondFailure, sends: 3},
		{name: "already canceled", count: 3, cancelBefore: true, cancelAfter: -1, expectedError: context.Canceled},
		{name: "canceled midway", count: 3, cancelAfter: 0, expectedError: context.Canceled, sends: 1},
		{name: "canceled at final submission", count: 3, cancelAfter: 2, expectedError: context.Canceled, sends: 3},
		{name: "commit failed", count: 3, cancelAfter: -1, commitError: commitFailure, expectedError: commitFailure, sends: 3, commits: 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			if tc.cancelBefore {
				cancel()
			}
			sends, commits := 0, 0
			err := completeGroupEpochFanout(ctx, tc.count, func(i int) error {
				if i != sends {
					t.Fatalf("submission order: got %d, want %d", i, sends)
				}
				sends++
				if i == tc.cancelAfter {
					cancel()
				}
				return tc.failures[i]
			}, func() error {
				commits++
				if sends != tc.count {
					t.Fatal("commit preceded last submission")
				}
				return tc.commitError
			})
			if !errors.Is(err, tc.expectedError) || (tc.expectedSecondError != nil && !errors.Is(err, tc.expectedSecondError)) {
				t.Fatalf("unexpected result: %v", err)
			}
			if sends != tc.sends || commits != tc.commits {
				t.Fatalf("sends=%d commits=%d, want %d/%d", sends, commits, tc.sends, tc.commits)
			}
		})
	}
}

func TestGroupEpochFanoutEmptyRejectsCommit(t *testing.T) {
	err := completeGroupEpochFanout(context.Background(), 0, func(int) error {
		t.Fatal("empty fanout sent a node")
		return nil
	}, func() error {
		t.Fatal("empty fanout committed")
		return nil
	})
	if err == nil {
		t.Fatal("empty fanout accepted")
	}
}
