package meowcaller

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestPickupOutcomePreservesCauseAndClassification(t *testing.T) {
	for _, test := range []struct {
		got, want, wrong error
	}{
		{pickupNotSubmitted(context.Canceled), ErrPickupNotSubmitted, ErrPickupOutcomeUnknown},
		{pickupOutcomeUnknown(context.Canceled), ErrPickupOutcomeUnknown, ErrPickupNotSubmitted},
	} {
		if !errors.Is(test.got, test.want) || errors.Is(test.got, test.wrong) || !errors.Is(test.got, context.Canceled) {
			t.Fatalf("classification lost: %v", test.got)
		}
	}
}

func TestPickupSettlementNeverAuthorizesPendingSend(t *testing.T) {
	done := make(chan struct{})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := waitPickupSettlement(ctx, done); !errors.Is(err, ErrPickupOutcomeUnknown) || !errors.Is(err, context.Canceled) {
		t.Fatalf("pending send: %v", err)
	}
	close(done)
	if err := waitPickupSettlement(context.Background(), done); err != nil {
		t.Fatal(err)
	}
	if err := waitPickupSettlement(context.Background(), nil); err != nil {
		t.Fatal(err)
	}
}

func TestPickupSettlementWaitsForCompletion(t *testing.T) {
	done := make(chan struct{})
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	result := make(chan error, 1)
	go func() { result <- waitPickupSettlement(ctx, done) }()
	select {
	case err := <-result:
		t.Fatalf("settled before provider return: %v", err)
	case <-time.After(time.Millisecond):
	}
	close(done)
	if err := <-result; err != nil {
		t.Fatal(err)
	}
}
