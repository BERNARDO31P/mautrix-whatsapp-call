package relay

import (
	"errors"
	"io"
	"testing"
)

func TestRelayWriteResult(t *testing.T) {
	original := errors.New("transport error")
	for _, tt := range []struct {
		n, expected int
		input, want error
	}{
		{10, 10, nil, nil}, {0, 10, nil, io.ErrShortWrite}, {5, 10, nil, io.ErrShortWrite}, {11, 10, nil, io.ErrShortWrite}, {5, 10, original, original}, {0, 0, nil, nil},
	} {
		if got := relayWriteResult(tt.n, tt.expected, tt.input); !errors.Is(got, tt.want) {
			t.Fatalf("%+v: %v", tt, got)
		}
	}
}
