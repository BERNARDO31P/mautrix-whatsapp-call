package meowcaller

import "testing"

func TestVideoFrameOrientation(t *testing.T) {
	for _, tc := range []struct{ input, want int }{
		{-1, 0}, {0, 0}, {1, 1}, {2, 2}, {3, 3}, {4, 0},
	} {
		if got := videoFrameOrientation(tc.input); got != tc.want {
			t.Errorf("orientation %d: got %d, want %d", tc.input, got, tc.want)
		}
	}
}
