package meowcaller

// The receiver retains -1 until the first orientation extension arrives.
// A missing extension does not make a complete picture invalid. Export a
// valid upright default until a real rotation is observed; later frames
// retain the last rotation in the per-stream receive state.
func videoFrameOrientation(orientation int) int {
	if orientation < 0 || orientation > 3 {
		return 0
	}
	return orientation
}
