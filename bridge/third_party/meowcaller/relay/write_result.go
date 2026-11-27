package relay

import "io"

// A relay packet is a single DataChannel message: a partial write must not be
// retried as a second message, nor treated as a successful packet.
func relayWriteResult(n, expected int, err error) error {
	if err != nil {
		return err
	}
	if n != expected {
		return io.ErrShortWrite
	}
	return nil
}
