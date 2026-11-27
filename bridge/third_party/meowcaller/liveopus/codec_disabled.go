//go:build !liveopus || !cgo

package liveopus

import "errors"

const Available = false

func Version() string { return "unavailable" }

var errUnavailable = errors.New("native Opus codec is unavailable in this build")

type Encoder struct{}
type Decoder struct{}

func NewEncoder(int) (*Encoder, error)            { return nil, errUnavailable }
func (*Encoder) Encode([]float32) ([]byte, error) { return nil, errUnavailable }
func (*Encoder) Close()                           {}
func NewDecoder() (*Decoder, error)               { return nil, errUnavailable }
func (*Decoder) Decode([]byte) ([]float32, error) { return nil, errUnavailable }
func (*Decoder) Close()                           {}
