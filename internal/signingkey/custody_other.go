//go:build !unix

package signingkey

import (
	"errors"
	"os"
)

func openNoFollow(string) (*os.File, error) {
	return nil, errors.New("no-follow key file reads are not supported on this platform")
}
