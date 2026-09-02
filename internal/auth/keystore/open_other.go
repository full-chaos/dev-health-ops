//go:build !unix

package keystore

import (
	"errors"
	"os"
)

// errUnsupportedPlatform is a structural, platform-wide refusal: it is
// returned for every path on a platform with no O_NOFOLLOW, whether or not
// anything is there. Failing closed is the only correct posture -- opening the
// key without the symlink guard would be a weaker custody contract wearing the
// same name.
var errUnsupportedPlatform = errors.New("bounded no-follow file reads are not supported on this platform")

func openNoFollow(string) (*os.File, error) {
	return nil, errUnsupportedPlatform
}
