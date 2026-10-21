// Package sessionid is the session.v1 wire form of a session id, "ses_" plus
// the unpadded base64url of the uuid's 16 bytes. It has no dependency beyond
// the uuid type, so a reader of access tokens (go-api's policy) can parse the
// sid claim without linking the session store.
package sessionid

import (
	"encoding/base64"
	"errors"
	"strings"

	"github.com/google/uuid"
)

const prefix = "ses_"

// ErrMalformed is a string that is not a session id in its wire form.
var ErrMalformed = errors.New("sessionid: not a session id in its wire form")

// Wire is the wire form of id.
func Wire(id uuid.UUID) string { return prefix + base64.RawURLEncoding.EncodeToString(id[:]) }

// Parse reverses Wire.
func Parse(s string) (uuid.UUID, error) {
	if !strings.HasPrefix(s, prefix) {
		return uuid.UUID{}, ErrMalformed
	}
	raw, err := base64.RawURLEncoding.DecodeString(strings.TrimPrefix(s, prefix))
	if err != nil || len(raw) != 16 {
		return uuid.UUID{}, ErrMalformed
	}
	return uuid.UUID(raw), nil
}
