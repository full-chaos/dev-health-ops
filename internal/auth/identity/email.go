package identity

import "errors"

// ErrInvalidEmail reports an address the login and register routes refuse.
var ErrInvalidEmail = errors.New("identity: not a valid email address")

// NormalizeEmail returns the login key of an address. Stub: refuses every
// input.
func NormalizeEmail(raw string) (string, error) {
	return "", ErrInvalidEmail
}
