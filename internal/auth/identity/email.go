package identity

import (
	"errors"
	"unicode/utf8"

	"github.com/full-chaos/dev-health-ops/internal/pythonparity"
	"github.com/full-chaos/dev-health-ops/internal/pythonparity/emailvalidator"
)

// ErrInvalidEmail reports an address the login and register routes refuse.
var ErrInvalidEmail = errors.New("identity: not a valid email address")

// NormalizeEmail returns the login key of an address: the go-api register and
// login path, pydantic EmailStr validation (NFC local part, lower-cased domain,
// display-name form reduced to the address) and then Python str.lower() and
// str.strip() (internal/api/session register.go and login.go). The result is a
// fixed point: NormalizeEmail of it returns it unchanged.
func NormalizeEmail(raw string) (string, error) {
	if !utf8.ValidString(raw) {
		return "", ErrInvalidEmail
	}
	normalized, _, ok := emailvalidator.ValidateEmail([]rune(raw))
	if !ok {
		return "", ErrInvalidEmail
	}
	return pythonparity.Strip(pythonparity.Lower(string(normalized))), nil
}
