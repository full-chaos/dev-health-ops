package identity

import (
	"errors"
	"strings"
	"unicode/utf8"

	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"
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

// NormalizeFederatedEmail is NormalizeEmail for an address a provider or IdP
// reports. It also refuses U+FFFD anywhere: encoding/json turns a lone
// surrogate escape into it, so the result is not the provider's address, and a
// real one is no mailbox of ours. Local sign-in keeps NormalizeEmail.
func NormalizeFederatedEmail(raw string) (string, error) {
	if strings.ContainsRune(raw, utf8.RuneError) {
		return "", ErrInvalidEmail
	}
	return NormalizeEmail(raw)
}

// ErrUnusableProviderEmail: an identity provider returned an address the shared email text rule refuses. The text
// names no value.
var ErrUnusableProviderEmail = errors.New("identity: the provider returned an unusable email address")

// RefuseProviderEmail is the one provider email text rule of every federated sign-in and link (go-api and
// auth-service): pyjson.ClassEmail (lone surrogate, invalid UTF-8, NUL, control characters, U+FFFD after the edge
// trim). Callers apply it before any identity lookup, so the answer never depends on which accounts exist. An empty
// address passes; the callers that need one refuse it themselves.
func RefuseProviderEmail(raw string) error {
	if pyjson.BadTextForm(pyjson.ClassEmail, raw) != pyjson.TextFormNone {
		return ErrUnusableProviderEmail
	}
	return nil
}

// NormalizeLocalRegisterEmail is NormalizeEmail for an address a new local account is created with: it also refuses
// the forms the register routes refuse (pyjson.ClassEmail: U+FFFD, control characters, a lone surrogate), so a caller
// that skips the route validator cannot store them. Sign-in keeps NormalizeEmail: a stored address must still log in.
func NormalizeLocalRegisterEmail(raw string) (string, error) {
	if pyjson.BadTextForm(pyjson.ClassEmail, raw) != pyjson.TextFormNone {
		return "", ErrInvalidEmail
	}
	return NormalizeEmail(raw)
}
