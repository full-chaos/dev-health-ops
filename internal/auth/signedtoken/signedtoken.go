// Package signedtoken is the single-use link token the auth flows e-mail:
// org invites (services/invites.py), e-mail verification
// (services/email_verification.py) and password reset
// (services/password_reset.py). All three Python modules carry the same
// _token_secret, _sign_token, _build_token, _hash_token and
// _validate_signed_token; this package is the one Go copy of them.
//
// A token is "<uuid hex>.<hex HMAC-SHA256 of the uuid hex>". Only its SHA-256
// hex is stored, so a database read does not yield a usable link.
package signedtoken

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"strings"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/pythonparity"
)

// DevFallbackSecret is _token_secret()'s last resort when neither
// JWT_SECRET_KEY nor SETTINGS_ENCRYPTION_KEY is set.
const DevFallbackSecret = "dev-key-not-for-prod"

// ErrNonASCIISignature is hmac.compare_digest's TypeError: Python refuses to
// compare a str holding a non-ASCII character, so a token whose id parses and
// whose signature part is not ASCII fails the request (the bare 500) instead
// of being refused as invalid.
var ErrNonASCIISignature = errors.New("signedtoken: signature holds a non-ASCII character")

// Secret is _token_secret(): the first non-empty of jwtSecret and
// settingsKey (Python's `or` chain skips an empty value), else
// DevFallbackSecret.
func Secret(jwtSecret, settingsKey string) string {
	switch {
	case jwtSecret != "":
		return jwtSecret
	case settingsKey != "":
		return settingsKey
	default:
		return DevFallbackSecret
	}
}

func idHex(id uuid.UUID) string { return strings.ReplaceAll(id.String(), "-", "") }

func sign(id uuid.UUID, secret string) string {
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write([]byte(idHex(id)))
	return hex.EncodeToString(mac.Sum(nil))
}

// Hash is _hash_token: the SHA-256 hex of the token's UTF-8 bytes.
func Hash(token string) string {
	sum := sha256.Sum256([]byte(token))
	return hex.EncodeToString(sum[:])
}

// Build is _build_token plus _hash_token: the token for id and the hash that
// is stored for it.
func Build(id uuid.UUID, secret string) (token, tokenHash string) {
	token = idHex(id) + "." + sign(id, secret)
	return token, Hash(token)
}

// Valid is _validate_signed_token. The text before the first "." must be
// what uuid.UUID() accepts and the text after it must equal the signature of
// that id, compared in constant time. It reports false where Python returns
// None, and ErrNonASCIISignature where hmac.compare_digest raises.
//
// A valid token is then looked up by Hash of the WHOLE text, so an id Python
// accepts in a non-canonical spelling (braces, hyphens moved, upper case)
// passes here and simply matches no stored row, as it does in Python.
func Valid(token, secret string) (bool, error) {
	idText, signature, found := strings.Cut(token, ".")
	if !found || idText == "" || signature == "" {
		return false, nil
	}
	id, err := pythonparity.ParseUUID(idText)
	if err != nil {
		return false, nil
	}
	for i := 0; i < len(signature); i++ {
		if signature[i] >= 0x80 {
			return false, ErrNonASCIISignature
		}
	}
	return hmac.Equal([]byte(signature), []byte(sign(id, secret))), nil
}
