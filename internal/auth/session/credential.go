package session

import (
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"fmt"
	"strings"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/auth/contracts"
)

// A refresh credential is "dhr1_" + base64url(32 bytes), 48 characters, inside
// refresh-request.v1's alphabet. The prefix names the kind and its version so
// a scanner can find a leaked one; it carries no entropy.
const (
	credentialPrefix  = "dhr1_"
	credentialEntropy = 32
	credentialLength  = len(credentialPrefix) + 43
	successorKeyBytes = 32
)

// successorLabel separates successor derivation from any other use of the key.
const successorLabel = "dh/refresh-successor/v1"

var encoding = base64.RawURLEncoding

func encodeCredential(raw []byte) contracts.SecretString {
	return contracts.NewSecretString(credentialPrefix + encoding.EncodeToString(raw))
}

func newCredential() (contracts.SecretString, error) {
	raw := make([]byte, credentialEntropy)
	if _, err := rand.Read(raw); err != nil {
		return contracts.SecretString{}, fmt.Errorf("session: reading randomness: %w", err)
	}
	return encodeCredential(raw), nil
}

// wellFormed reports whether s has the credential shape. It is checked before
// any database read, so a malformed value costs no query.
func wellFormed(s contracts.SecretString) bool {
	v := s.Reveal()
	if len(v) != credentialLength || !strings.HasPrefix(v, credentialPrefix) {
		return false
	}
	raw, err := encoding.DecodeString(v[len(credentialPrefix):])
	return err == nil && len(raw) == credentialEntropy
}

// hashCredential is the only form a credential takes at rest.
func hashCredential(s contracts.SecretString) []byte {
	sum := sha256.Sum256([]byte(s.Reveal()))
	return sum[:]
}

// deriveSuccessor computes the credential that replaces parent.
//
// The secret is stored as a hash, so the 30 s grace can only hand a racing
// client the SAME successor if that successor can be computed again. The key
// is a server secret: the stored child id plus a stolen consumed parent is
// not enough to compute a live child.
func deriveSuccessor(key *[successorKeyBytes]byte, childID uuid.UUID, parent contracts.SecretString) contracts.SecretString {
	mac := hmac.New(sha256.New, key[:])
	mac.Write([]byte(successorLabel))
	mac.Write(childID[:])
	mac.Write([]byte(parent.Reveal()))
	return encodeCredential(mac.Sum(nil))
}

// decodeSuccessorKey reads the base64url (unpadded) form of a 32-byte key.
func decodeSuccessorKey(s contracts.SecretString) (*[successorKeyBytes]byte, error) {
	raw, err := encoding.DecodeString(s.Reveal())
	if err != nil || len(raw) != successorKeyBytes {
		return nil, fmt.Errorf("session: the successor key must be %d bytes, base64url without padding", successorKeyBytes)
	}
	var key [successorKeyBytes]byte
	copy(key[:], raw)
	return &key, nil
}

// WireSessionID is the session.v1 form of a session id: "ses_" + base64url of
// the uuid's 16 bytes.
func WireSessionID(id uuid.UUID) string { return "ses_" + encoding.EncodeToString(id[:]) }

// WirePrincipalID is the session.v1 form of a principal id.
func WirePrincipalID(id uuid.UUID) string { return "prn_" + encoding.EncodeToString(id[:]) }

// ParseWireSessionID reverses WireSessionID.
func ParseWireSessionID(s string) (uuid.UUID, error) {
	raw, err := encoding.DecodeString(strings.TrimPrefix(s, "ses_"))
	if !strings.HasPrefix(s, "ses_") || err != nil || len(raw) != 16 {
		return uuid.UUID{}, fmt.Errorf("session: %w", ErrSessionNotFound)
	}
	return uuid.UUID(raw), nil
}
