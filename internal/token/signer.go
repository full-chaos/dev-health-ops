package token

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"encoding/base64"
	"encoding/json"
	"errors"
	"regexp"

	"github.com/full-chaos/dev-health-ops/internal/auth/keystore"
)

// typPattern is the JOSE `typ` syntax the endpoint profile pins
// (endpoint-auth-profile.v1 token.token_type).
var typPattern = regexp.MustCompile(`^[a-z][a-z0-9+._-]{0,31}$`)

// signAttempts bounds the retries when a rotation lands between reading the
// set and signing with its active key.
const signAttempts = 3

// probeMessage is what the readiness check signs.
var probeMessage = []byte("dev-health auth-service signing probe")

// Signer signs compact JWS tokens (EdDSA) with custody's active key.
type Signer struct {
	custody keystore.Custody
}

// FileConfig selects file custody: a key-set manifest, or one key file and its
// kid (an active one-key set). authconfig makes the two forms exclusive.
type FileConfig struct {
	KeySetPath string
	KeyPath    string
	KeyID      string
}

// NewFileSigner returns a Signer over file custody.
func NewFileSigner(config FileConfig) *Signer {
	if config.KeySetPath != "" {
		return &Signer{custody: keystore.NewFileCustody(config.KeySetPath)}
	}
	return &Signer{custody: keystore.FileSource{Path: config.KeyPath, KeyID: config.KeyID}}
}

// KeySet reads and validates the current key set.
func (s *Signer) KeySet(ctx context.Context) (KeySet, error) {
	return LoadKeySet(ctx, s.custody)
}

type header struct {
	Algorithm string `json:"alg"`
	KeyID     string `json:"kid"`
	Type      string `json:"typ"`
}

// Sign returns the compact JWS of payload, a JSON object, with the protected
// header {"alg":"EdDSA","kid":<active kid>,"typ":typ}, and the kid it names.
func (s *Signer) Sign(ctx context.Context, typ string, payload []byte) (string, string, error) {
	if !typPattern.MatchString(typ) || !isJSONObject(payload) {
		return "", "", failure(ReasonInvalidInput)
	}
	var lastErr error
	for range signAttempts {
		set, err := s.KeySet(ctx)
		if err != nil {
			return "", "", err
		}
		if afterSnapshot != nil {
			afterSnapshot()
		}
		kid := set.Active().KeyID
		encodedHeader, err := json.Marshal(header{Algorithm: keystore.Algorithm, KeyID: kid, Type: typ})
		if err != nil {
			return "", "", err
		}
		input := encode(encodedHeader) + "." + encode(payload)
		signature, err := s.custody.Sign(ctx, kid, []byte(input))
		if err == nil {
			return input + "." + encode(signature), kid, nil
		}
		if !rotatedAway(err) {
			return "", "", err
		}
		lastErr = err
	}
	return "", "", lastErr
}

// Check proves the capability end to end: the set is valid, and the active key
// signs a probe that verifies under its published public key.
func (s *Signer) Check(ctx context.Context) error {
	var lastErr error
	for range signAttempts {
		set, err := s.KeySet(ctx)
		if err != nil {
			return err
		}
		if afterSnapshot != nil {
			afterSnapshot()
		}
		active := set.Active()
		signature, err := s.custody.Sign(ctx, active.KeyID, probeMessage)
		if err == nil {
			if !ed25519.Verify(active.PublicKey, probeMessage, signature) {
				return failure(ReasonProbeMismatch)
			}
			return nil
		}
		if !rotatedAway(err) {
			return err
		}
		lastErr = err
	}
	return lastErr
}

// afterSnapshot is a test hook run after the key set is read and before
// custody signs.
var afterSnapshot func()

// rotatedAway reports a sign refused because the kid stopped being active
// between reading the set and signing.
func rotatedAway(err error) bool {
	var custody *keystore.Error
	return errors.As(err, &custody) && custody.Reason == keystore.ReasonKeyNotActive
}

func encode(raw []byte) string { return base64.RawURLEncoding.EncodeToString(raw) }

func isJSONObject(payload []byte) bool {
	trimmed := bytes.TrimSpace(payload)
	if len(trimmed) == 0 || trimmed[0] != '{' || !json.Valid(trimmed) {
		return false
	}
	var object map[string]json.RawMessage
	return json.Unmarshal(trimmed, &object) == nil
}
