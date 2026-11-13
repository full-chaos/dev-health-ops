// Package signingkey loads and generates the Ed25519 signing key files of the
// edge issuer: the PKCS#8 PEM read by the edge-token signer and the key-file
// tools, and the matching JWKS. It mints no token.
package signingkey

import (
	"bytes"
	"crypto/ed25519"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"fmt"
)

const (
	// Algorithm is the JWKS key algorithm of the generated key files.
	Algorithm = "EdDSA"

	// Env var names are the ones the key Secret is already wired under.
	PrivateKeyEnvVar = "GO_API_ENVELOPE_PRIVATE_KEY"
	KeyIDEnvVar      = "GO_API_ENVELOPE_KEY_ID"
	IssuerEnvVar     = "GO_API_ENVELOPE_ISSUER"
	AudienceEnvVar   = "GO_API_ENVELOPE_AUDIENCE"

	DefaultKeyID    = "go-api-envelope-2026-08"
	DefaultIssuer   = "dev-health-ops-edge"
	DefaultAudience = "query-api"
)

// ErrInvalidPrivateKeyPEM is returned by LoadPrivateKey for any PEM that
// is not exactly one "PRIVATE KEY" block parsing as a PKCS#8 Ed25519 key.
// The underlying x509/pem error is wrapped in, but the caller's PEM BYTES
// never are -- this is signing key material.
var ErrInvalidPrivateKeyPEM = errors.New("signingkey: not a PKCS#8 Ed25519 private key PEM")

// LoadPrivateKey parses pemBytes the same way
// principal_envelope._load_private_key does on the Python side: exactly
// one PEM block, labelled "PRIVATE KEY", PKCS#8-encoded, and Ed25519.
// Anything else -- multiple blocks, a different label, a different key
// type -- is refused rather than guessed at. Errors carry a reason class only,
// never any byte of the input (label, PEM, DER or parser text).
func LoadPrivateKey(pemBytes []byte) (ed25519.PrivateKey, error) {
	block, rest := pem.Decode(pemBytes)
	if block == nil {
		return nil, ErrInvalidPrivateKeyPEM
	}
	if block.Type != "PRIVATE KEY" {
		return nil, fmt.Errorf("%w: PEM block label is not PRIVATE KEY", ErrInvalidPrivateKeyPEM)
	}
	if len(bytes.TrimSpace(rest)) != 0 {
		return nil, fmt.Errorf("%w: more than one PEM block", ErrInvalidPrivateKeyPEM)
	}
	parsed, err := x509.ParsePKCS8PrivateKey(block.Bytes)
	if err != nil {
		return nil, fmt.Errorf("%w: not a PKCS#8 key", ErrInvalidPrivateKeyPEM)
	}
	key, ok := parsed.(ed25519.PrivateKey)
	if !ok || len(key) != ed25519.PrivateKeySize {
		return nil, fmt.Errorf("%w: key is not Ed25519", ErrInvalidPrivateKeyPEM)
	}
	return key, nil
}
