// Package envelopemint mints an effective-principal envelope in Go, byte-
// compatible with the Python edge's issuer
// (graphql/principal_envelope.py:issue_effective_principal_envelope) and
// verifiable by cmd/query-api/internal/principal.Verifier unchanged.
//
// This exists so a caller that already holds the Ed25519 signing key --
// the tools pod, via the SAME Secret key the api pod's environment reads
// (GO_API_ENVELOPE_PRIVATE_KEY) -- can mint its own envelope locally
// instead of asking a running api pod to do it. The signing key never
// leaves whatever process already holds it; this package only ever reads
// it from bytes the caller supplies.
//
// Field names, the v1 schema version, the algorithm (EdDSA/Ed25519), the
// default key id, issuer, audience and TTL all mirror
// principal_envelope.py exactly -- see Claims' doc comment for the
// field-by-field mapping. cmd/query-api/internal/principal.Claims cannot
// be imported here (it lives under an `internal/` directory scoped to
// cmd/query-api, by Go's own visibility rule); Claims below is kept
// field-for-field identical to it instead, and
// cmd/query-api/internal/principal's own test suite proves the two stay
// compatible by signing with this package and verifying with that one.
package envelopemint

import (
	"bytes"
	"crypto/ed25519"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/google/uuid"
)

const (
	// SchemaVersion is the envelope's `v` claim. Bump it, here and in
	// principal_envelope.py and cmd/query-api/internal/principal.Claims
	// together, whenever a claim is added, removed, or its meaning
	// changes -- a verifier that did not check `v` would silently accept
	// a schema it was not written against.
	SchemaVersion = 1

	// Algorithm is the only signing method this package or the verifier
	// accepts. Ed25519/EdDSA, not RS256: dev-health-go's
	// Ed25519JWKSVerifier is Ed25519-only by design (matches
	// principal_envelope.py's module doc).
	Algorithm = "EdDSA"

	// DefaultTTL matches principal_envelope.ENVELOPE_DEFAULT_TTL_SECONDS.
	// Deliberately short: this envelope is minted fresh immediately
	// before use, not held as a session credential.
	DefaultTTL = 60 * time.Second

	// Env var names match principal_envelope.py's os.getenv calls
	// exactly, so a pod already wired to hand the Python issuer these
	// values (same Secret, same keys) needs no new configuration to hand
	// this package the same values.
	PrivateKeyEnvVar = "GO_API_ENVELOPE_PRIVATE_KEY"
	KeyIDEnvVar      = "GO_API_ENVELOPE_KEY_ID"
	IssuerEnvVar     = "GO_API_ENVELOPE_ISSUER"
	AudienceEnvVar   = "GO_API_ENVELOPE_AUDIENCE"

	DefaultKeyID    = "go-api-envelope-2026-08"
	DefaultIssuer   = "dev-health-ops-edge"
	DefaultAudience = "query-api"
)

// Claims is the v1 effective-principal envelope claim schema. Field names
// and JSON tags match principal_envelope.EffectivePrincipalEnvelopeClaims
// and cmd/query-api/internal/principal.Claims exactly.
type Claims struct {
	SchemaVersion       int      `json:"v"`
	OrgID               string   `json:"org_id"`
	Role                string   `json:"role"`
	IsSuperuser         bool     `json:"is_superuser"`
	IsSuperuserVerified bool     `json:"is_superuser_verified"`
	Permissions         []string `json:"permissions"`
	TokenVersion        int      `json:"token_version"`
	Tier                string   `json:"tier"`
	LicensedFeatures    []string `json:"licensed_features"`
	ImpersonatedBy      *string  `json:"impersonated_by,omitempty"`
	ImpersonationActive bool     `json:"impersonation_active"`
	jwt.RegisteredClaims
}

// ErrInvalidPrivateKeyPEM is returned by LoadPrivateKey for any PEM that
// is not exactly one "PRIVATE KEY" block parsing as a PKCS#8 Ed25519 key.
// The underlying x509/pem error is wrapped in, but the caller's PEM BYTES
// never are -- this is signing key material.
var ErrInvalidPrivateKeyPEM = errors.New("envelopemint: not a PKCS#8 Ed25519 private key PEM")

// LoadPrivateKey parses pemBytes the same way
// principal_envelope._load_private_key does on the Python side: exactly
// one PEM block, labelled "PRIVATE KEY", PKCS#8-encoded, and Ed25519.
// Anything else -- multiple blocks, a different label, a different key
// type -- is refused rather than guessed at.
func LoadPrivateKey(pemBytes []byte) (ed25519.PrivateKey, error) {
	block, rest := pem.Decode(pemBytes)
	if block == nil {
		return nil, ErrInvalidPrivateKeyPEM
	}
	if block.Type != "PRIVATE KEY" {
		return nil, fmt.Errorf("%w: PEM block is %q, want \"PRIVATE KEY\"", ErrInvalidPrivateKeyPEM, block.Type)
	}
	if len(bytes.TrimSpace(rest)) != 0 {
		return nil, fmt.Errorf("%w: more than one PEM block", ErrInvalidPrivateKeyPEM)
	}
	parsed, err := x509.ParsePKCS8PrivateKey(block.Bytes)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrInvalidPrivateKeyPEM, err)
	}
	key, ok := parsed.(ed25519.PrivateKey)
	if !ok || len(key) != ed25519.PrivateKeySize {
		return nil, fmt.Errorf("%w: parsed key is %T, want ed25519.PrivateKey", ErrInvalidPrivateKeyPEM, parsed)
	}
	return key, nil
}

// LoadPrivateKeyFromEnv reads PrivateKeyEnvVar and parses it with
// LoadPrivateKey. Reading by env var name (never a CLI flag value) keeps
// the key off argv and out of the process table, the same posture
// go-api-prove's own -proof-bearer-secret-file doctrine already applies
// to every other credential in this codebase.
func LoadPrivateKeyFromEnv() (ed25519.PrivateKey, error) {
	raw := os.Getenv(PrivateKeyEnvVar)
	if strings.TrimSpace(raw) == "" {
		return nil, fmt.Errorf("%s is not set", PrivateKeyEnvVar)
	}
	return LoadPrivateKey([]byte(raw))
}

// Principal is the identity a minted envelope claims. It never names a
// real user row -- see ProveEnvelopePrincipal's doc comment.
type Principal struct {
	Subject             string
	OrgID               string
	Role                string
	IsSuperuser         bool
	IsSuperuserVerified bool
	Permissions         []string
	TokenVersion        int
	Tier                string
	LicensedFeatures    []string
}

// ProveEnvelopePrincipal is the fixed synthetic identity every envelope
// this package mints for go-api-prove carries. It is not a real user row:
// proving the Python/Go read paths agree for an org needs a principal
// wide enough to exercise every compared operation, not any one person's
// permission set, and query-api's own request path does not yet
// authorize on Role/Permissions/Tier/LicensedFeatures at all (Wave 0:
// "auth stays put initially" -- see this repo's go-api-wave-0 doc) --
// only IsSuperuser/IsSuperuserVerified and the org binding matter today.
// Tier is set to the widest tier and LicensedFeatures left empty rather
// than guessed at, since nothing downstream reads either yet.
func ProveEnvelopePrincipal(orgID string) Principal {
	return Principal{
		Subject:             "00000000-0000-4000-8000-000000000000",
		OrgID:               orgID,
		Role:                "owner",
		IsSuperuser:         true,
		IsSuperuserVerified: true,
		Permissions:         []string{},
		TokenVersion:        0,
		Tier:                "enterprise",
		LicensedFeatures:    []string{},
	}
}

// Options configures Mint. Issuer/Audience/KeyID/TTL default to the same
// values principal_envelope.py defaults to (env-overridable there under
// the identically-named vars) when left zero.
type Options struct {
	Issuer   string
	Audience string
	KeyID    string
	TTL      time.Duration
}

func (o Options) withDefaults() Options {
	if o.Issuer == "" {
		o.Issuer = envOr(IssuerEnvVar, DefaultIssuer)
	}
	if o.Audience == "" {
		o.Audience = envOr(AudienceEnvVar, DefaultAudience)
	}
	if o.KeyID == "" {
		o.KeyID = envOr(KeyIDEnvVar, DefaultKeyID)
	}
	if o.TTL <= 0 {
		o.TTL = DefaultTTL
	}
	return o
}

func envOr(name, fallback string) string {
	if v := os.Getenv(name); v != "" {
		return v
	}
	return fallback
}

// Mint signs and returns an effective-principal envelope for principal,
// using privateKey and opts (zero-valued fields default the same way the
// Python issuer's env vars do). The returned string is the raw JWT --
// three dot-separated base64url segments, no "Bearer " prefix.
func Mint(privateKey ed25519.PrivateKey, principal Principal, opts Options) (string, error) {
	if len(privateKey) != ed25519.PrivateKeySize {
		return "", errors.New("envelopemint: privateKey is not a valid Ed25519 private key")
	}
	if strings.TrimSpace(principal.OrgID) == "" {
		return "", errors.New("envelopemint: org id must not be empty")
	}
	opts = opts.withDefaults()
	now := time.Now()

	permissions := principal.Permissions
	if permissions == nil {
		permissions = []string{}
	}
	licensedFeatures := principal.LicensedFeatures
	if licensedFeatures == nil {
		licensedFeatures = []string{}
	}

	claims := Claims{
		SchemaVersion:       SchemaVersion,
		OrgID:               principal.OrgID,
		Role:                principal.Role,
		IsSuperuser:         principal.IsSuperuser,
		IsSuperuserVerified: principal.IsSuperuserVerified,
		Permissions:         permissions,
		TokenVersion:        principal.TokenVersion,
		Tier:                principal.Tier,
		LicensedFeatures:    licensedFeatures,
		ImpersonationActive: false,
		RegisteredClaims: jwt.RegisteredClaims{
			Subject:   principal.Subject,
			Issuer:    opts.Issuer,
			Audience:  jwt.ClaimStrings{opts.Audience},
			IssuedAt:  jwt.NewNumericDate(now),
			ExpiresAt: jwt.NewNumericDate(now.Add(opts.TTL)),
			ID:        uuid.NewString(),
		},
	}

	token := jwt.NewWithClaims(jwt.SigningMethodEdDSA, claims)
	token.Header["kid"] = opts.KeyID
	signed, err := token.SignedString(privateKey)
	if err != nil {
		return "", fmt.Errorf("envelopemint: sign: %w", err)
	}
	return signed, nil
}

// MintProveEnvelope is the entry point cmd/mint-envelope uses: mint an
// envelope for ProveEnvelopePrincipal(orgID).
func MintProveEnvelope(privateKey ed25519.PrivateKey, orgID string, opts Options) (string, error) {
	return Mint(privateKey, ProveEnvelopePrincipal(orgID), opts)
}
