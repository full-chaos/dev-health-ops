package principal

import (
	"crypto/ed25519"
	"encoding/base64"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
)

const (
	testIssuer   = "dev-health-ops-edge"
	testAudience = "query-api"
	testKID      = "test-key-2026-08"
)

// writeJWKS writes a one-key Ed25519 JWKS document to a temp file, matching
// the shape principal_envelope.build_envelope_jwks() produces, and returns
// its path.
func writeJWKS(t *testing.T, pub ed25519.PublicKey, kid string) string {
	t.Helper()
	doc := map[string]any{
		"keys": []map[string]any{
			{
				"kty": "OKP",
				"crv": "Ed25519",
				"x":   base64.RawURLEncoding.EncodeToString(pub),
				"kid": kid,
				"use": "sig",
				"alg": "EdDSA",
			},
		},
	}
	encoded, err := json.Marshal(doc)
	if err != nil {
		t.Fatalf("marshal jwks: %v", err)
	}
	path := filepath.Join(t.TempDir(), "jwks.json")
	if err := os.WriteFile(path, encoded, 0o600); err != nil {
		t.Fatalf("write jwks: %v", err)
	}
	return path
}

func signEnvelope(t *testing.T, priv ed25519.PrivateKey, kid string, claims Claims) string {
	t.Helper()
	token := jwt.NewWithClaims(jwt.SigningMethodEdDSA, claims)
	token.Header["kid"] = kid
	signed, err := token.SignedString(priv)
	if err != nil {
		t.Fatalf("sign token: %v", err)
	}
	return signed
}

func baseClaims() Claims {
	now := time.Now()
	return Claims{
		SchemaVersion:       SupportedSchemaVersion,
		OrgID:               "org-1",
		Role:                "admin",
		IsSuperuser:         false,
		IsSuperuserVerified: false,
		Permissions:         []string{"org:write", "analytics:read"},
		TokenVersion:        3,
		Tier:                "team",
		LicensedFeatures:    []string{"ai_review"},
		ImpersonationActive: false,
		RegisteredClaims: jwt.RegisteredClaims{
			Subject:   "11111111-1111-4111-8111-111111111111",
			Issuer:    testIssuer,
			Audience:  jwt.ClaimStrings{testAudience},
			IssuedAt:  jwt.NewNumericDate(now),
			ExpiresAt: jwt.NewNumericDate(now.Add(60 * time.Second)),
		},
	}
}

func TestVerify_ValidEnvelopeRoundTrips(t *testing.T) {
	pub, priv, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}
	jwksPath := writeJWKS(t, pub, testKID)
	token := signEnvelope(t, priv, testKID, baseClaims())

	v := NewVerifier(jwksPath, testIssuer, testAudience)
	claims, err := v.Verify(token)
	if err != nil {
		t.Fatalf("Verify: unexpected error: %v", err)
	}
	if claims.UserID() != "11111111-1111-4111-8111-111111111111" {
		t.Errorf("UserID = %q", claims.UserID())
	}
	if claims.OrgID != "org-1" {
		t.Errorf("OrgID = %q", claims.OrgID)
	}
	if claims.Tier != "team" {
		t.Errorf("Tier = %q", claims.Tier)
	}
}

func TestVerify_RejectsWrongAudience(t *testing.T) {
	pub, priv, _ := ed25519.GenerateKey(nil)
	jwksPath := writeJWKS(t, pub, testKID)
	claims := baseClaims()
	claims.Audience = jwt.ClaimStrings{"some-other-service"}
	token := signEnvelope(t, priv, testKID, claims)

	v := NewVerifier(jwksPath, testIssuer, testAudience)
	if _, err := v.Verify(token); err == nil {
		t.Fatal("Verify: expected error for wrong audience, got nil")
	}
}

func TestVerify_RejectsWrongIssuer(t *testing.T) {
	pub, priv, _ := ed25519.GenerateKey(nil)
	jwksPath := writeJWKS(t, pub, testKID)
	claims := baseClaims()
	claims.Issuer = "not-the-real-edge"
	token := signEnvelope(t, priv, testKID, claims)

	v := NewVerifier(jwksPath, testIssuer, testAudience)
	if _, err := v.Verify(token); err == nil {
		t.Fatal("Verify: expected error for wrong issuer, got nil")
	}
}

func TestVerify_RejectsExpiredEnvelope(t *testing.T) {
	pub, priv, _ := ed25519.GenerateKey(nil)
	jwksPath := writeJWKS(t, pub, testKID)
	claims := baseClaims()
	past := time.Now().Add(-2 * time.Minute)
	claims.IssuedAt = jwt.NewNumericDate(past)
	claims.ExpiresAt = jwt.NewNumericDate(past.Add(30 * time.Second))
	token := signEnvelope(t, priv, testKID, claims)

	v := NewVerifier(jwksPath, testIssuer, testAudience)
	if _, err := v.Verify(token); err == nil {
		t.Fatal("Verify: expected error for expired envelope, got nil")
	}
}

func TestVerify_RejectsMissingExpiry(t *testing.T) {
	pub, priv, _ := ed25519.GenerateKey(nil)
	jwksPath := writeJWKS(t, pub, testKID)
	claims := baseClaims()
	claims.ExpiresAt = nil
	token := signEnvelope(t, priv, testKID, claims)

	v := NewVerifier(jwksPath, testIssuer, testAudience)
	if _, err := v.Verify(token); err == nil {
		t.Fatal("Verify: expected error for missing exp, got nil")
	}
}

func TestVerify_RejectsUnsupportedSchemaVersion(t *testing.T) {
	pub, priv, _ := ed25519.GenerateKey(nil)
	jwksPath := writeJWKS(t, pub, testKID)
	claims := baseClaims()
	claims.SchemaVersion = 2
	token := signEnvelope(t, priv, testKID, claims)

	v := NewVerifier(jwksPath, testIssuer, testAudience)
	_, err := v.Verify(token)
	if err == nil {
		t.Fatal("Verify: expected error for unsupported schema version, got nil")
	}
	if !errors.Is(err, ErrUnsupportedSchemaVersion) {
		t.Errorf("Verify: error = %v, want wrapping ErrUnsupportedSchemaVersion", err)
	}
}

func TestVerify_RejectsUnknownKID(t *testing.T) {
	pub, priv, _ := ed25519.GenerateKey(nil)
	jwksPath := writeJWKS(t, pub, testKID)
	token := signEnvelope(t, priv, "some-other-kid", baseClaims())

	v := NewVerifier(jwksPath, testIssuer, testAudience)
	if _, err := v.Verify(token); err == nil {
		t.Fatal("Verify: expected error for unknown kid, got nil")
	}
}

func TestVerify_RejectsTokenSignedByAnotherKey(t *testing.T) {
	// A different keypair than the one published in the JWKS -- proves
	// signature verification actually runs, not just claim-shape checks.
	pub, _, _ := ed25519.GenerateKey(nil)
	_, otherPriv, _ := ed25519.GenerateKey(nil)
	jwksPath := writeJWKS(t, pub, testKID)
	token := signEnvelope(t, otherPriv, testKID, baseClaims())

	v := NewVerifier(jwksPath, testIssuer, testAudience)
	if _, err := v.Verify(token); err == nil {
		t.Fatal("Verify: expected error for signature from a non-published key, got nil")
	}
}

func TestVerify_RejectsAlgConfusion(t *testing.T) {
	// A token asserting "none" alg (or any non-EdDSA method) must never
	// verify, regardless of what the JWKS contains.
	pub, _, _ := ed25519.GenerateKey(nil)
	jwksPath := writeJWKS(t, pub, testKID)

	claims := baseClaims()
	unsigned := jwt.NewWithClaims(jwt.SigningMethodNone, claims)
	unsigned.Header["kid"] = testKID
	token, err := unsigned.SignedString(jwt.UnsafeAllowNoneSignatureType)
	if err != nil {
		t.Fatalf("sign none-alg token: %v", err)
	}

	v := NewVerifier(jwksPath, testIssuer, testAudience)
	if _, err := v.Verify(token); err == nil {
		t.Fatal("Verify: expected error for alg=none, got nil")
	}
}
