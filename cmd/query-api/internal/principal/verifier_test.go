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

// mustVerifier builds a Verifier and fails the test immediately if
// construction rejects the (valid, by construction, in every existing
// test) issuer/audience pair -- see TestNewVerifier_RejectsEmptyIssuer and
// TestNewVerifier_RejectsEmptyAudience for the negative cases.
func mustVerifier(t *testing.T, jwksPath, issuer, audience string) *Verifier {
	t.Helper()
	v, err := NewVerifier(jwksPath, issuer, audience)
	if err != nil {
		t.Fatalf("NewVerifier: unexpected error: %v", err)
	}
	return v
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

	v := mustVerifier(t, jwksPath, testIssuer, testAudience)
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

	v := mustVerifier(t, jwksPath, testIssuer, testAudience)
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

	v := mustVerifier(t, jwksPath, testIssuer, testAudience)
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

	v := mustVerifier(t, jwksPath, testIssuer, testAudience)
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

	v := mustVerifier(t, jwksPath, testIssuer, testAudience)
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

	v := mustVerifier(t, jwksPath, testIssuer, testAudience)
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

	v := mustVerifier(t, jwksPath, testIssuer, testAudience)
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

	v := mustVerifier(t, jwksPath, testIssuer, testAudience)
	if _, err := v.Verify(token); err == nil {
		t.Fatal("Verify: expected error for signature from a non-published key, got nil")
	}
}

func TestNewVerifier_RejectsEmptyIssuer(t *testing.T) {
	pub, _, _ := ed25519.GenerateKey(nil)
	jwksPath := writeJWKS(t, pub, testKID)

	if _, err := NewVerifier(jwksPath, "", testAudience); err == nil {
		t.Fatal("NewVerifier: expected error for empty issuer, got nil")
	}
}

func TestNewVerifier_RejectsEmptyAudience(t *testing.T) {
	pub, _, _ := ed25519.GenerateKey(nil)
	jwksPath := writeJWKS(t, pub, testKID)

	if _, err := NewVerifier(jwksPath, testIssuer, ""); err == nil {
		t.Fatal("NewVerifier: expected error for empty audience, got nil")
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

	v := mustVerifier(t, jwksPath, testIssuer, testAudience)
	if _, err := v.Verify(token); err == nil {
		t.Fatal("Verify: expected error for alg=none, got nil")
	}
}

// --- CheckJWKS (CHAOS-4708) ------------------------------------------------
//
// Fast, no-network, no-container proof of CheckJWKS's own contract, at the
// unit level -- the end-to-end proof against a real /readyz server lives in
// cmd/query-api's query_route_readyz_jwks_integration_test.go (needs real
// ClickHouse/Postgres to reach buildQueryRoute/readinessCheck at all).
// These pin the same three states that ticket's evidence bar asks for --
// missing, malformed, and valid -- directly against the method the
// readiness check calls, independent of the HTTP plumbing around it.

func TestCheckJWKS_MissingFile_ReturnsError(t *testing.T) {
	v := mustVerifier(t, filepath.Join(t.TempDir(), "does-not-exist.json"), testIssuer, testAudience)
	if err := v.CheckJWKS(); err == nil {
		t.Fatal("CheckJWKS: expected error for a JWKS path that does not exist, got nil")
	}
}

func TestCheckJWKS_MalformedFile_ReturnsError(t *testing.T) {
	path := filepath.Join(t.TempDir(), "malformed.json")
	if err := os.WriteFile(path, []byte("not json at all"), 0o600); err != nil {
		t.Fatal(err)
	}
	v := mustVerifier(t, path, testIssuer, testAudience)
	if err := v.CheckJWKS(); err == nil {
		t.Fatal("CheckJWKS: expected error for a malformed (non-JSON) JWKS document, got nil")
	}
}

func TestCheckJWKS_EmptyKeysArray_ReturnsError(t *testing.T) {
	// Structurally valid JSON, zero keys -- a distinct malformed shape
	// from "not JSON at all": authverify.Ed25519JWKSVerifier.Keys()
	// treats an empty key set the same as a decode failure
	// (ErrInvalidJWKS), not as a vacuously "healthy" zero-key success.
	path := filepath.Join(t.TempDir(), "empty-keys.json")
	if err := os.WriteFile(path, []byte(`{"keys":[]}`), 0o600); err != nil {
		t.Fatal(err)
	}
	v := mustVerifier(t, path, testIssuer, testAudience)
	if err := v.CheckJWKS(); err == nil {
		t.Fatal("CheckJWKS: expected error for a JWKS document with zero keys, got nil")
	}
}

// TestCheckJWKS_ValidFile_ReturnsNil is the OTHER direction, in the same
// file as the three error cases above: a fix that made CheckJWKS always
// return an error (or, at the /readyz layer, always 503) would still pass
// every test above but must fail this one.
func TestCheckJWKS_ValidFile_ReturnsNil(t *testing.T) {
	pub, _, _ := ed25519.GenerateKey(nil)
	jwksPath := writeJWKS(t, pub, testKID)
	v := mustVerifier(t, jwksPath, testIssuer, testAudience)
	if err := v.CheckJWKS(); err != nil {
		t.Fatalf("CheckJWKS: unexpected error for a valid JWKS document: %v", err)
	}
}

// TestCheckJWKS_ConcatenatedDocuments_ReturnsError is codex round 1's P1
// (gpt-5.6-terra, xhigh, chaos-4708-20260901T065704), re-executed and
// CONFIRMED before this fix landed: authverify.Ed25519JWKSVerifier.Keys()
// parses with a streaming json.Decoder.Decode, which reads only the
// FIRST JSON value in a file and never checks for trailing content. A
// file holding two concatenated JWKS documents (kid "old" then kid
// "new") made Keys() return a NIL error with ONLY {"old": <key>} --
// CheckJWKS (before this test's fix) reported the instance healthy while
// a token signed with "new" would fail Verify() with "no jwks key for
// kid \"new\"". This is exactly the failure shape CHAOS-4708 exists to
// catch, at one remove: an atomically-wrong rotation write (append
// instead of replace) is indistinguishable, from Keys()'s point of view,
// from a clean single-document file containing only the stale key.
//
// RED-on-pre-fix (executed): before CheckJWKS re-read the raw file and
// required json.Valid on the whole byte slice, this test's
// `err == nil` branch was taken -- CheckJWKS returned nil for a
// two-document file, matching the reproduction above exactly.
func TestCheckJWKS_ConcatenatedDocuments_ReturnsError(t *testing.T) {
	oldPub, _, _ := ed25519.GenerateKey(nil)
	newPub, _, _ := ed25519.GenerateKey(nil)
	docOld := jwksDocBytes(t, oldPub, "old")
	docNew := jwksDocBytes(t, newPub, "new")

	path := filepath.Join(t.TempDir(), "concatenated.json")
	if err := os.WriteFile(path, append(docOld, docNew...), 0o600); err != nil {
		t.Fatal(err)
	}

	v := mustVerifier(t, path, testIssuer, testAudience)
	if err := v.CheckJWKS(); err == nil {
		t.Fatal("CheckJWKS: expected error for two concatenated JWKS documents (a rotation-appended-instead-of-replaced shape), got nil -- " +
			"this is CHAOS-4708 codex round 1's P1: readiness would report healthy while a token signed with the second document's key fails Verify()")
	}
}

// jwksDocBytes marshals a one-key JWKS document (no trailing newline) --
// a helper for TestCheckJWKS_ConcatenatedDocuments_ReturnsError, which
// needs to concatenate two such documents byte-for-byte rather than
// write one via writeJWKS.
func jwksDocBytes(t *testing.T, pub ed25519.PublicKey, kid string) []byte {
	t.Helper()
	doc := map[string]any{
		"keys": []map[string]any{
			{
				"kty": "OKP", "crv": "Ed25519",
				"x":   base64.RawURLEncoding.EncodeToString(pub),
				"kid": kid, "use": "sig", "alg": "EdDSA",
			},
		},
	}
	encoded, err := json.Marshal(doc)
	if err != nil {
		t.Fatal(err)
	}
	return encoded
}
