package principal

import (
	"context"
	"crypto/ed25519"
	"crypto/x509"
	"encoding/pem"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/envelopemint"
)

// TestEnvelopemintMatchesThisVerifier proves internal/envelopemint (the
// tools pod's local envelope signer, used by cmd/mint-envelope) mints an
// envelope THIS verifier accepts -- byte-compatible, not just
// self-consistent with its own package's fixtures. envelopemint cannot
// import this package back (this directory is internal/ scoped to
// cmd/query-api), so the compatibility proof has to live here instead,
// importing envelopemint the ordinary way.
func TestEnvelopemintMatchesThisVerifier(t *testing.T) {
	pub, priv, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}
	jwksPath := writeJWKS(t, pub, testKID)

	token, err := envelopemint.MintProveEnvelope(priv, "70d529e0", envelopemint.Options{
		Issuer:   testIssuer,
		Audience: testAudience,
		KeyID:    testKID,
	})
	if err != nil {
		t.Fatalf("envelopemint.MintProveEnvelope: %v", err)
	}

	v := mustVerifier(t, jwksPath, testIssuer, testAudience)
	claims, err := v.Verify(context.Background(), token)
	if err != nil {
		t.Fatalf("Verify: an envelopemint-minted envelope was rejected: %v", err)
	}
	if claims.OrgID != "70d529e0" {
		t.Errorf("OrgID = %q, want 70d529e0", claims.OrgID)
	}
	if !claims.IsSuperuser || !claims.IsSuperuserVerified {
		t.Errorf("IsSuperuser/IsSuperuserVerified = %v/%v, want true/true", claims.IsSuperuser, claims.IsSuperuserVerified)
	}
	if claims.SchemaVersion != SupportedSchemaVersion {
		t.Errorf("SchemaVersion = %d, want %d", claims.SchemaVersion, SupportedSchemaVersion)
	}
}

// TestEnvelopemintDefaultsMatchThisVerifiersExpectations pins that
// envelopemint's DEFAULT issuer/audience/kid (used when Options is
// zero-valued, i.e. exactly how cmd/mint-envelope calls it with no env
// overrides) still verify -- catching drift between envelopemint's
// defaults and this package's/principal_envelope.py's, since a default
// silently diverging is the one class of break a caller passing explicit
// options in every other test here would never see.
func TestEnvelopemintDefaultsMatchThisVerifiersExpectations(t *testing.T) {
	pub, priv, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}
	jwksPath := writeJWKS(t, pub, envelopemint.DefaultKeyID)

	token, err := envelopemint.MintProveEnvelope(priv, "70d529e0", envelopemint.Options{})
	if err != nil {
		t.Fatalf("envelopemint.MintProveEnvelope: %v", err)
	}

	v := mustVerifier(t, jwksPath, envelopemint.DefaultIssuer, envelopemint.DefaultAudience)
	if _, err := v.Verify(context.Background(), token); err != nil {
		t.Fatalf("Verify: default-configured envelopemint envelope was rejected: %v", err)
	}
}

// TestEnvelopemintLoadPrivateKeyRoundTripsAKeyThisPackageCanUse proves the
// PEM shape envelopemint.LoadPrivateKey expects (PKCS#8, "PRIVATE KEY"
// block) is the SAME shape a key destined for GO_API_ENVELOPE_PRIVATE_KEY
// already takes -- catching a format mismatch before it reaches
// production as "the tools pod's key doesn't parse", not a verify-time
// signature failure.
func TestEnvelopemintLoadPrivateKeyRoundTripsAKeyThisPackageCanUse(t *testing.T) {
	_, priv, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}
	der, err := x509.MarshalPKCS8PrivateKey(priv)
	if err != nil {
		t.Fatalf("marshal pkcs8: %v", err)
	}
	pemBytes := pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: der})

	loaded, err := envelopemint.LoadPrivateKey(pemBytes)
	if err != nil {
		t.Fatalf("LoadPrivateKey: %v", err)
	}
	if !loaded.Equal(priv) {
		t.Fatal("LoadPrivateKey returned a different key than was encoded")
	}
}
