package envelopekeys

import (
	"context"
	"crypto/ed25519"
	"os"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/envelopemint"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/principal"
)

// TestGeneratedKeyFilesVerifyAnEnvelopeMintedThroughTheRealPaths proves the
// files `dho mint envelope-keys` writes work end to end: the private key is
// read with the production LoadPrivateKey, an envelope is minted with the
// production Mint, and THIS package's production Verifier accepts it from the
// generated JWKS file.
func TestGeneratedKeyFilesVerifyAnEnvelopeMintedThroughTheRealPaths(t *testing.T) {
	dir := t.TempDir()
	paths, err := envelopemint.GenerateKeyFiles(dir, envelopemint.DefaultKeyID)
	if err != nil {
		t.Fatalf("GenerateKeyFiles: %v", err)
	}
	pemBytes, err := os.ReadFile(paths.Private)
	if err != nil {
		t.Fatal(err)
	}
	priv, err := envelopemint.LoadPrivateKey(pemBytes)
	if err != nil {
		t.Fatalf("LoadPrivateKey: %v", err)
	}
	token, err := envelopemint.MintProveEnvelope(priv, "70d529e0", envelopemint.Options{
		Issuer: envelopemint.DefaultIssuer, Audience: envelopemint.DefaultAudience, KeyID: envelopemint.DefaultKeyID,
	})
	if err != nil {
		t.Fatalf("Mint: %v", err)
	}
	v, err := principal.NewVerifier(paths.JWKS, envelopemint.DefaultIssuer, envelopemint.DefaultAudience)
	if err != nil {
		t.Fatalf("NewVerifier: %v", err)
	}
	if err := v.CheckJWKS(); err != nil {
		t.Fatalf("CheckJWKS on the generated file: %v", err)
	}
	claims, err := v.Verify(context.Background(), token)
	if err != nil {
		t.Fatalf("Verify rejected an envelope signed with the generated key: %v", err)
	}
	if claims.OrgID != "70d529e0" {
		t.Errorf("OrgID = %q", claims.OrgID)
	}
}

// TestAPlantedWrongKeyFailsVerificationAgainstTheGeneratedJWKS is the
// negative: an envelope signed by a different key, under the same key id, must
// be rejected -- otherwise the test above could pass on a verifier that
// checks nothing.
func TestAPlantedWrongKeyFailsVerificationAgainstTheGeneratedJWKS(t *testing.T) {
	paths, err := envelopemint.GenerateKeyFiles(t.TempDir(), envelopemint.DefaultKeyID)
	if err != nil {
		t.Fatal(err)
	}
	_, wrong, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatal(err)
	}
	token, err := envelopemint.MintProveEnvelope(wrong, "70d529e0", envelopemint.Options{
		Issuer: envelopemint.DefaultIssuer, Audience: envelopemint.DefaultAudience, KeyID: envelopemint.DefaultKeyID,
	})
	if err != nil {
		t.Fatal(err)
	}
	v, err := principal.NewVerifier(paths.JWKS, envelopemint.DefaultIssuer, envelopemint.DefaultAudience)
	if err != nil {
		t.Fatalf("NewVerifier: %v", err)
	}
	if _, err := v.Verify(context.Background(), token); err == nil {
		t.Fatal("Verify accepted an envelope signed by a key that is not in the generated JWKS")
	}
}
