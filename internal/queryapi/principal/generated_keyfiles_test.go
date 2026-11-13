package principal

import (
	"context"
	"os"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/signingkey"
)

// The files `dho mint envelope-keys` writes must work with this verifier: the private key
// loads with the production LoadPrivateKey and the verifier accepts an envelope it signs,
// from the generated JWKS file.
func TestGeneratedKeyFilesVerifyAnEnvelopeSignedWithThePrivateKey(t *testing.T) {
	paths, err := signingkey.GenerateKeyFiles(t.TempDir(), signingkey.DefaultKeyID)
	if err != nil {
		t.Fatalf("GenerateKeyFiles: %v", err)
	}
	pemBytes, err := os.ReadFile(paths.Private)
	if err != nil {
		t.Fatal(err)
	}
	priv, err := signingkey.LoadPrivateKey(pemBytes)
	if err != nil {
		t.Fatalf("LoadPrivateKey: %v", err)
	}
	v := mustVerifier(t, paths.JWKS, testIssuer, testAudience)
	if err := v.CheckJWKS(); err != nil {
		t.Fatalf("CheckJWKS on the generated file: %v", err)
	}
	claims, err := v.Verify(context.Background(), signEnvelope(t, priv, signingkey.DefaultKeyID, baseClaims()))
	if err != nil {
		t.Fatalf("Verify rejected an envelope signed with the generated key: %v", err)
	}
	if claims.OrgID != "org-1" {
		t.Errorf("OrgID = %q", claims.OrgID)
	}
}
