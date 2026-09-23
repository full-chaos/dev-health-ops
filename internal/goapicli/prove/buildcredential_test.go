package prove

import (
	"context"
	"crypto/ed25519"
	"crypto/x509"
	"encoding/pem"
	"net/http"
	"strings"
	"testing"

	"github.com/golang-jwt/jwt/v5"

	"github.com/full-chaos/dev-health-ops/internal/envelopemint"
)

// TestCredentials_DefaultMintCredentialReachesTheRealEnvelopeMinter
// exercises the DEFAULT value of mintCredential -- every other test in
// this package substitutes it (withFakeMinter), which proves the seam
// works but never proves the seam's own default actually reaches
// goapiproof.MintViaAllowlistedHelper -> internal/mintcli/envelope.Mint.
// Mirrors internal/goapicli/restprove/buildcredential_test.go's own real
// end-to-end proof for the identical wiring.
//
// This test does NOT call withFakeMinter, so it fails if credentials()
// or mintCredential's default assignment is ever changed to bypass the
// real minter.
func TestCredentials_DefaultMintCredentialReachesTheRealEnvelopeMinter(t *testing.T) {
	pub, priv, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}
	der, err := x509.MarshalPKCS8PrivateKey(priv)
	if err != nil {
		t.Fatalf("marshal pkcs8: %v", err)
	}
	pemText := string(pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: der}))
	t.Setenv(envelopemint.PrivateKeyEnvVar, pemText)

	_, proof, err := credentials(flags{orgID: "70d529e0"})
	if err != nil {
		t.Fatalf("credentials: %v", err)
	}

	req, err := http.NewRequest(http.MethodGet, "http://example.invalid/buildinfo", nil)
	if err != nil {
		t.Fatalf("NewRequest: %v", err)
	}
	if err := proof.Apply(context.Background(), req); err != nil {
		t.Fatalf("Apply (mint failed): %v", err)
	}

	header := req.Header.Get("Authorization")
	token := strings.TrimPrefix(header, "Bearer ")
	if token == header || strings.Count(token, ".") != 2 {
		t.Fatalf("Authorization header = %q, want \"Bearer <3-segment JWT>\"", header)
	}

	claims := &envelopemint.Claims{}
	if _, err := jwt.ParseWithClaims(token, claims, func(*jwt.Token) (any, error) {
		return pub, nil
	}, jwt.WithValidMethods([]string{"EdDSA"}), jwt.WithExpirationRequired()); err != nil {
		t.Fatalf("the minted envelope did not verify against its own public key: %v", err)
	}
	if claims.OrgID != "70d529e0" {
		t.Errorf("OrgID = %q, want 70d529e0", claims.OrgID)
	}
}
