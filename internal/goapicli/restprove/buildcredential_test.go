package restprove

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

// TestBuildCredential_MintEnvelopeMintsInProcess exercises the exact wiring
// spec S1 (CHAOS-6280) changed: buildCredential's -candidate-bearer-exec
// argv naming the "mint-envelope" allowlisted helper now reaches
// goapiproof.MintViaAllowlistedHelper -> internal/mintcli/envelope.Mint,
// entirely in process -- no subprocess, no fixed filesystem path. This is
// the REST prover's own real call path (run() builds candidateCredential
// this exact way, see run()'s own buildCredential("Authorization",
// "candidate bearer", "-candidate-bearer-exec", f.candidateBearerExec)
// call), not a hand-built stand-in for it.
func TestBuildCredential_MintEnvelopeMintsInProcess(t *testing.T) {
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

	cred, err := buildCredential("Authorization", "candidate bearer", "-candidate-bearer-exec",
		`["mint-envelope","-org","70d529e0"]`)
	if err != nil {
		t.Fatalf("buildCredential: %v", err)
	}

	req, err := http.NewRequest(http.MethodGet, "http://example.invalid/graphql", nil)
	if err != nil {
		t.Fatalf("NewRequest: %v", err)
	}
	if err := cred.Apply(context.Background(), req); err != nil {
		t.Fatalf("Apply (mint failed): %v", err)
	}

	header := req.Header.Get("Authorization")
	token := strings.TrimPrefix(header, "Bearer ")
	if token == header || strings.Count(token, ".") != 2 {
		t.Fatalf("Authorization header = %q, want \"Bearer <3-segment JWT>\"", header)
	}
	if got := cred.Mints(); got != 1 {
		t.Fatalf("Mints() = %d, want 1", got)
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
