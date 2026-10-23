// Package withissuer adds the EdDSA access token minted by authapi's real EdDSA issuer to the user
// credentials of package usertokens. It is separate because it imports authapi, which authapi's own
// tests (they use usertokens) must not.
package withissuer

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"os"
	"path/filepath"
	"testing"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/auth/authapi"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/usertokens"
	"github.com/full-chaos/dev-health-ops/internal/token"
)

// EdDSAIssuer is the issuer URL of the EdDSA token minted here.
const EdDSAIssuer = "https://auth.example.test"

// All is usertokens.All plus the EdDSA access token of the real issuer path, minted with a throwaway key.
func All(t testing.TB) []usertokens.Token {
	t.Helper()
	return append(usertokens.All(t), usertokens.Token{Kind: "eddsa-issuer", Value: Mint(t)})
}

// Mint mints one EdDSA access token through authapi.NewEdDSAIssuer and a file signer (never hand-built).
func Mint(t testing.TB) string {
	t.Helper()
	dir := t.TempDir()
	_, private, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	der, err := x509.MarshalPKCS8PrivateKey(private)
	if err != nil {
		t.Fatal(err)
	}
	keyPath := filepath.Join(dir, "k1.pem")
	if err := os.WriteFile(keyPath, pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: der}), 0o600); err != nil {
		t.Fatal(err)
	}
	manifest, err := json.Marshal(map[string]any{"keys": []map[string]string{{"kid": "k1", "state": "active", "file": "k1.pem"}}})
	if err != nil {
		t.Fatal(err)
	}
	manifestPath := filepath.Join(dir, "keyset.json")
	if err := os.WriteFile(manifestPath, manifest, 0o600); err != nil {
		t.Fatal(err)
	}
	signer := token.NewFileSigner(token.FileConfig{KeySetPath: manifestPath})
	issuer, err := authapi.NewEdDSAIssuer(usertokens.Secret, usertokens.LegacyIssuer, usertokens.Audience, signer, EdDSAIssuer)
	if err != nil {
		t.Fatal(err)
	}
	org := uuid.New()
	compact, err := issuer.AccessContext(context.Background(), authapi.Grant{
		PrincipalID: uuid.New(), OrganizationID: &org, SessionID: uuid.New(), Role: "admin",
		User: authapi.UserState{Email: "ada@example.com", TokenVersion: 1},
	})
	if err != nil {
		t.Fatal(err)
	}
	return compact
}
