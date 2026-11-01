package server

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-go/authverify"

	"github.com/full-chaos/dev-health-ops/internal/auth/accessverify"
	"github.com/full-chaos/dev-health-ops/internal/auth/acrmint"
	"github.com/full-chaos/dev-health-ops/internal/auth/contracts"
	"github.com/full-chaos/dev-health-ops/internal/token"
)

// acrFileSigner is the production signer type over a fresh one-key set.
func acrFileSigner(t *testing.T) *token.Signer {
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
	if err := os.WriteFile(filepath.Join(dir, "acr-k1.pem"), pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: der}), 0o600); err != nil {
		t.Fatal(err)
	}
	set, err := json.Marshal(map[string]any{"keys": []map[string]string{{"kid": "acr-k1", "state": "active", "file": "acr-k1.pem"}}})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "keyset.json"), set, 0o600); err != nil {
		t.Fatal(err)
	}
	return token.NewFileSigner(token.FileConfig{KeySetPath: filepath.Join(dir, "keyset.json")})
}

type noImpersonation struct{}

func (noImpersonation) ImpersonatingActor(context.Context, uuid.UUID, uuid.UUID) (bool, error) {
	return false, nil
}

// TestAnACRMintedTokenIsRefusedAtQueryAPIBeforeAnyRead is the O1 query-api
// cell fed by the real producer (CHAOS-12250): an acrmint token, signed by a
// key query-api trusts and with its issuer, is each route's 401 on the real
// listeners and reads nothing. Positive controls: a platform token over the
// same key set is served after reads; the minted token is a valid
// access-token.v1 document.
//
// The sid of a minted ACR token is an OAuth client session's, and this cell is what
// lets internal/api/policy's sessionStateQuery read a row's own revoked_at: a client
// session's sid is refused here with zero reads, and no platform minter writes one.
// If a platform minter ever writes a client session's sid, that query changes first.
func TestAnACRMintedTokenIsRefusedAtQueryAPIBeforeAnyRead(t *testing.T) {
	quietStdLog(t)
	ctx := context.Background()
	root, err := contracts.RepoRoot(".")
	if err != nil {
		t.Fatal(err)
	}
	signer := acrFileSigner(t)
	set, err := signer.KeySet(ctx)
	if err != nil {
		t.Fatal(err)
	}
	control := newAccessMinter(t)
	keys := authverify.StaticKeys{"query-kid": control.keys["query-kid"], set.Active().KeyID: set.Active().PublicKey}
	minter, err := acrmint.New(acrmint.Config{Signer: signer, IssuerURL: accessTestIssuer, Impersonation: noImpersonation{}, Now: func() time.Time { return accessTestNow }})
	if err != nil {
		t.Fatal(err)
	}
	type target struct{ listener, method, path, refusal string }
	targets := []target{
		{"query", http.MethodPost, "/query", queryRefusal},
		{"query", http.MethodPost, "/graphql", graphQLRefusal},
		{"internal", http.MethodPost, "/query/run-operation", queryRefusal},
	}
	for _, audience := range []string{contracts.ACRAPIAudience, contracts.ACRMCPAudience} {
		t.Run(audience, func(t *testing.T) {
			store := &acrQueryReads{accessStore: newAccessStore()}
			stack := newAccessStackOn(t, queryAPIUserStore{queryAPIStoreBase: store}, testAccess(keys, nil, accessTestNow), true)
			for _, target := range targets {
				store.reads = 0
				if got := stack.send(target.listener, target.method, target.path, "", control.mint(ecUser, nil, nil)); got.Status != http.StatusOK || store.reads == 0 {
					t.Fatalf("positive control %s %s: %d %s with %d reads", target.method, target.path, got.Status, got.Body, store.reads)
				}
			}
			minted, _, err := minter.Mint(ctx, acrmint.Request{Audience: audience, PrincipalID: ecUser, SessionID: accessTestSession, OrganizationID: ecOrg})
			if err != nil {
				t.Fatal(err)
			}
			if _, err := contracts.ParseAccessToken(root, acrCompactDocument(t, minted)); err != nil {
				t.Fatalf("the minted %s token is not a valid access-token.v1 document: %v", audience, err)
			}
			store.reads = 0
			for _, target := range targets {
				got := stack.send(target.listener, target.method, target.path, "", minted)
				if got.Status != http.StatusUnauthorized || got.Body != target.refusal || store.reads != 0 {
					t.Fatalf("%s %s with the minted %s token: %d %q with %d reads; want the route's 401 and no read", target.method, target.path, audience, got.Status, got.Body, store.reads)
				}
			}
			reason, refused := acrRefusalReason(t), 0
			for series := range stack.reader.counts(t) {
				if strings.HasPrefix(series, accessverify.RefusedMetric+"|") && strings.Contains(series, ",reason="+reason+",") {
					refused++
				}
			}
			if refused != len(targets) {
				t.Fatalf("refused series with reason %s: %d, want %d: %v", reason, refused, len(targets), stack.reader.counts(t))
			}
		})
	}
}
