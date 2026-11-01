package apiservice

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
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-go/authverify"

	"github.com/full-chaos/dev-health-ops/internal/auth/accessverify"
	"github.com/full-chaos/dev-health-ops/internal/auth/acrmint"
	"github.com/full-chaos/dev-health-ops/internal/auth/contracts"
	"github.com/full-chaos/dev-health-ops/internal/token"
)

// acrFileSigner is the production signer type over a fresh one-key set: the
// signer kind auth-service gives the ACR mint.
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

// TestAnACRMintedTokenIsRefusedAtGoAPIBeforeAnyRead is the O1 go-api cell fed
// by the real producer (CHAOS-12250): a token from acrmint, signed by a key
// go-api's verifier trusts and with go-api's issuer, is the generic 401 at the
// api listener and reads nothing. Positive controls: a platform token over the
// same key set is served after reads, and the minted token is a valid
// access-token.v1 document.
//
// The sid of a minted ACR token is an OAuth client session's, and this cell is what
// lets internal/api/policy's sessionStateQuery read a row's own revoked_at: a client
// session's sid is refused here with zero reads, and no platform minter writes one.
// If a platform minter ever writes a client session's sid, that query changes first.
func TestAnACRMintedTokenIsRefusedAtGoAPIBeforeAnyRead(t *testing.T) {
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
	control := newEdDSAMinter(t)
	keys := authverify.StaticKeys{"probe-kid": control.keys["probe-kid"], set.Active().KeyID: set.Active().PublicKey}
	minter, err := acrmint.New(acrmint.Config{Signer: signer, IssuerURL: probeIssuer, Impersonation: noImpersonation{}, Now: func() time.Time { return probeNow }})
	if err != nil {
		t.Fatal(err)
	}
	for _, audience := range []string{contracts.ACRAPIAudience, contracts.ACRMCPAudience} {
		t.Run(audience, func(t *testing.T) {
			store := &acrReadCounter{probeStore: newProbeStore()}
			f := newRoutedFixture(t, store, userRows(t), probeIssuer, keys, nil, probeNow)
			if got := probeDo(f.handler, probeRequest{"control", http.MethodGet, probeAuthenticated, bearer(control.mint(probeUser, nil))}); got.Status != http.StatusOK || store.reads == 0 {
				t.Fatalf("positive control: %d %s with %d reads", got.Status, got.Body, store.reads)
			}
			minted, _, err := minter.Mint(ctx, acrmint.Request{Audience: audience, PrincipalID: probeUser, SessionID: probeLiveSID, OrganizationID: probeOrg})
			if err != nil {
				t.Fatal(err)
			}
			if _, err := contracts.ParseAccessToken(root, compactDocument(t, minted)); err != nil {
				t.Fatalf("the minted %s token is not a valid access-token.v1 document: %v", audience, err)
			}
			store.reads = 0
			for _, path := range []string{probeAuthenticated, probeAdmin} {
				got := probeDo(f.handler, probeRequest{audience, http.MethodGet, path, bearer(minted)})
				if got.Status != http.StatusUnauthorized || got.Body != genericRefusal || store.reads != 0 {
					t.Fatalf("GET %s with the minted %s token: %d %s with %d reads; want the generic 401 and no read", path, audience, got.Status, got.Body, store.reads)
				}
			}
			counters := f.counters(t)
			for _, path := range []string{probeAuthenticated, probeAdmin} {
				series := accessverify.RefusedMetric + "|profile=GET " + path + ",reason=" + acrRefusalReason(t) + ",service=" + goAPIAccessService
				if counters[series] == 0 {
					t.Errorf("no %s", series)
				}
			}
		})
	}
}
