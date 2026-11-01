package authruntime

import (
	"crypto/ed25519"
	"encoding/json"
	"io"
	"log/slog"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/auth/authapi"
	"github.com/full-chaos/dev-health-ops/internal/auth/authconfig"
	"github.com/full-chaos/dev-health-ops/internal/auth/session"
	"github.com/full-chaos/dev-health-ops/internal/storage/postgres/authschema"
	"github.com/full-chaos/dev-health-ops/internal/token"
)

func authorizeTestDeps(t *testing.T) (*pgxpool.Pool, authschema.ValidatedIdentifier, keySetSigner) {
	t.Helper()
	pool, err := pgxpool.New(t.Context(), "postgres://nobody@127.0.0.1:1/none")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	schema, err := authschema.NewValidatedIdentifier("auth")
	if err != nil {
		t.Fatal(err)
	}
	public, _, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatal(err)
	}
	set, err := token.NewKeySet([]token.Key{{KeyID: "k1", State: token.State("active"), Algorithm: token.Algorithm, PublicKey: public}})
	if err != nil {
		t.Fatal(err)
	}
	return pool, schema, keySetSigner{set: set}
}

func withAuthorizeRows(t *testing.T, raw []byte) {
	t.Helper()
	saved := authorizeProfileRows
	authorizeProfileRows = raw
	t.Cleanup(func() { authorizeProfileRows = saved })
}

// The embedded document has no authorize row yet: the routes stay dark.
func TestTheAuthorizeRoutesAreDarkWithoutTheirRows(t *testing.T) {
	pool, schema, signer := authorizeTestDeps(t)
	cfg := authconfig.Config{TokenIssuerURL: "https://auth.example.com", AuthorizeCallerSubjects: []string{session.WirePrincipalID(uuid.New())}}
	routes, err := authorizeRoutes(cfg, slog.New(slog.NewTextHandler(io.Discard, nil)), pool, schema, signer)
	if err != nil || len(routes) != 0 {
		t.Fatalf("routes %d, %v; want 0 without rows", len(routes), err)
	}
}

func TestTheAuthorizeRoutesMountOnTheirRows(t *testing.T) {
	pool, schema, signer := authorizeTestDeps(t)
	raw, err := json.Marshal(map[string]any{"rows": []map[string]any{
		{"service": authapi.AccessService, "method": "POST", "route": authapi.PatternAuthorize, "accepted_credential_classes": []string{"workload_token"}},
		{"service": authapi.AccessService, "method": "POST", "route": authapi.PatternAuthorizeBatch, "accepted_credential_classes": []string{"workload_token"}},
	}})
	if err != nil {
		t.Fatal(err)
	}
	withAuthorizeRows(t, raw)
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	for name, cfg := range map[string]authconfig.Config{
		"configured":       {TokenIssuerURL: "https://auth.example.com", AuthorizeCallerSubjects: []string{session.WirePrincipalID(uuid.New())}},
		"no callers":       {TokenIssuerURL: "https://auth.example.com"},
		"no issuer at all": {},
	} {
		routes, err := authorizeRoutes(cfg, logger, pool, schema, signer)
		if err != nil || len(routes) != 2 {
			t.Fatalf("%s: routes %d, %v; want both", name, len(routes), err)
		}
	}
}

func TestTheAuthorizeCallerGateNeedsASignerThatReadsItsKeySet(t *testing.T) {
	pool, schema, _ := authorizeTestDeps(t)
	cfg := authconfig.Config{TokenIssuerURL: "https://auth.example.com", AuthorizeCallerSubjects: []string{session.WirePrincipalID(uuid.New())}}
	_, err := authorizeRoutes(cfg, slog.New(slog.NewTextHandler(io.Discard, nil)), pool, schema, noKeySet{})
	if err == nil || !strings.Contains(err.Error(), "key set") {
		t.Fatalf("a signer without a key set was not refused: %v", err)
	}
}
