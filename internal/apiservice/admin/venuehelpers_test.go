//go:build integration

package admin_test

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/apiservice"
	"github.com/full-chaos/dev-health-ops/internal/auth/edgetoken"
	"github.com/full-chaos/dev-health-ops/internal/platform/config"
	"github.com/full-chaos/dev-health-ops/internal/storage/valkey"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/venueoracle"
)

// venueJWTIssuer/venueJWTAudience mirror config.go's unexported
// defaultJWTIssuer/defaultJWTAudience and auth.py's own JWT_ISSUER/
// JWT_AUDIENCE defaults -- both planes agree on these with neither
// JWT_ISSUER nor JWT_AUDIENCE set, which is every venue test's own env.
const (
	venueJWTIssuer   = "dev-health-ops"
	venueJWTAudience = "dev-health-api"
)

// startGoServer builds the real Go api -- Postgres pool, Valkey client,
// the protected-route runtime, every mounted route area, and the
// OrgScope/Impersonation middlewares -- against venue's Go copy, the same
// wiring apiservice's own configure() does when deps.Pool is set
// (service.go). Everything it opens is closed by t.Cleanup.
func startGoServer(t *testing.T, ctx context.Context, venue *venueoracle.Venue, jwtKey string) (base string, pool *pgxpool.Pool) {
	t.Helper()
	goPool, err := pgxpool.New(ctx, venue.GoAPIDatabaseURI(t))
	if err != nil {
		t.Fatalf("go pool: %v", err)
	}
	t.Cleanup(goPool.Close)

	verifier, err := edgetoken.New(jwtKey, venueJWTIssuer, venueJWTAudience)
	if err != nil {
		t.Fatalf("verifier: %v", err)
	}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	auth, err := policy.NewAuthenticator(verifier, policy.PGStore{Pool: goPool}, logger)
	if err != nil {
		t.Fatalf("authenticator: %v", err)
	}
	guard := policy.NewGuard(auth, logger)
	valkeyClient, err := valkey.Open(ctx, valkey.DefaultConfig(venue.ValkeyURI))
	if err != nil {
		t.Fatalf("valkey: %v", err)
	}
	t.Cleanup(valkeyClient.Close)

	cfg, err := config.Load(config.Spec{Service: config.APIServiceName, LookupEnv: func(string) (string, bool) { return "", false }})
	if err != nil {
		t.Fatalf("config.Load: %v", err)
	}
	routes := apiservice.Routes(apiservice.Deps{Pool: goPool, Valkey: valkeyClient, Auth: auth, Guard: guard}, logger)
	requestScope := policy.NewScope(auth, logger)
	server, err := apiservice.NewServer(cfg, logger, routes, requestScope.OrgScope, requestScope.Impersonation)
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}
	ts := httptest.NewServer(server.Handler())
	t.Cleanup(ts.Close)
	return ts.URL, goPool
}

// redactField blanks key's value in a JSON object body (top-level only),
// leaving every other field untouched, so Compare's equality check ignores
// a field that is legitimately wall-clock-derived and not asserted equal to
// the microsecond between the two planes. A non-JSON-object body (an empty
// 200, or a 4xx body with no such key) passes through unchanged.
func redactField(body, key string) string {
	if body == "" {
		return body
	}
	var decoded map[string]json.RawMessage
	if err := json.Unmarshal([]byte(body), &decoded); err != nil {
		return body
	}
	if _, present := decoded[key]; !present {
		return body
	}
	decoded[key] = json.RawMessage(`""`)
	encoded, err := json.Marshal(decoded)
	if err != nil {
		return body
	}
	return string(encoded)
}

// repoRoot walks up from THIS file to the directory holding src/dev_health_ops
// (this repo's Python source root), matching internal/testsupport/chschema's
// own repoRoot helper.
func repoRoot(t *testing.T) string {
	t.Helper()
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("cannot locate this test's source file")
	}
	directory := filepath.Dir(file)
	for {
		if info, statErr := os.Stat(filepath.Join(directory, "src", "dev_health_ops")); statErr == nil && info.IsDir() {
			return directory
		}
		parent := filepath.Dir(directory)
		if parent == directory {
			t.Fatalf("no src/dev_health_ops above %s", file)
		}
		directory = parent
	}
}
