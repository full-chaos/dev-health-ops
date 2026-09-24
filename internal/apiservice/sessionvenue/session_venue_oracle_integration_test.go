//go:build integration

package sessionvenue_test

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	valkeygo "github.com/valkey-io/valkey-go"

	"github.com/full-chaos/dev-health-ops/internal/api/oauthprovider"
	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"
	"github.com/full-chaos/dev-health-ops/internal/apiservice"
	"github.com/full-chaos/dev-health-ops/internal/auth/edgetoken"
	"github.com/full-chaos/dev-health-ops/internal/platform/config"
	chclickhouse "github.com/full-chaos/dev-health-ops/internal/storage/clickhouse"
	"github.com/full-chaos/dev-health-ops/internal/storage/valkey"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/sessionscenario"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/venueoracle"
)

// TestSessionVenueOracle is the session routes' venue differential: the
// REAL Python api (TestClient over dev_health_ops.api.main:app) and the
// REAL dho api route set run internal/testsupport/sessionscenario against
// two copies of one seeded database, each plane carrying its own tokens;
// answers are compared byte for byte (tokens by their claims), then the
// users, login_attempts, refresh_tokens and audit_logs rows. The Python
// answers are also the committed golden internal/api/session replays.
func TestSessionVenueOracle(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	defer cancel()
	provider := &sessionscenario.FakeProvider{}
	fake := httptest.NewServer(provider)
	t.Cleanup(fake.Close)
	venue := venueoracle.Start(t, ctx, venueoracle.Options{
		Root: venueRoot(), JWTKey: sessionscenario.Key, Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
		PythonEnv: append([]string{"VENUE_OAUTH_PROVIDER_BASE_URL=" + fake.URL}, sessionscenario.SocialEnv...),
		Seed: func(t *testing.T, ctx context.Context, admin *pgxpool.Pool, venue *venueoracle.Venue) map[string]map[string]any {
			// The password hash is made by Python's own bcrypt, a real
			// cross-language artefact.
			raw := venue.CallPython(t, venueoracle.PythonCall{Target: "dev_health_ops.api.services.users:_hash_password",
				Args: []any{sessionscenario.Password}})
			value, err := pyjson.Decode(raw[0])
			hash, ok := value.(string)
			if err != nil || !ok {
				t.Fatalf("password hash: %v %s", err, raw[0])
			}
			return sessionscenario.Seed(t, ctx, admin, hash)
		},
	})
	for _, entry := range sessionscenario.SocialEnv {
		key, value, _ := strings.Cut(entry, "=")
		t.Setenv(key, value)
	}
	t.Setenv("SOCIAL_GOOGLE_CLIENT_SECRET", "")
	for _, database := range []string{venue.PythonClickHouseDB, venue.GoClickHouseDB} {
		conn, err := chclickhouse.Open(ctx, chclickhouse.DefaultConfig(venue.AdminClickHouseURI(t, database)))
		if err != nil {
			t.Fatal(err)
		}
		for _, statement := range sessionscenario.MetricsStatements {
			if err := conn.Exec(ctx, statement); err != nil {
				t.Fatalf("seed metrics: %v", err)
			}
		}
		_ = conn.Close()
	}
	receipt := sessionscenario.Run(&sessionscenario.Harness{
		T: t, GoBase: startSessionGoAPI(t, ctx, venue, fake.URL), Tokens: venue.Tokens,
		Python: func(requests []venueoracle.Request) []venueoracle.Response { return venue.ServePython(t, requests) },
		Exec: func(sql string) {
			for _, database := range []string{venue.SourceDB, venue.GoDB} {
				execAdmin(t, ctx, venue.AdminURI(t, database), sql)
			}
		},
		// Both planes' limits live in their own Valkey.
		Reset: func() { flushValkey(t, ctx, venue.ValkeyURI, venue.PythonValkeyURI) },
		Rows: func(query string) (string, string) {
			return venueoracle.TableRows(t, ctx, venue.AdminURI(t, venue.SourceDB), query),
				venueoracle.TableRows(t, ctx, venue.AdminURI(t, venue.GoDB), query)
		},
	})
	receipt += fmt.Sprintf("fake provider calls (both planes): %d\n", provider.Calls())
	venueoracle.WriteProof(t)
	if path := os.Getenv("DEV_HEALTH_VENUE_RECEIPT"); path != "" {
		_ = os.WriteFile(path, []byte(receipt), 0o600)
	}
	t.Log("\n" + receipt)
}

func execAdmin(t *testing.T, ctx context.Context, uri, sql string) {
	t.Helper()
	pool, err := pgxpool.New(ctx, uri)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	if _, err := pool.Exec(ctx, sql); err != nil {
		t.Fatalf("%s: %v", sql, err)
	}
}

func flushValkey(t *testing.T, ctx context.Context, uris ...string) {
	t.Helper()
	for _, uri := range uris {
		options, err := valkeygo.ParseURL(uri)
		if err != nil {
			t.Fatal(err)
		}
		client, err := valkeygo.NewClient(options)
		if err != nil {
			t.Fatal(err)
		}
		if err := client.Do(ctx, client.B().Flushdb().Build()).Error(); err != nil {
			t.Fatal(err)
		}
		client.Close()
	}
}

// startSessionGoAPI builds the dho api route set against the venue's Go
// copy as the api role, with the social-login providers pointed at the
// fake provider. Its rate limits count in the venue's Go Valkey.
func startSessionGoAPI(t *testing.T, ctx context.Context, venue *venueoracle.Venue, providerURL string) string {
	t.Helper()
	pool, err := pgxpool.New(ctx, venue.GoAPIDatabaseURI(t))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	if os.Getenv("DEV_HEALTH_VENUE_GO_LOG") != "" {
		logger = slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelDebug}))
	}
	verifier, err := edgetoken.New(sessionscenario.Key, "dev-health-ops", "dev-health-api")
	if err != nil {
		t.Fatal(err)
	}
	signer, err := edgetoken.NewSigner(sessionscenario.Key, "dev-health-ops", "dev-health-api")
	if err != nil {
		t.Fatal(err)
	}
	auth, err := policy.NewAuthenticator(verifier, policy.PGStore{Pool: pool}, logger)
	if err != nil {
		t.Fatal(err)
	}
	valkeyClient, err := valkey.Open(ctx, valkey.DefaultConfig(venue.ValkeyURI))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(valkeyClient.Close)
	ch, err := chclickhouse.Open(ctx, chclickhouse.DefaultConfig(venue.GoAPIClickHouseURI(t)))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ch.Close() })
	cfg, err := config.Load(config.Spec{Service: config.APIServiceName, LookupEnv: func(string) (string, bool) { return "", false }})
	if err != nil {
		t.Fatal(err)
	}
	oauth := oauthprovider.NewClient()
	oauth.Endpoints = sessionscenario.ProviderEndpoints(providerURL)
	deps := apiservice.Deps{Pool: pool, Valkey: valkeyClient, ClickHouse: ch, Auth: auth, Guard: policy.NewGuard(auth, logger),
		Verifier: verifier, Signer: signer, SessionOAuth: oauth}
	scope := policy.NewScope(auth, logger)
	server, err := apiservice.NewServer(cfg, logger, apiservice.Routes(deps, logger), scope.OrgScope, scope.Impersonation)
	if err != nil {
		t.Fatal(err)
	}
	ts := httptest.NewServer(server.Handler())
	t.Cleanup(ts.Close)
	return ts.URL
}

// venueRoot is the repository root, where the venue finds the Python api.
func venueRoot() string {
	_, file, _, _ := runtime.Caller(0)
	return filepath.Clean(filepath.Join(filepath.Dir(file), "..", "..", ".."))
}
