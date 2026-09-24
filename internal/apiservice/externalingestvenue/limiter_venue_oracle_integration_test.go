//go:build integration

// Package externalingestvenue holds the external-ingest venue differential
// tests that need a venue of their own: the internal/apiservice venue
// package is at its 20-minute budget, so a new venue test lives in its own
// package.
package externalingestvenue

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"log/slog"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/apiservice"
	"github.com/full-chaos/dev-health-ops/internal/platform/config"
	"github.com/full-chaos/dev-health-ops/internal/storage/valkey"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/venueoracle"
)

// TestExternalIngestLimiterVenueOracle is CHAOS-6480's proof: slowapi
// buckets a route's requests by (key, exact request path) in a fixed window,
// and the real Python api and the real Go api must answer the same bursts the
// same way. Every request carries its own X-Forwarded-For (each plane trusts
// its own peer: the in-process Python client is "testclient", the Go server's
// is 127.0.0.1) so the per-IP auth-attempt throttle, a different limiter, never
// interferes with the route limiter under test.
func TestExternalIngestLimiterVenueOracle(t *testing.T) {
	ctx := context.Background()
	root := repoRoot(t)
	seed := newSeed()
	venue := venueoracle.Start(t, ctx, venueoracle.Options{
		Root:      root,
		JWTKey:    "venue-oracle-jwt-signing-key-32-bytes-min",
		PythonEnv: []string{"TRUSTED_PROXIES=testclient"},
		Seed: func(t *testing.T, ctx context.Context, admin *pgxpool.Pool, _ *venueoracle.Venue) map[string]map[string]any {
			seedVenue(t, ctx, admin, seed)
			return nil
		},
	})
	t.Setenv("TRUSTED_PROXIES", "127.0.0.1")
	pool, err := pgxpool.New(ctx, venue.GoAPIDatabaseURI(t))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	goValkey, err := valkey.Open(ctx, valkey.DefaultConfig(venue.ValkeyURI))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(goValkey.Close)
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	server, err := apiservice.NewServer(config.Config{APIAddress: "127.0.0.1:0"}, logger, apiservice.Routes(apiservice.Deps{Pool: pool, Valkey: goValkey}, logger))
	if err != nil {
		t.Fatal(err)
	}
	if err := server.Start(ctx); err != nil {
		t.Fatalf("start server: %v (api role gaps: %s)", err, venue.DiagnoseAPIRole(t, ctx))
	}
	t.Cleanup(func() { _ = server.Shutdown(ctx) })
	goBase := "http://" + server.Address()

	counter := 0
	burst := func(name, method string, count int, path func(int) string, auth bool, forwarded func(int) string) []venueoracle.Request {
		out := make([]venueoracle.Request, count)
		for index := range out {
			counter++
			headers := map[string]string{"X-Forwarded-For": forwarded(counter)}
			if auth {
				headers["Authorization"] = "Bearer " + seed.token
			}
			out[index] = venueoracle.Request{Name: fmt.Sprintf("%s #%03d", name, index+1), Method: method, Path: path(index), Headers: headers}
		}
		return out
	}
	distinctIP := func(n int) string { return fmt.Sprintf("10.%d.%d.%d", n/65536%256, n/256%256, n%256) }
	fixedIP := func(int) string { return "10.200.0.1" }
	batches := "/api/v1/external-ingest/batches/"
	var requests []venueoracle.Request
	// 125 lookups of 125 DISTINCT ids: each URL has its own budget (all 404).
	requests = append(requests, burst("get batch, distinct ids", "GET", 125, func(int) string { return batches + uuid.NewString() }, true, distinctIP)...)
	// 125 lookups of ONE id: 120 answered, then 429 (a fixed window).
	same := uuid.NewString()
	requests = append(requests, burst("get batch, one id", "GET", 125, func(int) string { return batches + same }, true, distinctIP)...)
	// The same id in another spelling is another URL, hence another budget.
	requests = append(requests, burst("get batch, same id upper-cased", "GET", 3, func(int) string { return batches + same[:8] + "-" + strings.ToUpper(same[9:]) }, true, distinctIP)...)
	// The query string is not part of the bucket: 125 lists differing only in it.
	requests = append(requests, burst("list batches, varying query", "GET", 125, func(index int) string {
		return fmt.Sprintf("/api/v1/external-ingest/batches?limit=%d", 1+index%50)
	}, true, distinctIP)...)
	// IP-keyed schema routes: one path from one IP is limited at 120; another
	// schema version from the same IP has its own budget.
	requests = append(requests, burst("get schema, one version", "GET", 125, func(int) string { return "/api/v1/external-ingest/schemas/external-ingest.v1" }, false, fixedIP)...)
	requests = append(requests, burst("get schema, other versions", "GET", 5, func(index int) string { return fmt.Sprintf("/api/v1/external-ingest/schemas/v-%d", index) }, false, fixedIP)...)
	requests = append(requests, burst("list schemas", "GET", 125, func(int) string { return "/api/v1/external-ingest/schemas" }, false, fixedIP)...)

	// The write routes (60 a minute each): a malformed body is read inside the
	// limited function in Python, so it counts against the limit and answers
	// its unhandled 500; the 61st request from one token is limited.
	post := func(name, path string) {
		out := burst(name, "POST", 65, func(int) string { return path }, true, distinctIP)
		for index := range out {
			out[index].Headers["Content-Type"] = "application/json"
			body := venueoracle.B64("{")
			out[index].Body = body
		}
		requests = append(requests, out...)
	}
	post("validate, malformed body", "/api/v1/external-ingest/validate")
	post("accept batch, malformed body", "/api/v1/external-ingest/batches")

	python := venue.ServePython(t, requests)
	receipt := venueoracle.Diff(t, goBase, requests, python, venueoracle.DiffOptions{})
	t.Logf("%d requests compared\n%s", len(requests), receipt)
}

func repoRoot(t *testing.T) string {
	t.Helper()
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("resolve package path")
	}
	return filepath.Clean(filepath.Join(filepath.Dir(file), "..", "..", ".."))
}

// seed is the one organization, customer_push source and token the bursts
// authenticate with (every scope the external-ingest routes check).
type seed struct{ orgID, sourceID, token string }

func newSeed() seed {
	return seed{orgID: uuid.New().String(), sourceID: uuid.New().String(), token: "fcpush_venue-limiter-" + uuid.New().String()}
}

func seedVenue(t *testing.T, ctx context.Context, admin *pgxpool.Pool, seed seed) {
	t.Helper()
	digest := sha256.Sum256([]byte(seed.token))
	statements := []struct {
		sql  string
		args []any
	}{
		{`INSERT INTO organizations (id, slug, name, tier) VALUES ($1::uuid, $2, $3, 'team')`,
			[]any{seed.orgID, "venue-external-ingest-limiter", "Venue External Ingest Limiter"}},
		{`INSERT INTO external_ingest_sources (id, org_id, system, instance, entity_family, mode, enabled, created_at, updated_at)
VALUES ($1::uuid, $2, 'github', 'acme/venue-repo', 'legacy', 'customer_push', true, now(), now())`,
			[]any{seed.sourceID, seed.orgID}},
		{`INSERT INTO external_ingest_tokens (id, org_id, source_id, name, token_hash, token_prefix, scopes, created_at)
VALUES ($1::uuid, $2, $3::uuid, 'venue limiter token', $4, 'fcpush_venue', $5::jsonb, now())`,
			[]any{uuid.New().String(), seed.orgID, seed.sourceID, hex.EncodeToString(digest[:]), `["schema:read","ingest:write","ingest:status"]`}},
	}
	for _, statement := range statements {
		if _, err := admin.Exec(ctx, statement.sql, statement.args...); err != nil {
			t.Fatalf("seed %s: %v", statement.sql, err)
		}
	}
}
