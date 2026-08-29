//go:build integration

package main

import (
	"context"
	"crypto/ed25519"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-go/clickhouse"
	"github.com/golang-jwt/jwt/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/cmd/query-api/internal/principal"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

const (
	itTestSchemaDigest = "sha256:test-schema-digest"
	itTestIssuer       = "dev-health-ops-edge"
	itTestAudience     = "query-api"
	itTestKID          = "test-key-2026-08"
)

// fakeCHClient is a minimal featureflags.QueryClient double: it always
// returns one row from the FIRST query it sees (the row query) and a
// count of 1 from every subsequent query (the count query) -- enough to
// prove the HTTP-level reachability contract this test exists for. It is
// NOT a substitute for the real-ClickHouse dual-run proof (that lives in
// the Python-side stage-2 test, ops/tests/api/graphql/test_go_api_dual_run_feature_flags.py,
// which seeds a real scratch ClickHouse via dev-hops fixtures generate).
type fakeCHClient struct{ calls int }

type fakeRows struct {
	rows [][]any
	i    int
}

func (r *fakeRows) Next() bool { return r.i < len(r.rows) }
func (r *fakeRows) Scan(dest ...any) error {
	row := r.rows[r.i]
	r.i++
	for i, d := range dest {
		switch ptr := d.(type) {
		case *string:
			*ptr = row[i].(string)
		case *time.Time:
			*ptr = row[i].(time.Time)
		case **time.Time:
			*ptr = nil
		case *uint64:
			*ptr = row[i].(uint64)
		case *uint32:
			*ptr = row[i].(uint32)
		case *float64:
			*ptr = row[i].(float64)
		case **uint64:
			// Codex review, PR #1992 round 2: complexitytimeseries.go's
			// nullable-scan fix (round 1) scans its aggregate metric
			// columns into **uint64/**uint32/**float64 destinations, but
			// this shared fake previously had no matching case -- every
			// metric silently stayed nil (Go's zero value for an
			// unpopulated *uint64 struct field) while
			// TestComplexityTimeseriesRoute_ReachableOnlyWhenSwitchEnabled
			// kept passing because it only asserted the repo label, not
			// any metric value. A nil row[i] here mirrors a real NULL
			// column value; a non-nil one allocates and populates, same
			// as ClickHouse-go's UInt64.ScanRow contract for a **uint64
			// destination (lib/column/column_gen.go).
			if row[i] == nil {
				*ptr = nil
			} else {
				v := row[i].(uint64)
				*ptr = &v
			}
		case **uint32:
			if row[i] == nil {
				*ptr = nil
			} else {
				v := row[i].(uint32)
				*ptr = &v
			}
		case **float64:
			if row[i] == nil {
				*ptr = nil
			} else {
				v := row[i].(float64)
				*ptr = &v
			}
		}
	}
	return nil
}
func (r *fakeRows) Err() error   { return nil }
func (r *fakeRows) Close() error { return nil }

// Query alternates row-query/count-query responses by call PARITY, not by
// a one-shot "first call ever" check -- Resolve makes exactly two calls
// per invocation (row query, then count query), and this fake is reused
// across MULTIPLE resolver invocations within one test function (once per
// t.Run subtest that reaches the resolver). A one-shot version of this
// fake fed a count-shaped 1-column row into a later invocation's ROW
// query and panicked on the resulting index-out-of-range Scan -- exactly
// the kind of defect a weak assertion (status-code-only) would hide, since
// gqlgen recovers a resolver panic into a normal-status GraphQL error
// response.
func (c *fakeCHClient) Query(_ context.Context, _ string, _ []clickhouse.Binding) (clickhouse.RowScanner, error) {
	c.calls++
	if c.calls%2 == 1 {
		return &fakeRows{rows: [][]any{
			{"launchdarkly", "flag-a", "proj", "boolean", time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC), nil},
		}}, nil
	}
	return &fakeRows{rows: [][]any{{uint64(1)}}}, nil
}

// fakeReviewEdgesCHClient is a minimal reviewedges.QueryClient double for
// the CHAOS-4368 Wave 2 reachability test below. Unlike fakeCHClient,
// reviewedges.Resolve issues exactly ONE query per invocation (no
// separate count query), so this fake needs no call-parity bookkeeping --
// every call gets the same single scripted row, which is enough to prove
// the HTTP-level reachability contract this test exists for. It is NOT a
// substitute for the real-ClickHouse dual-run proof (that lives in the
// Python-side stage-2 test,
// ops/tests/api/graphql/test_go_api_dual_run_review_edges.py).
type fakeReviewEdgesCHClient struct{}

func (c *fakeReviewEdgesCHClient) Query(_ context.Context, _ string, _ []clickhouse.Binding) (clickhouse.RowScanner, error) {
	return &fakeRows{rows: [][]any{
		{"reviewer@example.com", "author@example.com", uint32(3), time.Date(2026, 8, 20, 0, 0, 0, 0, time.UTC), "repo-a"},
	}}, nil
}

// fakeCognitiveLoadCHClient is a minimal cognitiveload.QueryClient double
// for the CHAOS-4369 Wave 3 reachability test below. The org-wide path
// (no teamId/repoId in cognitiveLoadVariables) issues exactly TWO
// sequential queries PER Resolve invocation (user metrics, then team
// metrics) -- this fake alternates by call PARITY, not a one-shot "first
// call ever" check, since it is reused across MULTIPLE resolver
// invocations within one test function (same discipline fakeCHClient's
// own doc comment documents, and the same bug class it exists to avoid:
// a one-shot version returns real data only for the very first Resolve
// call across the whole test, then silently empty results for every
// subsequent subtest). It is NOT a substitute for the real-ClickHouse
// dual-run proof (ops/tests/api/graphql/test_go_api_dual_run_cognitive_load.py).
type fakeCognitiveLoadCHClient struct{ calls int }

func (c *fakeCognitiveLoadCHClient) Query(_ context.Context, _ string, _ []clickhouse.Binding) (clickhouse.RowScanner, error) {
	c.calls++
	if c.calls%2 == 1 {
		return &fakeRows{rows: [][]any{
			{time.Date(2026, 8, 20, 0, 0, 0, 0, time.UTC), uint64(4), uint64(2), uint64(1)},
		}}, nil
	}
	return &fakeRows{rows: nil}, nil
}

// fakeComplexityTimeseriesCHClient is a minimal complexitytimeseries.QueryClient
// double for the CHAOS-4369 Wave 3 reachability test below. Resolve's default
// REPO-scope path issues two queries in order (the repo timeseries fetch,
// then a repo-label lookup for the repo IDs it saw) -- this fake scripts one
// row for each call in that order, enough to prove the HTTP-level
// reachability contract this test exists for. It is NOT a substitute for the
// real-ClickHouse dual-run proof (Python-side stage-2 test,
// ops/tests/api/graphql/test_go_api_dual_run_complexity_timeseries.py).
type fakeComplexityTimeseriesCHClient struct{ calls int }

func (c *fakeComplexityTimeseriesCHClient) Query(_ context.Context, _ string, _ []clickhouse.Binding) (clickhouse.RowScanner, error) {
	c.calls++
	if c.calls%2 == 1 {
		return &fakeRows{rows: [][]any{
			{time.Date(2026, 8, 20, 0, 0, 0, 0, time.UTC), "repo-a", uint64(1000), uint64(50), float64(12.5), uint64(2), uint64(1)},
		}}, nil
	}
	return &fakeRows{rows: [][]any{
		{"repo-a", "org/repo-a"},
	}}, nil
}

// fakeHotspotsCHClient is a minimal hotspots.QueryClient double for the
// CHAOS-4369 Wave 3 reachability test below. Resolve issues two queries
// in order (the hotspot fetch, then a repo-label lookup for the repo IDs
// it saw) -- this fake scripts one row for each call in that order,
// enough to prove the HTTP-level reachability contract this test exists
// for. It is NOT a substitute for the real-ClickHouse dual-run proof
// (Python-side stage-2 test,
// ops/tests/api/graphql/test_go_api_dual_run_hotspots.py).
type fakeHotspotsCHClient struct{ calls int }

func (c *fakeHotspotsCHClient) Query(_ context.Context, _ string, _ []clickhouse.Binding) (clickhouse.RowScanner, error) {
	c.calls++
	if c.calls%2 == 1 {
		return &fakeRows{rows: [][]any{
			{"repo-a", "src/main.go", uint64(500), uint32(20), uint32(30), 4.5, 0.75, 92.3},
		}}, nil
	}
	return &fakeRows{rows: [][]any{
		{"repo-a", "org/repo-a"},
	}}, nil
}

// fakeOperatingReviewCHClient is a minimal operatingreview.QueryClient
// double for the CHAOS-4352 Wave 4 Lane B (CHAOS-4505) reachability test
// below. operatingreview.Resolve issues exactly 20 queries per invocation
// (10 tables x 2 periods) -- this fake returns an empty row set for every
// call, which is enough to prove the HTTP-level reachability contract
// this test exists for (a real, computed OperatingReview payload with
// every metric "unchanged", since current == prior == all zero -- see
// operatingReviewVariables' assertion for exactly which static string
// that guarantees in the response body). It is NOT a substitute for the
// real-ClickHouse dual-run proof (Python-side stage-2 test,
// ops/tests/api/graphql/test_go_api_dual_run_operating_review.py), and
// specifically does NOT exercise fetchAIGovernance's live-execution
// requirement -- see that function's own doc comment for why the
// Go-side fix must be proven against a REAL engine, not this fake.
type fakeOperatingReviewCHClient struct{ calls int }

func (c *fakeOperatingReviewCHClient) Query(_ context.Context, _ string, _ []clickhouse.Binding) (clickhouse.RowScanner, error) {
	c.calls++
	return &fakeRows{}, nil
}

func writeTestJWKS(t *testing.T, pub ed25519.PublicKey) string {
	t.Helper()
	doc := map[string]any{
		"keys": []map[string]any{
			{
				"kty": "OKP", "crv": "Ed25519",
				"x":   base64.RawURLEncoding.EncodeToString(pub),
				"kid": itTestKID, "use": "sig", "alg": "EdDSA",
			},
		},
	}
	encoded, err := json.Marshal(doc)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "jwks.json")
	if err := os.WriteFile(path, encoded, 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

func signTestEnvelope(t *testing.T, priv ed25519.PrivateKey, orgID string) string {
	t.Helper()
	claims := principal.Claims{
		SchemaVersion: principal.SupportedSchemaVersion,
		OrgID:         orgID,
		Role:          "admin",
		RegisteredClaims: jwt.RegisteredClaims{
			Issuer:    itTestIssuer,
			Audience:  jwt.ClaimStrings{itTestAudience},
			Subject:   "user-1",
			ExpiresAt: jwt.NewNumericDate(time.Now().Add(time.Hour)),
		},
	}
	token := jwt.NewWithClaims(jwt.SigningMethodEdDSA, claims)
	token.Header["kid"] = itTestKID
	signed, err := token.SignedString(priv)
	if err != nil {
		t.Fatal(err)
	}
	return signed
}

func startTestRegistryPostgres(t *testing.T) *pgxpool.Pool {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	instance, err := containers.StartPostgres(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		closeCtx, closeCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer closeCancel()
		if err := instance.Close(closeCtx); err != nil {
			t.Errorf("terminate Postgres: %v", err)
		}
	})

	pool, err := pgxpool.New(ctx, instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)

	if _, err := pool.Exec(ctx, `
		CREATE TABLE go_api_routing_state (
			schema_digest TEXT NOT NULL,
			document_digest TEXT NOT NULL,
			selected_operation TEXT NOT NULL,
			mode TEXT NOT NULL,
			PRIMARY KEY (schema_digest, document_digest, selected_operation)
		)
	`); err != nil {
		t.Fatal(err)
	}
	return pool
}

// setRoutingMode upserts the go_api_routing_state row for one
// (schema_digest, document_digest, selected_operation) triple. operation
// is CHAOS-4368 Wave 2's generalization -- Wave 1 hardcoded
// 'featureFlags' here since it was the only operation this route ever
// mounted; a second operation (reviewEdges) now needs its own
// independently-gated row.
func setRoutingMode(t *testing.T, pool *pgxpool.Pool, documentDigest, operation, mode string) {
	t.Helper()
	if _, err := pool.Exec(context.Background(), `
		INSERT INTO go_api_routing_state (schema_digest, document_digest, selected_operation, mode)
		VALUES ($1, $2, $3, $4)
		ON CONFLICT (schema_digest, document_digest, selected_operation) DO UPDATE SET mode = $4
	`, itTestSchemaDigest, documentDigest, operation, mode); err != nil {
		t.Fatal(err)
	}
}

func postGraphQL(t *testing.T, handler http.HandlerFunc, query, bearer string) *httptest.ResponseRecorder {
	t.Helper()
	return postGraphQLWithVariables(t, handler, query, bearer, map[string]any{
		"orgId": "org-1",
		"limit": 1000,
	})
}

// postGraphQLWithVariables is postGraphQL generalized over the request's
// GraphQL variables -- CHAOS-4368 Wave 2's reviewEdges document takes a
// single `$input` object variable, a different shape than featureFlags's
// individual scalar variables, so a caller-supplied variables map is
// needed rather than postGraphQL's hardcoded featureFlags-shaped ones.
func postGraphQLWithVariables(t *testing.T, handler http.HandlerFunc, query, bearer string, variables map[string]any) *httptest.ResponseRecorder {
	t.Helper()
	body, err := json.Marshal(map[string]any{
		"query":     query,
		"variables": variables,
	})
	if err != nil {
		t.Fatal(err)
	}
	req := httptest.NewRequest(http.MethodPost, "/query", strings.NewReader(string(body)))
	req.Header.Set("Content-Type", "application/json")
	if bearer != "" {
		req.Header.Set("Authorization", "Bearer "+bearer)
	}
	rec := httptest.NewRecorder()
	handler(rec, req)
	return rec
}

// TestFeatureFlagsRoute_ReachableOnlyWhenSwitchEnabled is the CHAOS-4367
// Wave-1-specific extension of routeswitch's generic table-driven
// reachability test: it exercises the REAL HTTP handler this wave mounts
// (newQueryHandler: real Mux, real PostgresSwitch reading a real Postgres
// table, real gqlgen server, real principal.Verifier), not a fake
// handlerNamed stand-in -- proving the featureFlags route in query-api is
// live only when go_api_routing_state says so, end to end through the
// actual HTTP entry point.
func TestFeatureFlagsRoute_ReachableOnlyWhenSwitchEnabled(t *testing.T) {
	pool := startTestRegistryPostgres(t)

	pub, priv, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatal(err)
	}
	jwksPath := writeTestJWKS(t, pub)
	verifier, err := principal.NewVerifier(jwksPath, itTestIssuer, itTestAudience)
	if err != nil {
		t.Fatal(err)
	}

	handler := newQueryHandler(&fakeCHClient{}, pool, verifier, itTestSchemaDigest)
	documentDigest := digestHex(registeredFeatureFlagsDocument)
	token := signTestEnvelope(t, priv, "org-1")

	t.Run("disabled_by_default", func(t *testing.T) {
		rec := postGraphQL(t, handler, registeredFeatureFlagsDocument, token)
		if rec.Code != http.StatusNotFound {
			t.Fatalf("no routing-state row: got %d, want 404", rec.Code)
		}
	})

	t.Run("mode_python_unreachable", func(t *testing.T) {
		setRoutingMode(t, pool, documentDigest, "featureFlags", "python")
		rec := postGraphQL(t, handler, registeredFeatureFlagsDocument, token)
		if rec.Code != http.StatusNotFound {
			t.Fatalf("mode=python: got %d, want 404", rec.Code)
		}
	})

	t.Run("mode_canary_reachable_and_returns_data", func(t *testing.T) {
		setRoutingMode(t, pool, documentDigest, "featureFlags", "canary")
		rec := postGraphQL(t, handler, registeredFeatureFlagsDocument, token)
		if rec.Code != http.StatusOK {
			t.Fatalf("mode=canary: got %d, body=%s", rec.Code, rec.Body.String())
		}
		if strings.Contains(rec.Body.String(), `"errors"`) || !strings.Contains(rec.Body.String(), "flag-a") {
			t.Fatalf("expected response to contain the fake row's flag key with no errors, got %s", rec.Body.String())
		}
	})

	t.Run("unregistered_document_is_unreachable_even_when_enabled", func(t *testing.T) {
		setRoutingMode(t, pool, documentDigest, "featureFlags", "canary")
		rec := postGraphQL(t, handler, "query { __typename }", token)
		if rec.Code != http.StatusNotFound {
			t.Fatalf("unregistered document: got %d, want 404", rec.Code)
		}
	})

	t.Run("missing_bearer_token_is_unauthorized_even_when_enabled", func(t *testing.T) {
		setRoutingMode(t, pool, documentDigest, "featureFlags", "canary")
		rec := postGraphQL(t, handler, registeredFeatureFlagsDocument, "")
		if rec.Code != http.StatusUnauthorized {
			t.Fatalf("no token: got %d, want 401", rec.Code)
		}
	})

	t.Run("rollback_to_disabled_revokes_reachability_immediately", func(t *testing.T) {
		setRoutingMode(t, pool, documentDigest, "featureFlags", "canary")
		rec := postGraphQL(t, handler, registeredFeatureFlagsDocument, token)
		if rec.Code != http.StatusOK {
			t.Fatalf("expected 200 before rollback, got %d", rec.Code)
		}
		// Status-code-only would pass even for a GraphQL-level error
		// response (gqlgen returns HTTP 200 for a field error, not just a
		// successful result) -- assert the body actually carries real
		// data, not an error masquerading as a 200.
		if strings.Contains(rec.Body.String(), `"errors"`) || !strings.Contains(rec.Body.String(), "flag-a") {
			t.Fatalf("expected a real featureFlags result before rollback, got %s", rec.Body.String())
		}
		setRoutingMode(t, pool, documentDigest, "featureFlags", "disabled")
		if rec := postGraphQL(t, handler, registeredFeatureFlagsDocument, token); rec.Code != http.StatusNotFound {
			t.Fatalf("expected 404 immediately after rollback, got %d", rec.Code)
		}
	})
}

func reviewEdgesVariables() map[string]any {
	return map[string]any{
		"input": map[string]any{
			"orgId":     "org-1",
			"sinceDate": "2026-08-01",
			"untilDate": "2026-08-31",
			"limit":     500,
		},
	}
}

// TestReviewEdgesRoute_ReachableOnlyWhenSwitchEnabled is CHAOS-4368 Wave
// 2's extension of TestFeatureFlagsRoute_ReachableOnlyWhenSwitchEnabled to
// the SECOND operation this route now mounts: it exercises the same real
// HTTP handler (newQueryHandler: real Mux, real PostgresSwitch reading a
// real Postgres table, real gqlgen server, real principal.Verifier), this
// time dispatching reviewEdges, proving its reachability is gated
// independently by its OWN go_api_routing_state row.
func TestReviewEdgesRoute_ReachableOnlyWhenSwitchEnabled(t *testing.T) {
	pool := startTestRegistryPostgres(t)

	pub, priv, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatal(err)
	}
	jwksPath := writeTestJWKS(t, pub)
	verifier, err := principal.NewVerifier(jwksPath, itTestIssuer, itTestAudience)
	if err != nil {
		t.Fatal(err)
	}

	handler := newQueryHandler(&fakeReviewEdgesCHClient{}, pool, verifier, itTestSchemaDigest)
	documentDigest := digestHex(registeredReviewEdgesDocument)
	token := signTestEnvelope(t, priv, "org-1")

	t.Run("disabled_by_default", func(t *testing.T) {
		rec := postGraphQLWithVariables(t, handler, registeredReviewEdgesDocument, token, reviewEdgesVariables())
		if rec.Code != http.StatusNotFound {
			t.Fatalf("no routing-state row: got %d, want 404", rec.Code)
		}
	})

	t.Run("mode_canary_reachable_and_returns_data", func(t *testing.T) {
		setRoutingMode(t, pool, documentDigest, "reviewEdges", "canary")
		rec := postGraphQLWithVariables(t, handler, registeredReviewEdgesDocument, token, reviewEdgesVariables())
		if rec.Code != http.StatusOK {
			t.Fatalf("mode=canary: got %d, body=%s", rec.Code, rec.Body.String())
		}
		if strings.Contains(rec.Body.String(), `"errors"`) || !strings.Contains(rec.Body.String(), "reviewer@example.com") {
			t.Fatalf("expected response to contain the fake row's reviewer with no errors, got %s", rec.Body.String())
		}
	})

	t.Run("unregistered_document_is_unreachable_even_when_enabled", func(t *testing.T) {
		setRoutingMode(t, pool, documentDigest, "reviewEdges", "canary")
		rec := postGraphQLWithVariables(t, handler, "query { __typename }", token, nil)
		if rec.Code != http.StatusNotFound {
			t.Fatalf("unregistered document: got %d, want 404", rec.Code)
		}
	})

	t.Run("missing_bearer_token_is_unauthorized_even_when_enabled", func(t *testing.T) {
		setRoutingMode(t, pool, documentDigest, "reviewEdges", "canary")
		rec := postGraphQLWithVariables(t, handler, registeredReviewEdgesDocument, "", reviewEdgesVariables())
		if rec.Code != http.StatusUnauthorized {
			t.Fatalf("no token: got %d, want 401", rec.Code)
		}
	})

	// The two operations sharing one Mux/PostgresSwitch instance must NOT
	// leak reachability into each other: enabling reviewEdges must not
	// make featureFlags (a document this test never registered a routing
	// row for) reachable, and vice versa -- each go_api_routing_state row
	// is keyed by its own document_digest AND selected_operation.
	t.Run("enabling_reviewEdges_does_not_enable_featureFlags", func(t *testing.T) {
		setRoutingMode(t, pool, documentDigest, "reviewEdges", "canary")
		rec := postGraphQL(t, handler, registeredFeatureFlagsDocument, token)
		if rec.Code != http.StatusNotFound {
			t.Fatalf("featureFlags should stay unreachable when only reviewEdges is canaried: got %d", rec.Code)
		}
	})

	t.Run("rollback_to_disabled_revokes_reachability_immediately", func(t *testing.T) {
		setRoutingMode(t, pool, documentDigest, "reviewEdges", "canary")
		rec := postGraphQLWithVariables(t, handler, registeredReviewEdgesDocument, token, reviewEdgesVariables())
		if rec.Code != http.StatusOK {
			t.Fatalf("expected 200 before rollback, got %d", rec.Code)
		}
		if strings.Contains(rec.Body.String(), `"errors"`) || !strings.Contains(rec.Body.String(), "reviewer@example.com") {
			t.Fatalf("expected a real reviewEdges result before rollback, got %s", rec.Body.String())
		}
		setRoutingMode(t, pool, documentDigest, "reviewEdges", "disabled")
		if rec := postGraphQLWithVariables(t, handler, registeredReviewEdgesDocument, token, reviewEdgesVariables()); rec.Code != http.StatusNotFound {
			t.Fatalf("expected 404 immediately after rollback, got %d", rec.Code)
		}
	})
}

func cognitiveLoadVariables() map[string]any {
	return map[string]any{
		"input": map[string]any{
			"orgId":     "org-1",
			"sinceDate": "2026-08-01",
			"untilDate": "2026-08-31",
		},
	}
}

// TestCognitiveLoadRoute_ReachableOnlyWhenSwitchEnabled is CHAOS-4369 Wave
// 3's extension of TestFeatureFlagsRoute_ReachableOnlyWhenSwitchEnabled to
// the THIRD operation this route now mounts: it exercises the same real
// HTTP handler (newQueryHandler: real Mux, real PostgresSwitch reading a
// real Postgres table, real gqlgen server, real principal.Verifier), this
// time dispatching cognitiveLoad, proving its reachability is gated
// independently by its OWN go_api_routing_state row.
func TestCognitiveLoadRoute_ReachableOnlyWhenSwitchEnabled(t *testing.T) {
	pool := startTestRegistryPostgres(t)

	pub, priv, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatal(err)
	}
	jwksPath := writeTestJWKS(t, pub)
	verifier, err := principal.NewVerifier(jwksPath, itTestIssuer, itTestAudience)
	if err != nil {
		t.Fatal(err)
	}

	handler := newQueryHandler(&fakeCognitiveLoadCHClient{}, pool, verifier, itTestSchemaDigest)
	documentDigest := digestHex(registeredCognitiveLoadDocument)
	token := signTestEnvelope(t, priv, "org-1")

	t.Run("disabled_by_default", func(t *testing.T) {
		rec := postGraphQLWithVariables(t, handler, registeredCognitiveLoadDocument, token, cognitiveLoadVariables())
		if rec.Code != http.StatusNotFound {
			t.Fatalf("no routing-state row: got %d, want 404", rec.Code)
		}
	})

	t.Run("mode_canary_reachable_and_returns_data", func(t *testing.T) {
		setRoutingMode(t, pool, documentDigest, "cognitiveLoad", "canary")
		rec := postGraphQLWithVariables(t, handler, registeredCognitiveLoadDocument, token, cognitiveLoadVariables())
		if rec.Code != http.StatusOK {
			t.Fatalf("mode=canary: got %d, body=%s", rec.Code, rec.Body.String())
		}
		if strings.Contains(rec.Body.String(), `"errors"`) || !strings.Contains(rec.Body.String(), "2026-08-20") {
			t.Fatalf("expected response to contain the fake row's day with no errors, got %s", rec.Body.String())
		}
	})

	t.Run("unregistered_document_is_unreachable_even_when_enabled", func(t *testing.T) {
		setRoutingMode(t, pool, documentDigest, "cognitiveLoad", "canary")
		rec := postGraphQLWithVariables(t, handler, "query { __typename }", token, nil)
		if rec.Code != http.StatusNotFound {
			t.Fatalf("unregistered document: got %d, want 404", rec.Code)
		}
	})

	t.Run("missing_bearer_token_is_unauthorized_even_when_enabled", func(t *testing.T) {
		setRoutingMode(t, pool, documentDigest, "cognitiveLoad", "canary")
		rec := postGraphQLWithVariables(t, handler, registeredCognitiveLoadDocument, "", cognitiveLoadVariables())
		if rec.Code != http.StatusUnauthorized {
			t.Fatalf("no token: got %d, want 401", rec.Code)
		}
	})

	// The three operations sharing one Mux/PostgresSwitch instance must
	// NOT leak reachability into each other.
	t.Run("enabling_cognitiveLoad_does_not_enable_featureFlags_or_reviewEdges", func(t *testing.T) {
		setRoutingMode(t, pool, documentDigest, "cognitiveLoad", "canary")
		if rec := postGraphQL(t, handler, registeredFeatureFlagsDocument, token); rec.Code != http.StatusNotFound {
			t.Fatalf("featureFlags should stay unreachable when only cognitiveLoad is canaried: got %d", rec.Code)
		}
		if rec := postGraphQLWithVariables(t, handler, registeredReviewEdgesDocument, token, reviewEdgesVariables()); rec.Code != http.StatusNotFound {
			t.Fatalf("reviewEdges should stay unreachable when only cognitiveLoad is canaried: got %d", rec.Code)
		}
	})

	t.Run("rollback_to_disabled_revokes_reachability_immediately", func(t *testing.T) {
		setRoutingMode(t, pool, documentDigest, "cognitiveLoad", "canary")
		rec := postGraphQLWithVariables(t, handler, registeredCognitiveLoadDocument, token, cognitiveLoadVariables())
		if rec.Code != http.StatusOK {
			t.Fatalf("expected 200 before rollback, got %d", rec.Code)
		}
		if strings.Contains(rec.Body.String(), `"errors"`) || !strings.Contains(rec.Body.String(), "2026-08-20") {
			t.Fatalf("expected a real cognitiveLoad result before rollback, got %s", rec.Body.String())
		}
		setRoutingMode(t, pool, documentDigest, "cognitiveLoad", "disabled")
		if rec := postGraphQLWithVariables(t, handler, registeredCognitiveLoadDocument, token, cognitiveLoadVariables()); rec.Code != http.StatusNotFound {
			t.Fatalf("expected 404 immediately after rollback, got %d", rec.Code)
		}
	})
}

func complexityTimeseriesVariables() map[string]any {
	return map[string]any{
		"input": map[string]any{
			"orgId":       "org-1",
			"sinceUtc":    "2026-08-01T00:00:00Z",
			"untilUtc":    "2026-08-31T23:59:59Z",
			"granularity": "DAY",
			"scope":       "REPO",
			"limit":       500,
		},
	}
}

// TestComplexityTimeseriesRoute_ReachableOnlyWhenSwitchEnabled is CHAOS-4369
// Wave 3's extension of the same reachability contract to a THIRD operation
// this route now mounts: real Mux, real PostgresSwitch reading a real
// Postgres table, real gqlgen server, real principal.Verifier -- proving
// complexityTimeseries's reachability is gated independently by its OWN
// go_api_routing_state row.
func TestComplexityTimeseriesRoute_ReachableOnlyWhenSwitchEnabled(t *testing.T) {
	pool := startTestRegistryPostgres(t)

	pub, priv, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatal(err)
	}
	jwksPath := writeTestJWKS(t, pub)
	verifier, err := principal.NewVerifier(jwksPath, itTestIssuer, itTestAudience)
	if err != nil {
		t.Fatal(err)
	}

	handler := newQueryHandler(&fakeComplexityTimeseriesCHClient{}, pool, verifier, itTestSchemaDigest)
	documentDigest := digestHex(registeredComplexityTimeseriesDocument)
	token := signTestEnvelope(t, priv, "org-1")

	t.Run("disabled_by_default", func(t *testing.T) {
		rec := postGraphQLWithVariables(t, handler, registeredComplexityTimeseriesDocument, token, complexityTimeseriesVariables())
		if rec.Code != http.StatusNotFound {
			t.Fatalf("no routing-state row: got %d, want 404", rec.Code)
		}
	})

	t.Run("mode_canary_reachable_and_returns_data", func(t *testing.T) {
		setRoutingMode(t, pool, documentDigest, "complexityTimeseries", "canary")
		rec := postGraphQLWithVariables(t, handler, registeredComplexityTimeseriesDocument, token, complexityTimeseriesVariables())
		if rec.Code != http.StatusOK {
			t.Fatalf("mode=canary: got %d, body=%s", rec.Code, rec.Body.String())
		}
		// Asserts an actual metric value (not just the repo label) --
		// codex review, PR #1992 round 2: a metric-blind assertion here
		// would not have caught fakeRows.Scan silently dropping every
		// **uint64/**uint32/**float64-destined aggregate metric.
		if strings.Contains(rec.Body.String(), `"errors"`) ||
			!strings.Contains(rec.Body.String(), "org/repo-a") ||
			!strings.Contains(rec.Body.String(), `"locTotal":1000`) {
			t.Fatalf("expected response to contain the fake row's repo label and locTotal metric with no errors, got %s", rec.Body.String())
		}
	})

	t.Run("unregistered_document_is_unreachable_even_when_enabled", func(t *testing.T) {
		setRoutingMode(t, pool, documentDigest, "complexityTimeseries", "canary")
		rec := postGraphQLWithVariables(t, handler, "query { __typename }", token, nil)
		if rec.Code != http.StatusNotFound {
			t.Fatalf("unregistered document: got %d, want 404", rec.Code)
		}
	})

	t.Run("missing_bearer_token_is_unauthorized_even_when_enabled", func(t *testing.T) {
		setRoutingMode(t, pool, documentDigest, "complexityTimeseries", "canary")
		rec := postGraphQLWithVariables(t, handler, registeredComplexityTimeseriesDocument, "", complexityTimeseriesVariables())
		if rec.Code != http.StatusUnauthorized {
			t.Fatalf("no token: got %d, want 401", rec.Code)
		}
	})

	// The three operations sharing one Mux/PostgresSwitch instance must NOT
	// leak reachability into each other.
	t.Run("enabling_complexityTimeseries_does_not_enable_featureFlags", func(t *testing.T) {
		setRoutingMode(t, pool, documentDigest, "complexityTimeseries", "canary")
		rec := postGraphQL(t, handler, registeredFeatureFlagsDocument, token)
		if rec.Code != http.StatusNotFound {
			t.Fatalf("featureFlags should stay unreachable when only complexityTimeseries is canaried: got %d", rec.Code)
		}
	})

	t.Run("rollback_to_disabled_revokes_reachability_immediately", func(t *testing.T) {
		setRoutingMode(t, pool, documentDigest, "complexityTimeseries", "canary")
		rec := postGraphQLWithVariables(t, handler, registeredComplexityTimeseriesDocument, token, complexityTimeseriesVariables())
		if rec.Code != http.StatusOK {
			t.Fatalf("expected 200 before rollback, got %d", rec.Code)
		}
		if strings.Contains(rec.Body.String(), `"errors"`) ||
			!strings.Contains(rec.Body.String(), "org/repo-a") ||
			!strings.Contains(rec.Body.String(), `"locTotal":1000`) {
			t.Fatalf("expected a real complexityTimeseries result (incl. the locTotal metric) before rollback, got %s", rec.Body.String())
		}
		setRoutingMode(t, pool, documentDigest, "complexityTimeseries", "disabled")
		if rec := postGraphQLWithVariables(t, handler, registeredComplexityTimeseriesDocument, token, complexityTimeseriesVariables()); rec.Code != http.StatusNotFound {
			t.Fatalf("expected 404 immediately after rollback, got %d", rec.Code)
		}
	})
}

func hotspotsVariables() map[string]any {
	return map[string]any{
		"input": map[string]any{
			"orgId":    "org-1",
			"sinceUtc": "2026-08-01T00:00:00Z",
			"untilUtc": "2026-08-31T23:59:59Z",
			"limit":    50,
		},
	}
}

// TestHotspotsRoute_ReachableOnlyWhenSwitchEnabled is CHAOS-4369 Wave 3's
// extension of the same reachability contract to hotspots, the second
// Wave 3 operation (after complexityTimeseries): real Mux, real
// PostgresSwitch reading a real Postgres table, real gqlgen server, real
// principal.Verifier -- proving hotspots's reachability is gated
// independently by its OWN go_api_routing_state row.
func TestHotspotsRoute_ReachableOnlyWhenSwitchEnabled(t *testing.T) {
	pool := startTestRegistryPostgres(t)

	pub, priv, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatal(err)
	}
	jwksPath := writeTestJWKS(t, pub)
	verifier, err := principal.NewVerifier(jwksPath, itTestIssuer, itTestAudience)
	if err != nil {
		t.Fatal(err)
	}

	handler := newQueryHandler(&fakeHotspotsCHClient{}, pool, verifier, itTestSchemaDigest)
	documentDigest := digestHex(registeredHotspotsDocument)
	token := signTestEnvelope(t, priv, "org-1")

	t.Run("disabled_by_default", func(t *testing.T) {
		rec := postGraphQLWithVariables(t, handler, registeredHotspotsDocument, token, hotspotsVariables())
		if rec.Code != http.StatusNotFound {
			t.Fatalf("no routing-state row: got %d, want 404", rec.Code)
		}
	})

	t.Run("mode_canary_reachable_and_returns_data", func(t *testing.T) {
		setRoutingMode(t, pool, documentDigest, "hotspots", "canary")
		rec := postGraphQLWithVariables(t, handler, registeredHotspotsDocument, token, hotspotsVariables())
		if rec.Code != http.StatusOK {
			t.Fatalf("mode=canary: got %d, body=%s", rec.Code, rec.Body.String())
		}
		if strings.Contains(rec.Body.String(), `"errors"`) || !strings.Contains(rec.Body.String(), "org/repo-a") {
			t.Fatalf("expected response to contain the fake row's repo label with no errors, got %s", rec.Body.String())
		}
	})

	t.Run("unregistered_document_is_unreachable_even_when_enabled", func(t *testing.T) {
		setRoutingMode(t, pool, documentDigest, "hotspots", "canary")
		rec := postGraphQLWithVariables(t, handler, "query { __typename }", token, nil)
		if rec.Code != http.StatusNotFound {
			t.Fatalf("unregistered document: got %d, want 404", rec.Code)
		}
	})

	t.Run("missing_bearer_token_is_unauthorized_even_when_enabled", func(t *testing.T) {
		setRoutingMode(t, pool, documentDigest, "hotspots", "canary")
		rec := postGraphQLWithVariables(t, handler, registeredHotspotsDocument, "", hotspotsVariables())
		if rec.Code != http.StatusUnauthorized {
			t.Fatalf("no token: got %d, want 401", rec.Code)
		}
	})

	t.Run("enabling_hotspots_does_not_enable_featureFlags", func(t *testing.T) {
		setRoutingMode(t, pool, documentDigest, "hotspots", "canary")
		rec := postGraphQL(t, handler, registeredFeatureFlagsDocument, token)
		if rec.Code != http.StatusNotFound {
			t.Fatalf("featureFlags should stay unreachable when only hotspots is canaried: got %d", rec.Code)
		}
	})

	t.Run("rollback_to_disabled_revokes_reachability_immediately", func(t *testing.T) {
		setRoutingMode(t, pool, documentDigest, "hotspots", "canary")
		rec := postGraphQLWithVariables(t, handler, registeredHotspotsDocument, token, hotspotsVariables())
		if rec.Code != http.StatusOK {
			t.Fatalf("expected 200 before rollback, got %d", rec.Code)
		}
		if strings.Contains(rec.Body.String(), `"errors"`) || !strings.Contains(rec.Body.String(), "org/repo-a") {
			t.Fatalf("expected a real hotspots result before rollback, got %s", rec.Body.String())
		}
		setRoutingMode(t, pool, documentDigest, "hotspots", "disabled")
		if rec := postGraphQLWithVariables(t, handler, registeredHotspotsDocument, token, hotspotsVariables()); rec.Code != http.StatusNotFound {
			t.Fatalf("expected 404 immediately after rollback, got %d", rec.Code)
		}
	})
}

// TestBuildQueryRoute_FailsFastOnWrongClickHouseProtocol is the regression
// proof for a real bug codex review found (2026-08-28): CLICKHOUSE_URI
func operatingReviewVariables() map[string]any {
	return map[string]any{
		"orgId": "org-1",
		"input": map[string]any{
			"weekStart": "2026-08-24",
		},
	}
}

// TestOperatingReviewRoute_ReachableOnlyWhenSwitchEnabled is CHAOS-4352
// Wave 4 Lane B's (CHAOS-4505) extension of the same reachability
// contract to operatingReview: real Mux, real PostgresSwitch reading a
// real Postgres table, real gqlgen server, real principal.Verifier --
// proving operatingReview's reachability is gated independently by its
// OWN go_api_routing_state row. Unlike every other operation registered
// in this file, operatingReview's registered document carries BOTH
// `$orgId` and `$input` variables (matching featureFlags, not
// hotspots/cognitiveLoad/complexityTimeseries/reviewEdges) -- see
// registeredOperatingReviewDocument's own doc comment.
func TestOperatingReviewRoute_ReachableOnlyWhenSwitchEnabled(t *testing.T) {
	pool := startTestRegistryPostgres(t)

	pub, priv, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatal(err)
	}
	jwksPath := writeTestJWKS(t, pub)
	verifier, err := principal.NewVerifier(jwksPath, itTestIssuer, itTestAudience)
	if err != nil {
		t.Fatal(err)
	}

	handler := newQueryHandler(&fakeOperatingReviewCHClient{}, pool, verifier, itTestSchemaDigest)
	documentDigest := digestHex(registeredOperatingReviewDocument)
	token := signTestEnvelope(t, priv, "org-1")

	t.Run("disabled_by_default", func(t *testing.T) {
		rec := postGraphQLWithVariables(t, handler, registeredOperatingReviewDocument, token, operatingReviewVariables())
		if rec.Code != http.StatusNotFound {
			t.Fatalf("no routing-state row: got %d, want 404", rec.Code)
		}
	})

	t.Run("mode_canary_reachable_and_returns_data", func(t *testing.T) {
		setRoutingMode(t, pool, documentDigest, "operatingReview", "canary")
		rec := postGraphQLWithVariables(t, handler, registeredOperatingReviewDocument, token, operatingReviewVariables())
		if rec.Code != http.StatusOK {
			t.Fatalf("mode=canary: got %d, body=%s", rec.Code, rec.Body.String())
		}
		// The fake client returns zero rows for every one of the 20
		// queries, so current == prior == all-zero for every metric --
		// every buildMetric call hits the abs(delta)<0.000001 branch and
		// returns "unchanged" REGARDLESS of direction (checked before the
		// direction switch), so recommendations is empty and this static
		// string is guaranteed present, proving a REAL computed payload
		// came back, not just a 200.
		if strings.Contains(rec.Body.String(), `"errors"`) || !strings.Contains(rec.Body.String(), "No signals worsened this week.") {
			t.Fatalf("expected a real operatingReview result with no errors, got %s", rec.Body.String())
		}
	})

	t.Run("unregistered_document_is_unreachable_even_when_enabled", func(t *testing.T) {
		setRoutingMode(t, pool, documentDigest, "operatingReview", "canary")
		rec := postGraphQLWithVariables(t, handler, "query { __typename }", token, nil)
		if rec.Code != http.StatusNotFound {
			t.Fatalf("unregistered document: got %d, want 404", rec.Code)
		}
	})

	t.Run("missing_bearer_token_is_unauthorized_even_when_enabled", func(t *testing.T) {
		setRoutingMode(t, pool, documentDigest, "operatingReview", "canary")
		rec := postGraphQLWithVariables(t, handler, registeredOperatingReviewDocument, "", operatingReviewVariables())
		if rec.Code != http.StatusUnauthorized {
			t.Fatalf("no token: got %d, want 401", rec.Code)
		}
	})

	t.Run("enabling_operatingReview_does_not_enable_featureFlags", func(t *testing.T) {
		setRoutingMode(t, pool, documentDigest, "operatingReview", "canary")
		rec := postGraphQL(t, handler, registeredFeatureFlagsDocument, token)
		if rec.Code != http.StatusNotFound {
			t.Fatalf("featureFlags should stay unreachable when only operatingReview is canaried: got %d", rec.Code)
		}
	})

	t.Run("rollback_to_disabled_revokes_reachability_immediately", func(t *testing.T) {
		setRoutingMode(t, pool, documentDigest, "operatingReview", "canary")
		rec := postGraphQLWithVariables(t, handler, registeredOperatingReviewDocument, token, operatingReviewVariables())
		if rec.Code != http.StatusOK {
			t.Fatalf("expected 200 before rollback, got %d", rec.Code)
		}
		setRoutingMode(t, pool, documentDigest, "operatingReview", "disabled")
		if rec := postGraphQLWithVariables(t, handler, registeredOperatingReviewDocument, token, operatingReviewVariables()); rec.Code != http.StatusNotFound {
			t.Fatalf("expected 404 immediately after rollback, got %d", rec.Code)
		}
	})
}

// resolves to a DIFFERENT port for a Go process (native wire protocol)
// than for a Python process (HTTP) despite sharing the same env var name
// across this repo's deployments (deploy/go-workers/README.md, "ClickHouse:
// the Go worker needs the native port, not the HTTP port") -- pointing
// query-api's CLICKHOUSE_URI at the HTTP-shaped endpoint previously
// mounted /query successfully and only failed per-request, with a bare
// "ClickHouse query failed" that named no root cause. buildQueryRoute must
// now refuse to start at all: an HTTP server standing in for the wrong
// protocol proves this without depending on a real ClickHouse container
// being reachable in CI.
func TestBuildQueryRoute_FailsFastOnWrongClickHouseProtocol(t *testing.T) {
	notClickHouse := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer notClickHouse.Close()

	cfg := queryRouteConfig{
		ClickHouseURI:       "clickhouse://" + notClickHouse.Listener.Addr().String() + "/default",
		RegistryPostgresURI: "postgres://unused:unused@127.0.0.1:1/unused",
		EnvelopeJWKSPath:    "/dev/null",
		EnvelopeIssuer:      "issuer",
		EnvelopeAudience:    "audience",
		SchemaDigest:        "sha256:unused",
	}

	_, _, err := buildQueryRoute(cfg)
	if err == nil {
		t.Fatal("buildQueryRoute succeeded against a non-ClickHouse endpoint, want a readiness-check error")
	}
	if !strings.Contains(err.Error(), "ClickHouse readiness check failed") {
		t.Fatalf("error = %v, want it to name the ClickHouse readiness check", err)
	}
}

// fakeFlowMatrixCHClient is a minimal analytics.QueryClient double for
// the CHAOS-4506 reachability test below. ExecuteFlowMatrix issues its
// nodes and edges queries CONCURRENTLY (unlike hotspots's sequential
// pair), so this fake dispatches by inspecting the statement text rather
// than call order -- a call-order-keyed fake would be flaky under
// concurrent dispatch. Not a substitute for the real-ClickHouse dual-run
// proof (Python-side stage-2 test, not yet written for this operation).
type fakeFlowMatrixCHClient struct {
	mu sync.Mutex
}

func (c *fakeFlowMatrixCHClient) Query(_ context.Context, statement string, _ []clickhouse.Binding) (clickhouse.RowScanner, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	// CI FAILURE FOUND LIVE (2026-08-29, go-storage-integration-shard-packages-2,
	// PR #2009): both rows below were uint64(...), but queryNodes/queryEdges
	// (flowmatrix.go) scan their value column into a plain float64 -- every
	// flowMatrix value expression is wrapped in toFloat64(...) in the real SQL
	// (round 3's fix, this file's own package doc explains why: ClickHouse
	// returns UInt64 for SUM()-based measures and the driver does not convert),
	// so the REAL client never hands queryNodes/queryEdges a uint64 for this
	// column -- only this fake, gone stale relative to that fix, did. Panicked
	// with "interface conversion: interface {} is uint64, not float64" inside
	// fakeRows.Scan, only reachable via the `integration` build tag neither
	// ci/local_validate.sh (Python-only stages) nor an untagged `go test` run
	// exercises -- it survived every local check this lane ran and was caught
	// only by CI's go-storage-integration-shard-packages jobs. Test-only fix:
	// match what the real, already-fixed client actually returns.
	if strings.Contains(statement, "AS source_dimension,") {
		return &fakeRows{rows: [][]any{
			{"TEAM", "TEAM", "team-a", "team-b", float64(2)},
		}}, nil
	}
	return &fakeRows{rows: [][]any{
		{"TEAM", "team-a", float64(7)},
	}}, nil
}

func flowMatrixVariables() map[string]any {
	return map[string]any{
		"orgId": "org-1",
		"batch": map[string]any{
			"flowMatrix": map[string]any{
				"dimension": "TEAM",
				"measure":   "COUNT",
				"dateRange": map[string]any{
					"startDate": "2026-08-01",
					"endDate":   "2026-08-31",
				},
				"maxNodes": 50,
				"maxEdges": 200,
			},
		},
	}
}

// TestFlowMatrixRoute_ReachableOnlyWhenSwitchEnabled is CHAOS-4506's
// extension of the same reachability contract (real Mux, real
// PostgresSwitch, real gqlgen server, real principal.Verifier) to the
// flowMatrix operation -- the ONLY analytics-touching document this PR
// registers; see registeredFlowMatrixDocument's doc comment in
// query_route.go for why INVESTMENT_BREAKDOWN_QUERY/INVESTMENT_FULL_QUERY
// are deliberately NOT registered yet.
func TestFlowMatrixRoute_ReachableOnlyWhenSwitchEnabled(t *testing.T) {
	pool := startTestRegistryPostgres(t)

	pub, priv, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatal(err)
	}
	jwksPath := writeTestJWKS(t, pub)
	verifier, err := principal.NewVerifier(jwksPath, itTestIssuer, itTestAudience)
	if err != nil {
		t.Fatal(err)
	}

	handler := newQueryHandler(&fakeFlowMatrixCHClient{}, pool, verifier, itTestSchemaDigest)
	documentDigest := digestHex(registeredFlowMatrixDocument)
	token := signTestEnvelope(t, priv, "org-1")

	t.Run("disabled_by_default", func(t *testing.T) {
		rec := postGraphQLWithVariables(t, handler, registeredFlowMatrixDocument, token, flowMatrixVariables())
		if rec.Code != http.StatusNotFound {
			t.Fatalf("no routing-state row: got %d, want 404", rec.Code)
		}
	})

	t.Run("mode_canary_reachable_and_returns_data", func(t *testing.T) {
		setRoutingMode(t, pool, documentDigest, "flowMatrix", "canary")
		rec := postGraphQLWithVariables(t, handler, registeredFlowMatrixDocument, token, flowMatrixVariables())
		if rec.Code != http.StatusOK {
			t.Fatalf("mode=canary: got %d, body=%s", rec.Code, rec.Body.String())
		}
		if strings.Contains(rec.Body.String(), `"errors"`) || !strings.Contains(rec.Body.String(), "team-a") {
			t.Fatalf("expected response to contain the fake row's node id with no errors, got %s", rec.Body.String())
		}
	})

	t.Run("wrong_orgid_argument_is_rejected", func(t *testing.T) {
		setRoutingMode(t, pool, documentDigest, "flowMatrix", "canary")
		vars := flowMatrixVariables()
		vars["orgId"] = "org-2" // token authenticates org-1
		rec := postGraphQLWithVariables(t, handler, registeredFlowMatrixDocument, token, vars)
		if rec.Code != http.StatusOK {
			// gqlgen returns 200 with a GraphQL-level error for a
			// resolver error, not an HTTP error code -- the
			// AUTHORIZATION_ERROR is IN the body, checked below.
			t.Fatalf("expected 200 with a GraphQL error body, got %d: %s", rec.Code, rec.Body.String())
		}
		if !strings.Contains(rec.Body.String(), "AUTHORIZATION_ERROR") {
			t.Fatalf("expected an AUTHORIZATION_ERROR for a mismatched orgId argument, got %s", rec.Body.String())
		}
	})

	t.Run("unregistered_document_is_unreachable_even_when_enabled", func(t *testing.T) {
		setRoutingMode(t, pool, documentDigest, "flowMatrix", "canary")
		rec := postGraphQLWithVariables(t, handler, "query { __typename }", token, nil)
		if rec.Code != http.StatusNotFound {
			t.Fatalf("unregistered document: got %d, want 404", rec.Code)
		}
	})

	t.Run("missing_bearer_token_is_unauthorized_even_when_enabled", func(t *testing.T) {
		setRoutingMode(t, pool, documentDigest, "flowMatrix", "canary")
		rec := postGraphQLWithVariables(t, handler, registeredFlowMatrixDocument, "", flowMatrixVariables())
		if rec.Code != http.StatusUnauthorized {
			t.Fatalf("no token: got %d, want 401", rec.Code)
		}
	})

	t.Run("enabling_flowMatrix_does_not_enable_featureFlags", func(t *testing.T) {
		setRoutingMode(t, pool, documentDigest, "flowMatrix", "canary")
		rec := postGraphQL(t, handler, registeredFeatureFlagsDocument, token)
		if rec.Code != http.StatusNotFound {
			t.Fatalf("featureFlags should stay unreachable when only flowMatrix is canaried: got %d", rec.Code)
		}
	})
}
