package main

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestHealthzAlwaysOK(t *testing.T) {
	rec := httptest.NewRecorder()
	healthzHandler()(rec, httptest.NewRequest(http.MethodGet, "/healthz", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("healthz: got status %d, want 200", rec.Code)
	}
}

// TestHealthzStaysUpWhenReadyzWouldFail proves healthz and readyz are
// DISTINCT signals (CHAOS-4512's explicit requirement: "readiness and
// liveness must stay distinct -- do not collapse them"). healthzHandler
// takes no dependency argument at all, so there is nothing to wire a
// broken dependency into -- that absence of a parameter IS the proof:
// this handler cannot become unhealthy because a downstream store is
// unreachable, by construction.
func TestHealthzStaysUpWhenReadyzWouldFail(t *testing.T) {
	rec := httptest.NewRecorder()
	healthzHandler()(rec, httptest.NewRequest(http.MethodGet, "/healthz", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("healthz: got status %d, want 200 regardless of /query's dependency state", rec.Code)
	}
}

// TestReadyzHandler_NoQueryRouteConfigured_ReturnsOK covers readyzHandler's
// ready==nil branch: /query is not mounted in this deployment
// (loadQueryRouteConfig's ok=false, Wave-0 shape). There is no dependency
// to check, so this is not a degraded state -- but the response body must
// say so explicitly rather than reading identically to a
// verified-healthy 200 (CHAOS-4512: "do not let 'no dependencies
// configured' silently read as 'ready to serve'"). See readyzHandler's
// doc comment in main.go for the full reasoning.
func TestReadyzHandler_NoQueryRouteConfigured_ReturnsOK(t *testing.T) {
	rec := httptest.NewRecorder()
	readyzHandler(nil)(rec, httptest.NewRequest(http.MethodGet, "/readyz", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("readyz (unconfigured): got status %d, want 200", rec.Code)
	}
	if !strings.Contains(rec.Body.String(), "not configured") {
		t.Fatalf("readyz (unconfigured) body %q does not distinguish itself from a verified-healthy 200", rec.Body.String())
	}
}

// TestReadyzHandler_DependenciesHealthy_ReturnsOK pins the 200 direction
// once /query IS configured and its dependency check succeeds --
// CHAOS-4512's explicit instruction: "a fix that makes /readyz always 503
// would pass a null-only test", so this and
// TestReadyzHandler_DependencyUnreachable_Returns503 below run in the same
// package, asserting both directions.
func TestReadyzHandler_DependenciesHealthy_ReturnsOK(t *testing.T) {
	ready := func(ctx context.Context) error { return nil }
	rec := httptest.NewRecorder()
	readyzHandler(ready)(rec, httptest.NewRequest(http.MethodGet, "/readyz", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("readyz (healthy dependency check): got status %d, want 200 (body %q)", rec.Code, rec.Body.String())
	}
}

// TestReadyzHandler_DependencyUnreachable_Returns503 is CHAOS-4512's core
// fix, at the handler-logic level (query_route_readyz_integration_test.go
// proves the same contract end to end against real ClickHouse/Postgres
// dependencies). Before this fix, readyzHandler took no dependency
// argument at all and wrote 200 unconditionally -- this test would not
// even compile against that shape, let alone pass; see the PR's red-run
// evidence (a deliberate mutation reverting readyzHandler to ignore
// `ready`) for the executed proof.
//
// CHAOS-4724 changed WHAT the 503 body contains: it used to be
// wantErr.Error() verbatim (a real Postgres dial error renders a
// host:port); it is now only the failing dependency's class. This test
// now pins both halves of that: the body names the class ("postgres")
// AND does not contain the underlying host:port the wrapped error
// carries -- see TestReadyzHandler_UnhealthyBodyNeverLeaksDependencyDetail
// below for the same claim proved without depending on
// *readyzDependencyError at all.
func TestReadyzHandler_DependencyUnreachable_Returns503(t *testing.T) {
	underlying := "dial tcp 127.0.0.1:1: connect: connection refused"
	wantErr := &readyzDependencyError{Class: readyzClassPostgres, Cause: errors.New(underlying)}
	ready := func(ctx context.Context) error { return wantErr }
	rec := httptest.NewRecorder()
	readyzHandler(ready)(rec, httptest.NewRequest(http.MethodGet, "/readyz", nil))
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("readyz (unreachable dependency): got status %d, want 503 (body %q)", rec.Code, rec.Body.String())
	}
	if got := rec.Body.String(); got != "not ready: postgres" {
		t.Fatalf("readyz 503 body = %q, want exactly %q (runbook .remember/go-api-enablement-runbook.md §5b pins this)", got, "not ready: postgres")
	}
	if strings.Contains(rec.Body.String(), underlying) {
		t.Fatalf("readyz 503 body %q leaks the underlying dependency error %q to an unauthenticated caller", rec.Body.String(), underlying)
	}
}

// TestReadyzHandler_ContentTypeSetOnEveryBranch is CHAOS-4724 finding 1:
// readyzHandler previously set no Content-Type on any of its three
// branches, so Go content-sniffed every /readyz response. This test
// exercises readyzHandler only through its existing, pre-CHAOS-4724
// signature (no *readyzDependencyError involved) so it is a clean
// red-on-parent proof: on parent 512c4e77b, rec.Header().Get("Content-Type")
// is "" for all three branches; this fails there and passes once the
// handler sets it explicitly.
func TestReadyzHandler_ContentTypeSetOnEveryBranch(t *testing.T) {
	const wantContentType = "text/plain; charset=utf-8"

	cases := []struct {
		name  string
		ready func(context.Context) error
	}{
		{"not configured", nil},
		{"healthy", func(ctx context.Context) error { return nil }},
		{"unhealthy", func(ctx context.Context) error { return errors.New("boom") }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rec := httptest.NewRecorder()
			readyzHandler(tc.ready)(rec, httptest.NewRequest(http.MethodGet, "/readyz", nil))
			if got := rec.Header().Get("Content-Type"); got != wantContentType {
				t.Fatalf("readyz (%s) Content-Type = %q, want %q", tc.name, got, wantContentType)
			}
		})
	}
}

// TestReadyzHandler_UnhealthyBodyNeverLeaksDependencyDetail is CHAOS-4724
// finding 2, proved WITHOUT referencing *readyzDependencyError -- a
// caller of readinessCheck's `ready` func is only obligated to return an
// error, and this proves the handler never re-exposes whatever that
// error renders, regardless of how it got wrapped. It is deliberately
// the same shape a real pgx/clickhouse-go dial error or
// principal.Verifier.CheckJWKS error would take (a host:port, a
// filesystem path) -- both are exactly what an operator can already get
// from the log line this handler emits, and exactly what an
// unauthenticated caller must not get from the response body. Red on
// parent 512c4e77b: readyzHandler wrote err.Error() straight into the
// body there, so both substrings below were present in the 503 response.
func TestReadyzHandler_UnhealthyBodyNeverLeaksDependencyDetail(t *testing.T) {
	cases := []struct {
		name     string
		err      error
		wantNone string // a substring that must NOT appear in the response body
	}{
		{"postgres dial error renders host:port", errors.New("dial tcp 10.20.30.40:5432: connect: connection refused"), "10.20.30.40:5432"},
		{"clickhouse dial error renders host:port", errors.New("clickhouse: dial tcp 10.20.30.40:9000: i/o timeout"), "10.20.30.40:9000"},
		{"jwks error renders a filesystem path", errors.New("jwks document at /etc/query-api/secrets/jwks.json is not a single well-formed JSON value"), "/etc/query-api/secrets/jwks.json"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ready := func(ctx context.Context) error { return tc.err }
			rec := httptest.NewRecorder()
			readyzHandler(ready)(rec, httptest.NewRequest(http.MethodGet, "/readyz", nil))
			if rec.Code != http.StatusServiceUnavailable {
				t.Fatalf("readyz: got status %d, want 503 (body %q)", rec.Code, rec.Body.String())
			}
			if strings.Contains(rec.Body.String(), tc.wantNone) {
				t.Fatalf("readyz 503 body %q leaks %q to an unauthenticated caller", rec.Body.String(), tc.wantNone)
			}
			if !strings.HasPrefix(rec.Body.String(), "not ready: ") {
				t.Fatalf("readyz 503 body %q does not keep the load-bearing %q prefix (.remember/go-api-enablement-runbook.md §5b)", rec.Body.String(), "not ready: ")
			}
		})
	}
}

// TestReadyzDependencyClass_PinsAllThreeClassesAndFallback pins
// readyzDependencyClass's full contract: each of readinessCheck's three
// wrapped classes maps to exactly that class name, an error wrapped
// further (e.g. by a future caller's own fmt.Errorf("...: %w", depErr))
// still resolves via errors.As, and anything that is not a
// *readyzDependencyError at all -- a caller that forgot to wrap --
// degrades to the generic "dependency" label rather than ever falling
// through to that error's own, potentially detailed, Error() text.
func TestReadyzDependencyClass_PinsAllThreeClassesAndFallback(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want string
	}{
		{"clickhouse", &readyzDependencyError{Class: readyzClassClickHouse, Cause: errors.New("dial tcp 10.0.0.1:9000: i/o timeout")}, "clickhouse"},
		{"postgres", &readyzDependencyError{Class: readyzClassPostgres, Cause: errors.New("dial tcp 10.0.0.1:5432: connect: connection refused")}, "postgres"},
		{"jwks", &readyzDependencyError{Class: readyzClassJWKS, Cause: errors.New("jwks document at /var/run/secrets/jwks.json is empty")}, "jwks"},
		{"further-wrapped still resolves via errors.As", fmt.Errorf("readinessCheck: %w", &readyzDependencyError{Class: readyzClassJWKS, Cause: errors.New("boom")}), "jwks"},
		{"unwrapped error falls back safely", errors.New("dial tcp 10.0.0.1:5432: connect: connection refused"), "dependency"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := readyzDependencyClass(tc.err); got != tc.want {
				t.Fatalf("readyzDependencyClass(%v) = %q, want %q", tc.err, got, tc.want)
			}
		})
	}
}

// TestReadyzHandler_PinnedResponseBodies asserts the EXACT body bytes for
// all three /readyz outcomes, per the runbook's constraint
// (.remember/go-api-enablement-runbook.md §5b: "the enablement runbook's
// §5b readiness check asserts the exact body bytes -- `ready`,
// `ready: /query not configured`, and the `not ready: ...` prefix are
// load-bearing"). CHAOS-4724 narrowed the unhealthy shape from an
// unbounded error string to a closed set of dependency-class suffixes;
// this test pins that closed set so a future change to any of the three
// class constants (or to readyzDependencyClass's fallback) is a visible,
// deliberate decision here and in the runbook, not a silent drift.
func TestReadyzHandler_PinnedResponseBodies(t *testing.T) {
	cases := []struct {
		name  string
		ready func(context.Context) error
		want  string
	}{
		{"not configured", nil, "ready: /query not configured"},
		{"healthy", func(ctx context.Context) error { return nil }, "ready"},
		{"unhealthy clickhouse", func(ctx context.Context) error {
			return &readyzDependencyError{Class: readyzClassClickHouse, Cause: errors.New("boom")}
		}, "not ready: clickhouse"},
		{"unhealthy postgres", func(ctx context.Context) error {
			return &readyzDependencyError{Class: readyzClassPostgres, Cause: errors.New("boom")}
		}, "not ready: postgres"},
		{"unhealthy jwks", func(ctx context.Context) error {
			return &readyzDependencyError{Class: readyzClassJWKS, Cause: errors.New("boom")}
		}, "not ready: jwks"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rec := httptest.NewRecorder()
			readyzHandler(tc.ready)(rec, httptest.NewRequest(http.MethodGet, "/readyz", nil))
			if got := rec.Body.String(); got != tc.want {
				t.Fatalf("readyz body = %q, want exactly %q", got, tc.want)
			}
		})
	}
}

// TestReadyzHandler_BoundsSlowDependencyCheck proves the brief's "Bound
// the checks with a timeout. An unbounded readiness probe hangs the
// orchestrator that polls it" requirement: a ready func that ignores its
// own return value and blocks forever must still cause readyzHandler to
// respond -- because readyzHandler wraps the request context in
// readyzTimeout, and this ready func honors ctx.Done() (exactly like a
// real network call under a context deadline would), not because the
// func itself has any bound.
func TestReadyzHandler_BoundsSlowDependencyCheck(t *testing.T) {
	blocked := make(chan struct{})
	ready := func(ctx context.Context) error {
		defer close(blocked)
		<-ctx.Done()
		return ctx.Err()
	}

	start := time.Now()
	rec := httptest.NewRecorder()
	readyzHandler(ready)(rec, httptest.NewRequest(http.MethodGet, "/readyz", nil))
	elapsed := time.Since(start)

	select {
	case <-blocked:
	default:
		t.Fatal("ready func never observed ctx.Done() -- readyzHandler did not apply a deadline")
	}
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("readyz (timed-out dependency check): got status %d, want 503 (body %q)", rec.Code, rec.Body.String())
	}
	// Generous upper bound so this test is not flaky on a loaded CI
	// runner, while still catching a regression that removed the
	// timeout entirely (which would hang this test until Go's own test
	// timeout, not merely run a bit slow).
	const maxReasonableOverhead = 5 * time.Second
	if elapsed > readyzTimeout+maxReasonableOverhead {
		t.Fatalf("readyz took %s to respond to a blocked dependency check, want at most ~%s (readyzTimeout)", elapsed, readyzTimeout)
	}
}

// TestExecutableSchemaBuildsAndLinks proves the gqlgen-generated schema and
// the (all-panicking) resolver stubs actually compose into a working
// graphql.ExecutableSchema -- a real, if narrow, build-time proof that the
// schema-first codegen and the canonical SDL pin stay in sync. It does NOT
// exercise a resolver (every field panics by design, see main.go's package
// doc) -- only that construction succeeds.
func TestExecutableSchemaBuildsAndLinks(t *testing.T) {
	handler := newExecutableSchemaHandler()
	if handler == nil {
		t.Fatal("newExecutableSchemaHandler returned nil")
	}
}

func TestAddrDefaultsWhenEnvUnset(t *testing.T) {
	t.Setenv("QUERY_API_ADDR", "")
	if got := addr(); got != defaultAddr {
		t.Fatalf("addr() = %q, want default %q", got, defaultAddr)
	}
}

func TestAddrHonorsEnvOverride(t *testing.T) {
	t.Setenv("QUERY_API_ADDR", ":9999")
	if got := addr(); got != ":9999" {
		t.Fatalf("addr() = %q, want :9999", got)
	}
}
