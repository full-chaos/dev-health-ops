package main

import (
	"context"
	"errors"
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
func TestReadyzHandler_DependencyUnreachable_Returns503(t *testing.T) {
	wantErr := errors.New("registry postgres: dial tcp 127.0.0.1:1: connect: connection refused")
	ready := func(ctx context.Context) error { return wantErr }
	rec := httptest.NewRecorder()
	readyzHandler(ready)(rec, httptest.NewRequest(http.MethodGet, "/readyz", nil))
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("readyz (unreachable dependency): got status %d, want 503 (body %q)", rec.Code, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), wantErr.Error()) {
		t.Fatalf("readyz 503 body %q does not surface the dependency error %q", rec.Body.String(), wantErr.Error())
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
