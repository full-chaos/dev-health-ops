package server

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"
)

// The readiness contract of the query plane, through the operator listener the shell
// serves (/readyz on --http-addr) with the checks dho query-api registers: one required
// check per dependency class (ObserveProbe), or the explicit passing check of the "no
// /query configured" mode (NotConfiguredCheck). operatorReadyz is operator_test_helper_test.go.

// TestOperatorReadyz_NoQueryRouteConfigured_ReturnsOK: /query is not mounted in this
// deployment (loadQueryRouteConfig's ok=false, the Wave-0 shape). There is no
// dependency to check, so this is not a degraded state, but it must not read as a
// verified-healthy one either (CHAOS-4512): the check is counted as "not_configured".
func TestOperatorReadyz_NoQueryRouteConfigured_ReturnsOK(t *testing.T) {
	before := readyzOutcomeTotal(t, "not_configured", "query_routes")
	code, body := operatorReadyz(t, NotConfiguredCheckProbe())
	if code != http.StatusOK || body != `{"status":"ok"}` {
		t.Fatalf("readyz (unconfigured): %d %q, want 200 ok", code, body)
	}
	if got := readyzOutcomeTotal(t, "not_configured", "query_routes"); got != before+1 {
		t.Fatalf("not_configured outcome count = %d, want %d: the mode must be countable apart from a healthy answer", got, before+1)
	}
}

// TestOperatorReadyz_DependenciesHealthy_ReturnsOK pins the 200 direction once /query IS
// configured and every dependency check succeeds; with
// TestOperatorReadyz_DependencyUnreachable_Returns503 it asserts both directions
// (CHAOS-4512: "a fix that makes /readyz always 503 would pass a null-only test").
func TestOperatorReadyz_DependenciesHealthy_ReturnsOK(t *testing.T) {
	before := readyzOutcomeTotal(t, "healthy", "query_postgres")
	code, body := operatorReadyz(t, ReadinessProbe{Name: "query_postgres", Check: func(context.Context) error { return nil }})
	if code != http.StatusOK || body != `{"status":"ok"}` {
		t.Fatalf("readyz (healthy): %d %q", code, body)
	}
	if got := readyzOutcomeTotal(t, "healthy", "query_postgres"); got != before+1 {
		t.Fatalf("healthy outcome count = %d, want %d", got, before+1)
	}
}

// TestOperatorReadyz_DependencyUnreachable_Returns503 is CHAOS-4512's core fix at the
// handler-logic level (query_route_readyz_integration_test.go proves the same contract
// against real ClickHouse/Postgres). The 503 names the failing CHECK and nothing of
// the underlying error (CHAOS-4724: a real Postgres dial error renders a host:port).
func TestOperatorReadyz_DependencyUnreachable_Returns503(t *testing.T) {
	underlying := "dial tcp 127.0.0.1:1: connect: connection refused"
	probe := ReadinessProbe{Name: "query_postgres", Check: func(context.Context) error {
		return &readyzDependencyError{Class: readyzClassPostgres, Cause: errors.New(underlying)}
	}}
	before := readyzOutcomeTotal(t, "unhealthy", "query_postgres")
	code, body := operatorReadyz(t, probe)
	if code != http.StatusServiceUnavailable {
		t.Fatalf("readyz (unreachable dependency): %d %q, want 503", code, body)
	}
	if body != `{"failed_checks":["query_postgres"],"status":"not_ready"}` {
		t.Fatalf("readyz 503 body = %q, want exactly the failing check's name", body)
	}
	if strings.Contains(body, underlying) {
		t.Fatalf("readyz 503 body %q leaks the underlying dependency error %q to an unauthenticated caller", body, underlying)
	}
	if got := readyzOutcomeTotal(t, "unhealthy", "query_postgres"); got != before+1 {
		t.Fatalf("unhealthy outcome count = %d, want %d", got, before+1)
	}
}

// TestOperatorReadyz_UnhealthyBodyNeverLeaksDependencyDetail is CHAOS-4724 finding 2,
// proved WITHOUT referencing *readyzDependencyError: a probe is only obligated to
// return an error, and whatever it renders (a host:port, a filesystem path) never
// reaches the unauthenticated body, however it was wrapped.
func TestOperatorReadyz_UnhealthyBodyNeverLeaksDependencyDetail(t *testing.T) {
	cases := []struct {
		name     string
		err      error
		wantNone string
	}{
		{"postgres dial error renders host:port", errors.New("dial tcp 10.20.30.40:5432: connect: connection refused"), "10.20.30.40:5432"},
		{"clickhouse dial error renders host:port", errors.New("clickhouse: dial tcp 10.20.30.40:9000: i/o timeout"), "10.20.30.40:9000"},
		{"jwks error renders a filesystem path", errors.New("jwks document at /etc/query-api/secrets/jwks.json is not a single well-formed JSON value"), "/etc/query-api/secrets/jwks.json"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			code, body := operatorReadyz(t, ReadinessProbe{Name: "query_clickhouse", Check: func(context.Context) error { return tc.err }})
			if code != http.StatusServiceUnavailable {
				t.Fatalf("readyz: %d %q, want 503", code, body)
			}
			if strings.Contains(body, tc.wantNone) {
				t.Fatalf("readyz 503 body %q leaks %q to an unauthenticated caller", body, tc.wantNone)
			}
		})
	}
}

// TestReadyzDependencyClass_PinsAllThreeClassesAndFallback pins
// readyzDependencyClass's full contract: each of the probes' three
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

// TestOperatorReadyz_PinnedResponseBodies asserts the EXACT body bytes of every
// outcome: 200 {"status":"ok"} when every check passes, and 503 with the failing
// check names (sorted) and nothing else. Every check name is a closed set: the
// probes' own names plus "query_routes" (the no-/query mode) and "query_listener".
func TestOperatorReadyz_PinnedResponseBodies(t *testing.T) {
	fail := func(class string) func(context.Context) error {
		return func(context.Context) error { return &readyzDependencyError{Class: class, Cause: errors.New("boom")} }
	}
	cases := []struct {
		name   string
		probes []ReadinessProbe
		want   string
	}{
		{"healthy", []ReadinessProbe{{Name: "query_postgres", Check: func(context.Context) error { return nil }}}, `{"status":"ok"}`},
		{"unhealthy clickhouse", []ReadinessProbe{{Name: "query_clickhouse", Check: fail(readyzClassClickHouse)}}, `{"failed_checks":["query_clickhouse"],"status":"not_ready"}`},
		{"unhealthy postgres", []ReadinessProbe{{Name: "query_postgres", Check: fail(readyzClassPostgres)}}, `{"failed_checks":["query_postgres"],"status":"not_ready"}`},
		{"unhealthy jwks", []ReadinessProbe{{Name: "query_jwks", Check: fail(readyzClassJWKS)}}, `{"failed_checks":["query_jwks"],"status":"not_ready"}`},
		{"several fail: sorted, each named", []ReadinessProbe{
			{Name: "query_postgres", Check: fail(readyzClassPostgres)},
			{Name: "query_clickhouse", Check: fail(readyzClassClickHouse)},
			{Name: "query_jwks", Check: func(context.Context) error { return nil }},
		}, `{"failed_checks":["query_clickhouse","query_postgres"],"status":"not_ready"}`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if _, body := operatorReadyz(t, tc.probes...); body != tc.want {
				t.Fatalf("readyz body = %q, want exactly %q", body, tc.want)
			}
		})
	}
}

// TestObserveProbe_BoundsSlowDependencyCheck proves "an unbounded readiness probe
// hangs the orchestrator that polls it" cannot happen here: a probe that blocks until
// its context ends is released by ObserveProbe's own readyzTimeout (not by anything the
// probe does), and the failure it returns is the class alone.
func TestObserveProbe_BoundsSlowDependencyCheck(t *testing.T) {
	blocked := make(chan struct{})
	probe := ReadinessProbe{Name: "query_clickhouse", Check: func(ctx context.Context) error {
		defer close(blocked)
		<-ctx.Done()
		return ctx.Err()
	}}
	start := time.Now()
	// The parent never ends on its own within the bound: only ObserveProbe's own deadline can release the probe.
	parent, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	err := ObserveProbe(probe)(parent)
	elapsed := time.Since(start)
	select {
	case <-blocked:
	default:
		t.Fatal("the probe never observed ctx.Done(): ObserveProbe applied no deadline")
	}
	if err == nil || err.Error() != "dependency" {
		t.Fatalf("err = %v, want the class-only fallback \"dependency\"", err)
	}
	if elapsed > readyzTimeout+5*time.Second {
		t.Fatalf("a blocked probe took %s to fail, want about %s (readyzTimeout)", elapsed, readyzTimeout)
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
