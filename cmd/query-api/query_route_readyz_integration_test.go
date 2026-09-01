//go:build integration

package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

// TestReadyz_PostgresUnreachableSinceStartup_Returns503 is CHAOS-4512's
// literal defect, reproduced end to end: pgxpool.New (query_route.go's
// buildQueryRoute) never dials -- it is lazy by construction -- so a
// GO_API_REGISTRY_POSTGRES_URI that points at nothing listening has
// NEVER failed buildQueryRoute, and never fails startup. Before this
// fix, that meant readyzHandler answered 200 unconditionally for an
// instance whose /query path could never reach its registry. ClickHouse
// must be REAL and reachable here (a real testcontainer, not a stub) --
// buildQueryRoute's own eager ClickHouse ping runs first and would
// otherwise fail the whole build for the wrong reason, proving nothing
// about Postgres.
func TestReadyz_PostgresUnreachableSinceStartup_Returns503(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	ch, err := containers.StartClickHouse(ctx)
	if err != nil {
		t.Fatalf("start ClickHouse test dependency: %v", err)
	}
	t.Cleanup(func() {
		closeCtx, closeCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer closeCancel()
		_ = ch.Close(closeCtx)
	})

	cfg := queryRouteConfig{
		ClickHouseURI: ch.URI,
		// Deliberately unreachable -- nothing listens on port 1. Same
		// placeholder shape TestBuildQueryRoute_FailsFastOnWrongClickHouseProtocol
		// (query_route_integration_test.go) uses for an "unused, must not
		// be dialed for this test to prove what it claims" Postgres DSN.
		RegistryPostgresURI: "postgres://unused:unused@127.0.0.1:1/unused",
		EnvelopeJWKSPath:    "/dev/null",
		EnvelopeIssuer:      itTestIssuer,
		EnvelopeAudience:    itTestAudience,
		SchemaDigest:        itTestSchemaDigest,
	}

	_, ready, cleanup, buildErr := buildQueryRoute(cfg)
	if buildErr != nil {
		t.Fatalf("buildQueryRoute unexpectedly failed against an unreachable-but-lazily-dialed Postgres pool (this is the defect this test exists to prove buildQueryRoute does NOT fail on): %v", buildErr)
	}
	defer cleanup()

	rec := httptest.NewRecorder()
	readyzHandler(ready)(rec, httptest.NewRequest(http.MethodGet, "/readyz", nil))
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("/readyz = %d, want 503 -- the registry Postgres this instance depends on was never reachable (body %q)", rec.Code, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), "registry postgres") {
		t.Fatalf("/readyz 503 body %q does not name the registry postgres dependency", rec.Body.String())
	}
}

// TestReadyz_BothDependenciesReachable_ThenClickHouseDiesAfterStartup
// pins the 200 direction against REAL dependencies (not just the
// handler-logic unit tests in main_test.go), then proves the check is
// LIVE -- re-evaluated on every /readyz call, not cached from process
// start -- by killing a dependency that was genuinely healthy when
// buildQueryRoute ran and observing /readyz flip to 503 on the very next
// call, no restart involved.
func TestReadyz_BothDependenciesReachable_ThenClickHouseDiesAfterStartup(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	ch, err := containers.StartClickHouse(ctx)
	if err != nil {
		t.Fatalf("start ClickHouse test dependency: %v", err)
	}
	chClosed := false
	t.Cleanup(func() {
		if chClosed {
			return
		}
		closeCtx, closeCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer closeCancel()
		_ = ch.Close(closeCtx)
	})

	pg, err := containers.StartPostgres(ctx)
	if err != nil {
		t.Fatalf("start Postgres test dependency: %v", err)
	}
	t.Cleanup(func() {
		closeCtx, closeCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer closeCancel()
		_ = pg.Close(closeCtx)
	})

	cfg := queryRouteConfig{
		ClickHouseURI:       ch.URI,
		RegistryPostgresURI: pg.URI,
		EnvelopeJWKSPath:    "/dev/null",
		EnvelopeIssuer:      itTestIssuer,
		EnvelopeAudience:    itTestAudience,
		SchemaDigest:        itTestSchemaDigest,
	}

	_, ready, cleanup, buildErr := buildQueryRoute(cfg)
	if buildErr != nil {
		t.Fatalf("buildQueryRoute: %v", buildErr)
	}
	defer cleanup()

	t.Run("both dependencies reachable: 200", func(t *testing.T) {
		rec := httptest.NewRecorder()
		readyzHandler(ready)(rec, httptest.NewRequest(http.MethodGet, "/readyz", nil))
		if rec.Code != http.StatusOK {
			t.Fatalf("/readyz = %d, want 200 with both real dependencies reachable (body %q)", rec.Code, rec.Body.String())
		}
	})

	t.Run("clickhouse dies after startup: 503, not cached", func(t *testing.T) {
		closeCtx, closeCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer closeCancel()
		if err := ch.Close(closeCtx); err != nil {
			t.Fatalf("terminate ClickHouse: %v", err)
		}
		chClosed = true

		rec := httptest.NewRecorder()
		readyzHandler(ready)(rec, httptest.NewRequest(http.MethodGet, "/readyz", nil))
		if rec.Code != http.StatusServiceUnavailable {
			t.Fatalf("/readyz = %d, want 503 once ClickHouse is unreachable (body %q)", rec.Code, rec.Body.String())
		}
		if !strings.Contains(rec.Body.String(), "clickhouse") {
			t.Fatalf("/readyz 503 body %q does not name the clickhouse dependency", rec.Body.String())
		}
	})
}
