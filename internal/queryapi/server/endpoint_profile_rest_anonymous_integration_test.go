//go:build integration

package server

import (
	"context"
	"crypto/ed25519"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

// TestEndpointProfileRESTAnonymousClaimsMatchRealPlane probes every REST row
// anonymously against the real plane Build mounts with every dependency present
// and every route switch on, and compares the answer with the row's
// protected/public classification (CHAOS-10140, CHAOS-4780). The unit harness
// cannot mount these routes: their builders need ClickHouse, a Postgres
// registry and envelope JWKS.
func TestEndpointProfileRESTAnonymousClaimsMatchRealPlane(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	ch, err := containers.StartClickHouse(ctx)
	if err != nil {
		t.Fatalf("start ClickHouse: %v", err)
	}
	t.Cleanup(func() {
		closeCtx, closeCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer closeCancel()
		_ = ch.Close(closeCtx)
	})
	pg, err := containers.StartPostgres(ctx)
	if err != nil {
		t.Fatalf("start PostgreSQL: %v", err)
	}
	t.Cleanup(func() {
		closeCtx, closeCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer closeCancel()
		_ = pg.Close(closeCtx)
	})
	pub, _, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatal(err)
	}
	settings := map[string]string{
		"CLICKHOUSE_URI":               ch.URI,
		"GO_API_REGISTRY_POSTGRES_URI": pg.URI,
		"GO_API_ENVELOPE_JWKS_PATH":    writeTestJWKS(t, pub),
		"GO_API_ENVELOPE_ISSUER":       itTestIssuer,
		"GO_API_ENVELOPE_AUDIENCE":     itTestAudience,
		"DEV_HEALTH_ENV":               "ci",
		"GO_API_PROOF_ROUTE_ENABLED":   "true",
		edgeJWTSecretEnvVar:            "build-routes-edge-secret-32-bytes-long!!",
	}
	for _, name := range switchNames(t) {
		settings[name] = "true"
	}
	plane, err := Build(func(name string) string { return settings[name] })
	if err != nil {
		t.Fatalf("Build with every dependency present: %v", err)
	}
	defer plane.Close()

	restPaths := map[string]bool{"/api/v1/": true}
	for _, route := range RESTRoutes() {
		restPaths[route.Pattern] = true
	}
	var rows []profileRow
	for _, row := range flatRows(t) {
		if restPaths[row.route] {
			rows = append(rows, row)
		}
	}
	protected, public := 0, 0
	for _, row := range rows {
		if row.public {
			public++
		} else {
			protected++
		}
	}
	if protected < 25 || public < 1 {
		t.Fatalf("REST rows: %d protected, %d public: the inventory or the REST table changed shape", protected, public)
	}

	// RED SEED (CHAOS-10140 red commit, removed by the fix commit): relabel one
	// protected and one public REST row, the two directions the gate must catch.
	flippedProtected, flippedPublic := false, false
	for i := range rows {
		if !rows[i].public && !flippedProtected {
			rows[i].public, flippedProtected = true, true
		} else if rows[i].public && !flippedPublic {
			rows[i].public, flippedPublic = false, true
		}
	}

	serve := func(handler http.Handler) anonymousProbe {
		return func(method, path string) int {
			recorder := httptest.NewRecorder()
			handler.ServeHTTP(recorder, httptest.NewRequest(method, path, strings.NewReader("")))
			return recorder.Code
		}
	}
	probes := map[string]anonymousProbe{"query": serve(plane.Handler)}
	if plane.InternalHandler != nil {
		probes["internal"] = serve(plane.InternalHandler)
	}
	if violations := anonymousClaimViolations(rows, probes, nil); len(violations) > 0 {
		t.Errorf("REST rows disagree with the anonymous behaviour of the real plane:\n%s", strings.Join(violations, "\n"))
	}
}
