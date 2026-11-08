//go:build integration

package server

import (
	"context"
	"net/http"
	"sort"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

// With real data stores and every route switch on, a plane with no usable envelope key (the key unset, or set over a
// JWKS with no usable key) mounts every declared route, /query on the MCP listener included, offers no query_jwks
// probe, and is ready.
func TestBuildWithoutTheEnvelopeKeyMountsEveryDeclaredRoute(t *testing.T) {
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
	for name, key := range map[string]map[string]string{
		"key unset":                         nil,
		"key set, JWKS holds no usable key": fullKeyEnv(writeAbsentKeyJWKS(t, false)),
	} {
		t.Run(name, func(t *testing.T) {
			settings := map[string]string{
				"CLICKHOUSE_URI":               ch.URI,
				"GO_API_REGISTRY_POSTGRES_URI": pg.URI,
				accessTokenIssuerEnvVar:        accessTestIssuer,
				"DEV_HEALTH_ENV":               "ci",
				"GO_API_PROOF_ROUTE_ENABLED":   "true",
				edgeJWTSecretEnvVar:            "build-routes-edge-secret-32-bytes-long!!",
			}
			for k, v := range key {
				settings[k] = v
			}
			for _, switchName := range switchNames(t) {
				settings[switchName] = "true"
			}
			plane, err := Build(func(name string) string { return settings[name] })
			if err != nil {
				t.Fatalf("Build without a usable envelope key failed start: %v", err)
			}
			defer plane.Close()
			if plane.Ready == nil {
				t.Fatal("a configured plane offers no readiness")
			}
			for _, probe := range plane.Probes {
				if probe.Name == "query_jwks" {
					t.Errorf("a query_jwks probe without a usable envelope key")
				}
				if err := probe.Check(ctx); err != nil {
					t.Errorf("probe %s failed against real dependencies: %v", probe.Name, err)
				}
			}
			if err := plane.Ready(ctx); err != nil {
				t.Errorf("not ready without the envelope key: %v", err)
			}
			var unmounted []string
			for _, path := range declaredRoutePaths(t) {
				if path == "/api/v1/" {
					continue
				}
				if probe(plane.Handler, http.MethodGet, path) == http.StatusNotFound && probe(plane.Handler, http.MethodPost, path) == http.StatusNotFound {
					unmounted = append(unmounted, path)
				}
			}
			sort.Strings(unmounted)
			if len(unmounted) > 0 {
				t.Fatalf("without the envelope key these declared routes answer 404 (unmounted): %v", unmounted)
			}
			if plane.MCPHandler == nil || probe(plane.MCPHandler, http.MethodPost, "/query") == http.StatusNotFound {
				t.Errorf("MCP /query is not mounted without the envelope key")
			}
		})
	}
}
