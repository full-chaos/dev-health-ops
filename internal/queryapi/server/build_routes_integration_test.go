//go:build integration

package server

import (
	"context"
	"crypto/ed25519"
	"net/http"
	"net/http/httptest"
	"os"
	"regexp"
	"sort"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

var enabledSwitch = regexp.MustCompile(`"(GO_API_[A-Z_]+_ENABLED)"`)

// TestBuildWithEverythingConfiguredMountsEveryDeclaredRoute is the identity of the port
// (CHAOS-6447): with the dependencies real and every route switch on, Build mounts every
// path the source declares (an unauthenticated request is refused for a reason other than
// "no such route"), offers one readiness probe per dependency class, and the probes pass.
func TestBuildWithEverythingConfiguredMountsEveryDeclaredRoute(t *testing.T) {
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
	pub, priv, err := ed25519.GenerateKey(nil)
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
	// Every route switch the source names, on.
	for _, name := range switchNames(t) {
		settings[name] = "true"
	}
	plane, err := Build(func(name string) string { return settings[name] })
	if err != nil {
		t.Fatalf("Build with every dependency present: %v", err)
	}
	defer plane.Close()
	if plane.Ready == nil || len(plane.Probes) < 3 {
		t.Fatalf("a configured plane offers no readiness: ready=%v probes=%d", plane.Ready != nil, len(plane.Probes))
	}
	names := map[string]bool{}
	for _, probe := range plane.Probes {
		names[probe.Name] = true
		if err := probe.Check(ctx); err != nil {
			t.Errorf("probe %s failed against real dependencies: %v", probe.Name, err)
		}
	}
	for _, want := range []string{"query_clickhouse", "query_postgres", "query_jwks"} {
		if !names[want] {
			t.Errorf("no %s probe: %v", want, names)
		}
	}
	if err := plane.Ready(ctx); err != nil {
		t.Errorf("the combined readiness check fails against real dependencies: %v", err)
	}
	var unmounted []string
	for _, path := range declaredRoutePaths(t) {
		if path == "/api/v1/" {
			continue
		}
		mounted := false
		for _, method := range []string{http.MethodGet, http.MethodPost} {
			if probe(plane.Handler, method, path) != http.StatusNotFound {
				mounted = true
			}
		}
		if !mounted {
			unmounted = append(unmounted, path)
		}
	}
	sort.Strings(unmounted)
	if len(unmounted) > 0 {
		t.Fatalf("with every dependency and every switch present these declared routes answer 404 (unmounted): %v", unmounted)
	}
	// CHAOS-8307: the REAL plane mounts exactly the table's (method, pattern) pairs. An
	// authenticated request is refused with the method refusal (405) for every method a
	// pattern does not declare and is not for every method it declares. This is the row
	// set the unit test cannot build (the two explain builders open a write ClickHouse
	// connection at construction), so it covers all 24 patterns on the real plane.
	bearer := signTestEnvelope(t, priv, "org-1")
	declared := map[string]map[string]bool{}
	for _, route := range RESTRoutes() {
		if declared[route.Pattern] == nil {
			declared[route.Pattern] = map[string]bool{}
		}
		declared[route.Pattern][route.Method] = true
	}
	for pattern, methods := range declared {
		for _, method := range allMethods {
			request := httptest.NewRequest(method, pathParameter.ReplaceAllString(pattern, "x"), nil)
			request.Header.Set("Authorization", "Bearer "+bearer)
			recorder := httptest.NewRecorder()
			plane.Handler.ServeHTTP(recorder, request)
			switch refused := recorder.Code == http.StatusMethodNotAllowed; {
			case methods[method] && (refused || recorder.Code == http.StatusNotFound):
				t.Errorf("%s %s: declared in the table, the real plane answers %d", method, pattern, recorder.Code)
			case !methods[method] && !refused:
				t.Errorf("%s %s: not declared in the table, the real plane answers %d (not 405)", method, pattern, recorder.Code)
			}
		}
	}
	// The write-proof route is on the internal route set only: absent (404) from the public
	// handler, answered (any status but 404 for an empty request) by the internal one.
	if got := probe(plane.Handler, http.MethodPost, "/query/proof-write"); got != http.StatusNotFound {
		t.Errorf("POST /query/proof-write on the PUBLIC handler answered %d, want 404 (not mounted there)", got)
	}
	if probe(plane.InternalHandler, http.MethodPost, "/query/proof-write") == http.StatusNotFound {
		t.Errorf("POST /query/proof-write on the INTERNAL handler answered 404: the route is not mounted there")
	}
	// /graphql, the product path, is on the public route set: an empty,
	// credential-less POST is the edge's 401, never the mux's 404.
	if got := probe(plane.Handler, http.MethodPost, graphQLEdgePath); got != http.StatusUnauthorized {
		t.Errorf("POST %s on the PUBLIC handler answered %d, want 401 (mounted, refusing the missing credential)", graphQLEdgePath, got)
	}
}

// switchNames are the GO_API_*_ENABLED settings the package's source names.
func switchNames(t *testing.T) []string {
	t.Helper()
	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatal(err)
	}
	found := map[string]bool{}
	for _, entry := range entries {
		name := entry.Name()
		if entry.IsDir() || len(name) < 4 || name[len(name)-3:] != ".go" || (len(name) > 8 && name[len(name)-8:] == "_test.go") {
			continue
		}
		raw, err := os.ReadFile(name)
		if err != nil {
			t.Fatal(err)
		}
		for _, match := range enabledSwitch.FindAllSubmatch(raw, -1) {
			found[string(match[1])] = true
		}
	}
	names := make([]string, 0, len(found))
	for name := range found {
		names = append(names, name)
	}
	sort.Strings(names)
	if len(names) < 15 {
		t.Fatalf("found only %d route switches in the source: %v", len(names), names)
	}
	return names
}
