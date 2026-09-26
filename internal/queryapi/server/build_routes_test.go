package server

import (
	"net/http"
	"net/http/httptest"
	"regexp"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/migrationmatrix"
)

// declaredRoutePaths is every path the package mounts on the query mux, read from the
// source the way the migration matrix reads it (LoadQueryAPIMuxRoutes), so a route that
// is added or removed is added or removed here too.
func declaredRoutePaths(t *testing.T) []string {
	t.Helper()
	routes, err := migrationmatrix.LoadQueryAPIMuxRoutes(".")
	if err != nil {
		t.Fatal(err)
	}
	if len(routes) < 20 {
		t.Fatalf("found only %d mounted routes in the source: the extraction no longer covers server.Build", len(routes))
	}
	paths := make([]string, 0, len(routes))
	for _, route := range routes {
		paths = append(paths, route.Path)
	}
	return paths
}

var pathParameter = regexp.MustCompile(`\{[^}]+\}`)

// probe requests one declared path with no credential, the way an unauthenticated
// caller would, and returns the status. A pattern's {parameter} is filled in.
func probe(handler http.Handler, method, path string) int {
	url := pathParameter.ReplaceAllString(path, "x")
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, httptest.NewRequest(method, url, nil))
	return recorder.Code
}

// TestBuildWithNothingConfiguredMountsNoRoute: the "Wave-0 empty" shape. Every route
// the source declares is unmounted (a 404, from the mux or from the /api/v1 catch-all),
// no readiness probe is offered (nothing to check, the caller registers the explicit
// not-configured check), and Close is safe to call.
func TestBuildWithNothingConfiguredMountsNoRoute(t *testing.T) {
	plane, err := Build(func(string) string { return "" })
	if err != nil {
		t.Fatal(err)
	}
	defer plane.Close()
	if plane.Ready != nil || len(plane.Probes) != 0 {
		t.Fatalf("an unconfigured plane offers readiness checks: ready=%v probes=%v", plane.Ready != nil, plane.Probes)
	}
	for _, path := range declaredRoutePaths(t) {
		if path == "/api/v1/" {
			continue // the catch-all itself: its 404 is the "unmounted" answer
		}
		for _, method := range []string{http.MethodGet, http.MethodPost} {
			if code := probe(plane.Handler, method, path); code != http.StatusNotFound {
				t.Errorf("%s %s = %d with nothing configured, want 404 (unmounted)", method, path, code)
			}
		}
	}
	// An unknown REST path is the catch-all's 404 in the REST error shape.
	recorder := httptest.NewRecorder()
	plane.Handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/api/v1/not-a-route", nil))
	if recorder.Code != http.StatusNotFound || !strings.Contains(recorder.Body.String(), "Not Found") {
		t.Fatalf("/api/v1/not-a-route = %d %q, want the catch-all's REST 404", recorder.Code, recorder.Body.String())
	}
}

// A setting the routes read is read through the reader Build is given and nowhere
// else: a switch that is on but whose dependencies are absent does not mount, and
// does not fail the build either.
func TestBuildReadsSettingsOnlyThroughItsReader(t *testing.T) {
	t.Setenv("GO_API_META_ENABLED", "true") // the process environment must not matter
	var asked []string
	plane, err := Build(func(name string) string {
		asked = append(asked, name)
		return ""
	})
	if err != nil {
		t.Fatal(err)
	}
	defer plane.Close()
	if len(asked) < 10 {
		t.Fatalf("Build asked its reader for only %d settings: %v", len(asked), asked)
	}
	if code := probe(plane.Handler, http.MethodGet, "/api/v1/meta"); code != http.StatusNotFound {
		t.Fatalf("/api/v1/meta = %d: the process environment reached a route", code)
	}
}
