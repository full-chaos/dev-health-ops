package admin

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/api/policy"
)

// Every admin route answers no-store, a refused caller included: the guard
// runs inside the middleware, so the 401 path is the same code path as any
// error answer.
func TestEveryAdminRouteAnswersCacheControlNoStore(t *testing.T) {
	routes := Routes(Deps{Guard: policy.NewGuard(nil, nil)})
	if len(routes) < 50 {
		t.Fatalf("only %d admin routes built; the table did not load", len(routes))
	}
	sawModels := false
	for _, route := range routes {
		path := strings.NewReplacer("{", "x", "}", "").Replace(route.Pattern)
		recorder := httptest.NewRecorder()
		func() {
			defer func() { _ = recover() }() // a nil Authenticator may panic after the header is set
			route.Handler.ServeHTTP(recorder, httptest.NewRequest(route.Method, path, nil))
		}()
		if got := recorder.Header().Get("Cache-Control"); got != "no-store" {
			t.Errorf("%s %s: Cache-Control = %q, want no-store", route.Method, route.Pattern, got)
		}
		if route.Method == http.MethodGet && route.Pattern == governancePrefix+"/llm-settings/models" {
			sawModels = true
		}
	}
	if !sawModels {
		t.Fatal("GET /api/v1/admin/llm-settings/models not in the admin route table")
	}
}
