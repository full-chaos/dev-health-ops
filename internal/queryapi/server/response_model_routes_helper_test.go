package server

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/auth/httpapi"
)

// responseModelTestMux resolves a test request to its responseModelRoutes
// key, the way the production mux pattern does.
var responseModelTestMux = func() *http.ServeMux {
	mux := http.NewServeMux()
	for key := range responseModelRoutes {
		mux.HandleFunc(key, func(http.ResponseWriter, *http.Request) {})
	}
	return mux
}()

// serveRoute runs a route test's handler as production does: with its
// route's response_model flag on the writer. A 2xx body on a
// response_model route that did not go through writeModelResponse fails
// the test (the handler left the body on encoding/json).
func serveRoute(t *testing.T, handler http.HandlerFunc, rec *httptest.ResponseRecorder, req *http.Request) {
	t.Helper()
	_, key := responseModelTestMux.Handler(req)
	routeWriter := httpapi.NewRouteWriter(rec, key, responseModelRoutes[key])
	handler(routeWriter, req)
	if routeWriter.MissingModelBody() {
		t.Errorf("%s answered %d with a body not written by writeModelResponse; FastAPI writes it as a response_model", key, rec.Code)
	}
}
