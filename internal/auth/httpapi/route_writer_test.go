package httpapi

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

// TestMarkRouteHandsTheRouteFlagToTheHandler pins that a registered
// route's handler writes through a RouteWriter carrying the route's
// ResponseModel and key.
func TestMarkRouteHandsTheRouteFlagToTheHandler(t *testing.T) {
	for _, model := range []bool{true, false} {
		var got RouteWriter
		handler := markRoute(Route{Method: "GET", Pattern: "/x", ResponseModel: model, Handler: http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			got, _ = w.(RouteWriter)
		})})
		handler.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest("GET", "/x", nil))
		if got == nil || got.ResponseModelRoute() != model || got.RouteKey() != "GET /x" {
			t.Errorf("ResponseModel=%v: the handler saw %#v", model, got)
		}
	}
}
