package server

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

// CHAOS-13492: /healthz, /readyz and /metrics live on the operator listener only.
// The query (public) and internal listeners answer the plane's own response there.
func TestListenersServeNoOperatorPaths(t *testing.T) {
	plane := &Plane{Handler: http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusTeapot)
	}), Close: func() {}}
	public, internal := Listeners("127.0.0.1:0", "127.0.0.1:0", plane, nil)
	for name, l := range map[string]*Listener{"public": public, "internal": internal} {
		for _, path := range []string{"/healthz", "/readyz", "/metrics"} {
			rec := httptest.NewRecorder()
			l.server.Handler.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, path, nil))
			if rec.Code != http.StatusTeapot {
				t.Errorf("%s listener: %s = %d, want the plane's answer %d", name, path, rec.Code, http.StatusTeapot)
			}
		}
	}
}
