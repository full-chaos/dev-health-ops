package authapi

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/auth/clientaddr"
)

// C1: two users behind one web pod (same peer, same trusted web hop) get two limiter keys.
func TestTwoUsersBehindOneProxyGetTheirOwnLimiterKey(t *testing.T) {
	trust, err := clientaddr.ParseTrust("10.0.0.0/24")
	if err != nil {
		t.Fatal(err)
	}
	keyOf := func(xff string) string {
		r := httptest.NewRequest(http.MethodPost, "/", nil)
		r.RemoteAddr = "10.0.0.5:4000"
		r.Header.Set("X-Forwarded-For", xff)
		var key string
		clientaddr.Middleware(trust)(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) { key = clientKey(r) })).
			ServeHTTP(httptest.NewRecorder(), r)
		return key
	}
	a, b := keyOf("198.51.100.7, 10.0.0.9"), keyOf("198.51.100.8, 10.0.0.9")
	if a != "198.51.100.7" || b != "198.51.100.8" {
		t.Fatalf("keys = %q, %q, want one per end user", a, b)
	}
}
