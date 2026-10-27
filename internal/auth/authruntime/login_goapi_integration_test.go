//go:build integration

package authruntime

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/auth/passwordhash"
)

// TestGoAPIProxiedLoginMixedWindowIs503ThenOK drives one go-api against two
// auth-service upstreams in sequence: a pod without the lockout key (503 +
// Retry-After: 1, never 404), then the same address answered by a pod with the
// key over live PostgreSQL (normal login, 200).
func TestGoAPIProxiedLoginMixedWindowIs503ThenOK(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	e := newRoutesEnv(t, ctx)
	user, _ := e.seed(t, ctx)
	hash, err := passwordhash.Hash("mixed-window-password")
	if err != nil {
		t.Fatal(err)
	}
	var email string
	if err := e.admin.QueryRow(ctx, `UPDATE users SET password_hash = $2, email = split_part(email, '@', 1) || '@example.com'
		WHERE id = $1 RETURNING email`, user, hash).Scan(&email); err != nil {
		t.Fatal(err)
	}
	unset, hits := loginKeyUnsetService(t)
	var current atomic.Pointer[http.Handler]
	oldPod, newPod := unset, http.Handler(e.front.Config.Handler)
	current.Store(&oldPod)
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { (*current.Load()).ServeHTTP(w, r) }))
	defer upstream.Close()
	front := goAPIFront(t, upstream.URL)

	rec := goAPILogin(front, email, "mixed-window-password")
	if rec.Code != http.StatusServiceUnavailable || rec.Header().Get("Retry-After") != "1" {
		t.Fatalf("key unset: status %d Retry-After %q body %q; want 503 and 1", rec.Code, rec.Header().Get("Retry-After"), rec.Body.String())
	}
	if hits.Load() != 0 {
		t.Fatalf("auth-service limiter hit %d times while the key was unset", hits.Load())
	}
	current.Store(&newPod)
	rec = goAPILogin(front, email, "mixed-window-password")
	if rec.Code != http.StatusOK {
		t.Fatalf("key set: status %d body %q; want 200", rec.Code, rec.Body.String())
	}
}
