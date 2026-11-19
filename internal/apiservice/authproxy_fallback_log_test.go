package apiservice

import (
	"bytes"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

// A request of an aggregate whose flag is on that go-api answers itself (its own front checks, or no forward) is loud:
// one Warn line with the aggregate, the route pattern and a reason, at most once per route and window; never a path,
// body, header or token (CHAOS-14131). With the flag off nothing is logged.
func TestAuthProxyLogsAFlagOnLocalAnswerOncePerWindow(t *testing.T) {
	var logs bytes.Buffer
	proxy := newAuthProxy(mustProxyConfig(t, allOn("http://127.0.0.1:1")), slog.New(slog.NewTextHandler(&logs, nil)))
	clock := time.Date(2026, 11, 19, 12, 0, 0, 0, time.UTC)
	proxy.now = func() time.Time { return clock }
	local := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusUnprocessableEntity) })
	route := proxy.sessionRoute(AuthOIDC, local)
	serve := func() {
		request := httptest.NewRequest(http.MethodPost, "/api/v1/auth/oauth/secret-provider-id/callback",
			strings.NewReader(`{"code":"code-text","state":"state-text"}`))
		request.Pattern = "/api/v1/auth/oauth/{first}/{second}"
		request.Header.Set("Authorization", "Bearer token-text")
		route.ServeHTTP(httptest.NewRecorder(), request)
	}
	lines := func() []string {
		var out []string
		for _, line := range strings.Split(strings.TrimSpace(logs.String()), "\n") {
			if strings.Contains(line, "auth proxy: answered by go-api with the aggregate on") {
				out = append(out, line)
			}
		}
		return out
	}

	serve()
	got := lines()
	if len(got) != 1 {
		t.Fatalf("first local answer: %d fallback lines, want 1:\n%s", len(got), logs.String())
	}
	for _, want := range []string{"aggregate=oidc", "route=/api/v1/auth/oauth/{first}/{second}", "reason=not_forwarded"} {
		if !strings.Contains(got[0], want) {
			t.Errorf("fallback line %q lacks %q", got[0], want)
		}
	}
	for _, leaked := range []string{"secret-provider-id", "code-text", "state-text", "token-text"} {
		if strings.Contains(logs.String(), leaked) {
			t.Errorf("the log carries client text %q:\n%s", leaked, logs.String())
		}
	}

	clock = clock.Add(authProxyFallbackLogEvery - time.Second)
	serve()
	if got := lines(); len(got) != 1 {
		t.Fatalf("second local answer inside the window: %d fallback lines, want 1", len(got))
	}
	clock = clock.Add(2 * time.Second)
	serve()
	if got := lines(); len(got) != 2 {
		t.Fatalf("a local answer after the window: %d fallback lines, want 2", len(got))
	}

	logs.Reset()
	off := newAuthProxy(AuthProxyConfig{}, slog.New(slog.NewTextHandler(&logs, nil)))
	off.sessionRoute(AuthOIDC, local).ServeHTTP(httptest.NewRecorder(),
		httptest.NewRequest(http.MethodPost, "/api/v1/auth/oauth/p/callback", strings.NewReader(`{}`)))
	if got := lines(); len(got) != 0 {
		t.Fatalf("flag off: %d fallback lines, want none", len(got))
	}
}
