package session

import (
	"bytes"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/api/oauthprovider"
)

// socialLoginWithProviderID posts a GitHub social login whose profile carries
// the given id (a raw JSON value). pastGuard is true when the handler went on
// to the database (the zero pool panics there), false when it answered first.
func socialLoginWithProviderID(t *testing.T, idJSON string) (code int, body, logs string, pastGuard bool) {
	t.Helper()
	provider := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/user" {
			_, _ = w.Write([]byte(`{"id": ` + idJSON + `, "login": "u", "email": "guard@example.com"}`))
			return
		}
		w.WriteHeader(http.StatusNotFound)
	}))
	t.Cleanup(provider.Close)
	var logBuffer bytes.Buffer
	deps := completeDeps(t)
	deps.Logger = slog.New(slog.NewTextHandler(&logBuffer, nil))
	deps.Getenv = func(string) string { return "set" }
	deps.OAuth = &oauthprovider.Client{HTTP: provider.Client(),
		Endpoints: oauthprovider.Endpoints{GitHubUser: provider.URL + "/user", GitHubEmails: provider.URL + "/emails"}}
	var handler http.Handler
	for _, route := range Routes(deps) {
		if route.Pattern == "/api/v1/auth/social-login" {
			handler = route.Handler
		}
	}
	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodPost, "/api/v1/auth/social-login",
		strings.NewReader(`{"provider": "github", "provider_access_token": "x"}`))
	r.Header.Set("Content-Type", "application/json")
	func() {
		defer func() {
			if recover() != nil {
				pastGuard = true
			}
		}()
		handler.ServeHTTP(w, r)
	}()
	return w.Code, w.Body.String(), logBuffer.String(), pastGuard
}

// CHAOS-11619: a social sign-in whose provider id ValidSubject refuses is
// answered 400 before any database work, and neither the answer nor the log
// carries the id.
func TestSocialLoginRefusesAnInvalidProviderIDBeforeTheDatabase(t *testing.T) {
	for name, idJSON := range map[string]string{
		"empty":           `""`,
		"blank":           `"   "`,
		"edge whitespace": `" MARKER77"`,
		"control":         `"7\u0001MARKER77"`,
	} {
		t.Run(name, func(t *testing.T) {
			code, body, logs, pastGuard := socialLoginWithProviderID(t, idJSON)
			if pastGuard {
				t.Fatal("the handler went on to the database with an invalid provider id")
			}
			if code != http.StatusBadRequest {
				t.Fatalf("status %d body %q, want 400", code, body)
			}
			if strings.Contains(body, "MARKER77") || strings.Contains(logs, "MARKER77") {
				t.Fatalf("the provider id reached the response or the log: %q %q", body, logs)
			}
			if !strings.Contains(logs, "provider=github") {
				t.Fatalf("the refusal log does not name the provider: %q", logs)
			}
		})
	}
}

// A valid id still goes on to the database (control).
func TestSocialLoginGoesOnWithAValidProviderID(t *testing.T) {
	for _, idJSON := range []string{`12345`, `"12345"`} {
		if _, body, _, pastGuard := socialLoginWithProviderID(t, idJSON); !pastGuard {
			t.Fatalf("id %s: the handler answered before the database: %q", idJSON, body)
		}
	}
}
