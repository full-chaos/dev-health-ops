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

// socialLoginWithEmail posts a GitHub social login whose profile carries the given email (a raw JSON string value).
// pastGuard is true when the handler went on to the database (the zero pool panics there).
func socialLoginWithEmail(t *testing.T, emailJSON string) (code int, body, logs string, pastGuard bool) {
	t.Helper()
	provider := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/user" {
			_, _ = w.Write([]byte(`{"id": 7, "login": "u", "email": ` + emailJSON + `}`))
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

// A social sign-in whose provider email the shared email rule refuses is answered 400 before any database work, and
// neither the answer nor the log carries the address.
func TestSocialLoginRefusesAnUnusableProviderEmailBeforeTheDatabase(t *testing.T) {
	for name, emailJSON := range map[string]string{
		"replacement char": `"MARKER77�@example.test"`,
		"nul":              `"MARKER77\u0000@example.test"`,
		"control":          `"MARKER77\u0001@example.test"`,
		"del":              `"MARKER77\u007f@example.test"`,
		"c1":               `"MARKER77\u0085x@example.test"`,
	} {
		t.Run(name, func(t *testing.T) {
			code, body, logs, pastGuard := socialLoginWithEmail(t, emailJSON)
			if pastGuard {
				t.Fatal("the handler went on to the database with an unusable provider email")
			}
			if code != http.StatusBadRequest {
				t.Fatalf("status %d body %q, want 400", code, body)
			}
			if strings.Contains(body, "MARKER77") || strings.Contains(logs, "MARKER77") {
				t.Fatalf("the provider email reached the response or the log: %q %q", body, logs)
			}
			if !strings.Contains(logs, "provider=github") {
				t.Fatalf("the refusal log does not name the provider: %q", logs)
			}
		})
	}
}

// A usable address still goes on to the database (control).
func TestSocialLoginGoesOnWithAUsableProviderEmail(t *testing.T) {
	for _, emailJSON := range []string{`"a@example.test"`, `"\ta@example.test"`, `"jürgen@example.test"`} {
		if _, body, _, pastGuard := socialLoginWithEmail(t, emailJSON); !pastGuard {
			t.Fatalf("email %s: the handler answered before the database: %q", emailJSON, body)
		}
	}
}

// A lone surrogate in the profile email makes the profile unverified (oauthprovider.cleanProfileText): the sign-in is
// answered with the unverified-email 403 before any database work, whether or not an account exists.
func TestSocialLoginAnswersAnUnverifiedBadFormEmailBeforeTheDatabase(t *testing.T) {
	code, body, logs, pastGuard := socialLoginWithEmail(t, `"MARKER77\ud800@example.test"`)
	if pastGuard {
		t.Fatal("the handler went on to the database with an unusable provider email")
	}
	if code != http.StatusForbidden || !strings.Contains(body, providerEmailUnverified) {
		t.Fatalf("status %d body %q, want the 403 unverified-email answer", code, body)
	}
	if strings.Contains(body, "MARKER77") || strings.Contains(logs, "MARKER77") {
		t.Fatalf("the provider email reached the response or the log: %q %q", body, logs)
	}
}
