package session

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/full-chaos/dev-health-ops/internal/api/oauthprovider"
)

// socialLoginWithProfile posts a social login for provider whose profile body is exactly userBody (every provider
// endpoint of the stub serves it). pastGuard is true when the handler went on to the database (the zero pool
// panics there).
func socialLoginWithProfile(t *testing.T, provider, userBody string) (code int, body, logs string, pastGuard bool) {
	t.Helper()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/user", "/gl/api/v4/user", "/google":
			_, _ = w.Write([]byte(userBody))
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	t.Cleanup(server.Close)
	var logBuffer bytes.Buffer
	deps := completeDeps(t)
	deps.Logger = slog.New(slog.NewTextHandler(&logBuffer, nil))
	deps.Getenv = func(string) string { return "set" }
	deps.OAuth = &oauthprovider.Client{HTTP: server.Client(), Endpoints: oauthprovider.Endpoints{
		GitHubUser: server.URL + "/user", GitHubEmails: server.URL + "/emails", GitLabBase: server.URL + "/gl/", GoogleUser: server.URL + "/google"}}
	var handler http.Handler
	for _, route := range Routes(deps) {
		if route.Pattern == "/api/v1/auth/social-login" {
			handler = route.Handler
		}
	}
	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodPost, "/api/v1/auth/social-login",
		strings.NewReader(`{"provider": "`+provider+`", "provider_access_token": "x"}`))
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

// CHAOS-11619: a profile with no "id" key, or a JSON null id, from any provider is answered 400 before any database
// work; neither the answer nor the log carries a value, and the log names the provider.
func TestSocialLoginRefusesAProfileWithoutAUserIDBeforeTheDatabase(t *testing.T) {
	profiles := map[string]func(fields string) string{
		"github": func(fields string) string {
			return `{` + fields + `"login": "LOGINMARK", "email": "guard@example.com"}`
		},
		"gitlab": func(fields string) string {
			return `{` + fields + `"username": "LOGINMARK", "email": "guard@example.com", "confirmed_at": "2026-01-01T00:00:00Z"}`
		},
		"google": func(fields string) string {
			return `{` + fields + `"name": "LOGINMARK", "email": "guard@example.com", "verified_email": true}`
		},
	}
	for provider, profile := range profiles {
		for name, fields := range map[string]string{"no id key": ``, "null id": `"id": null, `} {
			t.Run(provider+"/"+name, func(t *testing.T) {
				code, body, logs, pastGuard := socialLoginWithProfile(t, provider, profile(fields))
				if pastGuard {
					t.Fatal("the handler went on to the database without a provider user id")
				}
				if code != http.StatusBadRequest || body != `{"detail":{"message":"The provider returned an invalid user id"}}` {
					t.Fatalf("status %d body %q, want the 400 invalid user id answer", code, body)
				}
				if !strings.Contains(logs, "provider="+provider) {
					t.Fatalf("the refusal log does not name the provider: %q", logs)
				}
				if strings.Contains(body, "LOGINMARK") || strings.Contains(logs, "LOGINMARK") {
					t.Fatalf("a profile value reached the response or the log: %q %q", body, logs)
				}
			})
		}
	}
}

// A present string "None" is a valid subject (one rule: ValidSubject): the handler goes on (control).
func TestSocialLoginGoesOnWithAPresentIDThatSpellsNone(t *testing.T) {
	if _, body, _, pastGuard := socialLoginWithProfile(t, "gitlab",
		`{"id": "None", "username": "u", "email": "guard@example.com", "confirmed_at": "2026-01-01T00:00:00Z"}`); !pastGuard {
		t.Fatalf("answered before the database: %q", body)
	}
}

// execRecorder is a pgx.Tx that only records Exec.
type execRecorder struct {
	pgx.Tx
	execs int
}

func (r *execRecorder) Exec(context.Context, string, ...any) (pgconn.CommandTag, error) {
	r.execs++
	return pgconn.CommandTag{}, errors.New("stop")
}

// CHAOS-11619: createSocialUser itself refuses an invalid provider id before its INSERT, whoever calls it.
func TestCreateSocialUserRefusesAnInvalidProviderIDBeforeTheInsert(t *testing.T) {
	h := handlers{Deps: completeDeps(t).withDefaults()}
	info := &oauthprovider.UserInfo{}
	for name, id := range map[string]string{"empty": "", "blank": "  ", "edge whitespace": " 7", "control": "7\x01"} {
		tx := &execRecorder{}
		if _, err := h.createSocialUser(context.Background(), tx, info, "a@example.test", "github", id); err == nil {
			t.Errorf("%s: no refusal", name)
		}
		if tx.execs != 0 {
			t.Errorf("%s: %d statements ran, want none", name, tx.execs)
		}
	}
	tx := &execRecorder{}
	_, _ = h.createSocialUser(context.Background(), tx, info, "a@example.test", "github", "12345")
	if tx.execs != 1 {
		t.Errorf("valid id: %d statements ran, want the one INSERT", tx.execs)
	}
}
