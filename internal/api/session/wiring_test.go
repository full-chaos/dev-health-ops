package session

import (
	"bytes"
	"errors"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/auth/edgetoken"
)

func completeDeps(t *testing.T) Deps {
	t.Helper()
	verifier, err := edgetoken.New("wiring-test-signing-key-0123456789abcdef", "dev-health-ops", "dev-health-api")
	if err != nil {
		t.Fatal(err)
	}
	signer, err := edgetoken.NewSigner("wiring-test-signing-key-0123456789abcdef", "dev-health-ops", "dev-health-api")
	if err != nil {
		t.Fatal(err)
	}
	auth := &policy.Authenticator{}
	return Deps{Pool: &pgxpool.Pool{}, Guard: policy.NewGuard(auth, slog.New(slog.DiscardHandler)), Auth: auth,
		Verifier: verifier, Signer: signer}
}

// Routes mounts the eight routes only when every one of its five required
// dependencies is present; each missing one alone leaves nothing mounted.
func TestRoutesNeedEveryRequiredDependency(t *testing.T) {
	if got := len(Routes(completeDeps(t))); got != 8 {
		t.Fatalf("complete deps mount %d routes, want 8", got)
	}
	for name, drop := range map[string]func(*Deps){
		"Pool":     func(d *Deps) { d.Pool = nil },
		"Guard":    func(d *Deps) { d.Guard = nil },
		"Auth":     func(d *Deps) { d.Auth = nil },
		"Verifier": func(d *Deps) { d.Verifier = nil },
		"Signer":   func(d *Deps) { d.Signer = nil },
	} {
		deps := completeDeps(t)
		drop(&deps)
		if got := Routes(deps); got != nil {
			t.Errorf("without %s: %d routes mounted, want none", name, len(got))
		}
	}
}

func TestWithDefaultsFillsEveryOptionalDependency(t *testing.T) {
	d := Deps{}.withDefaults()
	if d.Getenv == nil || d.Audit == nil || d.Now == nil || d.NewUUID == nil || d.Logger == nil || d.OAuth == nil || d.Limits == nil || d.Write == nil {
		t.Fatalf("a default is missing: %+v", d)
	}
	logger := slog.New(slog.DiscardHandler)
	if kept := (Deps{Logger: logger}).withDefaults().Logger; kept != logger {
		t.Fatal("a given logger was replaced")
	}
}

// fail writes the bare 500 and logs the step, with the error text only
// when there is an error.
func TestFailLogsTheStepAndWritesTheBare500(t *testing.T) {
	for _, tc := range []struct {
		err       error
		wantError bool
	}{{nil, false}, {errors.New("boom"), true}} {
		var logs bytes.Buffer
		h := handlers{Deps: Deps{Logger: slog.New(slog.NewTextHandler(&logs, nil))}}
		w := httptest.NewRecorder()
		h.fail(w, httptest.NewRequest(http.MethodPost, "/api/v1/auth/login", nil), "load user", tc.err)
		if w.Code != http.StatusInternalServerError || w.Body.String() != `{"detail":"Internal Server Error"}` {
			t.Fatalf("err %v: %d %q", tc.err, w.Code, w.Body.String())
		}
		line := logs.String()
		if !strings.Contains(line, "step=\"load user\"") || strings.Contains(line, "error=") != tc.wantError {
			t.Fatalf("err %v: log %q", tc.err, line)
		}
	}
}

// clientHost is Starlette's request.client.host.
func TestClientHost(t *testing.T) {
	for remote, want := range map[string]string{"": "<nil>", "198.51.100.4:5000": "198.51.100.4",
		"[2001:db8::1]:443": "2001:db8::1", "unix-peer": "unix-peer"} {
		r := httptest.NewRequest(http.MethodGet, "/", nil)
		r.RemoteAddr = remote
		got := "<nil>"
		if host := clientHost(r); host != nil {
			got = *host
		}
		if got != want {
			t.Errorf("%q: %q, want %q", remote, got, want)
		}
	}
}

// oauth.py answers 503 unless BOTH of the provider's client settings are
// non-empty; the check runs before the provider or the database is used.
func TestSocialLoginNeedsBothClientSettings(t *testing.T) {
	for name, env := range map[string]map[string]string{
		"neither":     {},
		"id only":     {"SOCIAL_GOOGLE_CLIENT_ID": "id"},
		"secret only": {"SOCIAL_GOOGLE_CLIENT_SECRET": "secret"},
		"empty id":    {"SOCIAL_GOOGLE_CLIENT_ID": "", "SOCIAL_GOOGLE_CLIENT_SECRET": "secret"},
	} {
		deps := completeDeps(t)
		deps.Getenv = func(key string) string { return env[key] }
		var handler http.Handler
		for _, route := range Routes(deps) {
			if route.Pattern == "/api/v1/auth/social-login" {
				handler = route.Handler
			}
		}
		w := httptest.NewRecorder()
		r := httptest.NewRequest(http.MethodPost, "/api/v1/auth/social-login",
			strings.NewReader(`{"provider": "google", "provider_access_token": "x"}`))
		r.Header.Set("Content-Type", "application/json")
		handler.ServeHTTP(w, r)
		if want := `{"detail":{"message":"Social login not configured for google"}}`; w.Code != http.StatusServiceUnavailable || w.Body.String() != want {
			t.Errorf("%s: %d %s", name, w.Code, w.Body.String())
		}
	}
}
