package authruntime

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/auth/authapi"
	"github.com/full-chaos/dev-health-ops/internal/auth/authconfig"
	"github.com/full-chaos/dev-health-ops/internal/auth/httpapi"
	"github.com/full-chaos/dev-health-ops/internal/storage/postgres/authschema"
)

const loginPattern = "/api/v1/auth/login"

func loginConfig(t *testing.T, lockout bool) authconfig.Config {
	t.Helper()
	env := map[string]string{
		authconfig.EnvDatabaseURI:    "postgres://auth@127.0.0.1:1/devhealth",
		authconfig.EnvSigningKeyFile: "/nonexistent",
		authconfig.EnvSigningKeyID:   "login-stub-test",
	}
	if lockout {
		env = compatEnvironment(env)
	} else {
		env = sessionEnvironment(env)
	}
	cfg, err := authconfig.Load(authconfig.Spec{LookupEnv: lookupFrom(env)})
	if err != nil {
		t.Fatal(err)
	}
	return cfg
}

func loginSessionRoutes(t *testing.T, cfg authconfig.Config, limits *authapi.Limits) []httpapi.Route {
	t.Helper()
	pool, err := pgxpool.New(context.Background(), "postgres://auth@127.0.0.1:1/devhealth")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	schema, err := authschema.NewValidatedIdentifier("auth")
	if err != nil {
		t.Fatal(err)
	}
	routes, err := sessionRoutes(cfg, slog.New(slog.NewTextHandler(io.Discard, nil)), pool, schema, nil, limits)
	if err != nil {
		t.Fatal(err)
	}
	return routes
}

func loginRoutesOf(routes []httpapi.Route) []httpapi.Route {
	var out []httpapi.Route
	for _, route := range routes {
		if route.Method == http.MethodPost && route.Pattern == loginPattern {
			out = append(out, route)
		}
	}
	return out
}

type loginProbe struct{ t *testing.T }

func (p loginProbe) Increment(context.Context, httpapi.Hit) (int64, error) {
	p.t.Error("the login stub reached the limiter")
	return 0, nil
}

func (p loginProbe) Peek(context.Context, httpapi.Hit) (int64, error) {
	p.t.Error("the login stub reached the limiter")
	return 0, nil
}

func (p loginProbe) Backend() string { return "probe" }

func TestRuntimeMountsLoginStubWhileLockoutKeyUnset(t *testing.T) {
	limits, err := authapi.NewLimits(loginProbe{t}, testAccessSecret)
	if err != nil {
		t.Fatal(err)
	}
	cfg := loginConfig(t, false)
	if cfg.Session.LoginEnabled {
		t.Fatal("test config enables login")
	}
	logins := loginRoutesOf(loginSessionRoutes(t, cfg, limits))
	if len(logins) != 1 || !logins[0].Unthrottled {
		t.Fatalf("%d login routes; want exactly the one unthrottled stub", len(logins))
	}
	rec := httptest.NewRecorder()
	logins[0].Handler.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, loginPattern, strings.NewReader(`{"email":"a@example.com","password":"x"}`)))
	if rec.Code != http.StatusServiceUnavailable || rec.Header().Get("Retry-After") != "1" {
		t.Fatalf("status %d Retry-After %q; want 503 and 1", rec.Code, rec.Header().Get("Retry-After"))
	}
}

func TestRuntimeLoginKeyOnMountsRealHandler(t *testing.T) {
	logins := loginRoutesOf(loginSessionRoutes(t, loginConfig(t, true), mustLimits(t)))
	if len(logins) != 1 || logins[0].Unthrottled {
		t.Fatalf("%d login routes (first unthrottled=%v); want exactly the one throttled real route", len(logins), len(logins) == 1 && logins[0].Unthrottled)
	}
	rec := httptest.NewRecorder()
	logins[0].Handler.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, loginPattern, strings.NewReader("{}")))
	if rec.Header().Get("Retry-After") == "1" && rec.Code == http.StatusServiceUnavailable &&
		strings.Contains(rec.Body.String(), "unavailable") && rec.Body.Len() < 100 {
		t.Fatal("the real login route answered the stub's body")
	}
}

func TestRuntimeSessionDisabledMountsOnlyTheRefreshLogoutStubs(t *testing.T) {
	routes, err := sessionRoutes(authconfig.Config{}, slog.New(slog.NewTextHandler(io.Discard, nil)), nil, authschema.ValidatedIdentifier{}, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	var got []string
	for _, route := range routes {
		got = append(got, route.Method+" "+route.Pattern)
	}
	if want := "POST /api/v1/auth/refresh,POST /api/v1/auth/logout"; strings.Join(got, ",") != want {
		t.Fatalf("routes %v; want exactly %s", got, want)
	}
}

func TestLoginStubAnswers503ThroughTheServerUnderLoad(t *testing.T) {
	cfg := loginConfig(t, false)
	routes := loginSessionRoutes(t, cfg, mustLimits(t))
	release, entered := make(chan struct{}), make(chan struct{}, 1)
	routes = append(routes, httpapi.Route{Method: http.MethodGet, Pattern: "/v1/test-hold", Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		entered <- struct{}{}
		<-release
	})})
	options := apiServerOptions(cfg, slog.New(slog.NewTextHandler(io.Discard, nil)), routes)
	options.RateLimit, options.RateLimitBurst, options.MaxInFlight = 0.001, 1, 1
	options.Address = "127.0.0.1:0"
	server, err := httpapi.NewServer(options)
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{})
	go func() {
		server.Handler().ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/v1/test-hold", nil))
		close(done)
	}()
	<-entered
	for round := 0; round < 4; round++ {
		req := httptest.NewRequest(http.MethodPost, loginPattern, strings.NewReader("{}"))
		req.ContentLength = 1 << 30
		rec := httptest.NewRecorder()
		server.Handler().ServeHTTP(rec, req)
		if rec.Code != http.StatusServiceUnavailable || rec.Header().Get("Retry-After") != "1" {
			t.Fatalf("round %d: %d Retry-After %q; want 503 and 1", round, rec.Code, rec.Header().Get("Retry-After"))
		}
	}
	close(release)
	<-done
}

func TestOnlyTheLoginPairIsUnthrottledWhileLoginIsNotEnabled(t *testing.T) {
	for name, lockout := range map[string]bool{"lockout unset": false, "lockout set": true} {
		got := map[string]bool{}
		for _, route := range loginSessionRoutes(t, loginConfig(t, lockout), mustLimits(t)) {
			if route.Unthrottled {
				got[route.Method+" "+route.Pattern] = true
			}
		}
		if lockout && len(got) != 0 {
			t.Fatalf("%s: unthrottled %v; want none", name, got)
		}
		if !lockout && (len(got) != 1 || !got["POST "+loginPattern]) {
			t.Fatalf("%s: unthrottled %v; want exactly POST %s", name, got, loginPattern)
		}
	}
}

func TestUnthrottledSetOfTheFullRouteSetIsExactlyTheStubs(t *testing.T) {
	notOwned := []string{
		"GET /api/v1/auth/verify", "POST /api/v1/auth/resend-verification", "POST /api/v1/auth/forgot-password",
		"POST /api/v1/auth/reset-password", "POST /api/v1/auth/change-password", "POST /api/v1/auth/register",
	}
	schema, err := authschema.NewValidatedIdentifier("auth")
	if err != nil {
		t.Fatal(err)
	}
	pool, err := pgxpool.New(context.Background(), "postgres://auth@127.0.0.1:1/devhealth")
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	for _, owned := range []string{"", "false", "true"} {
		for _, lockout := range []bool{false, true} {
			env := map[string]string{
				authconfig.EnvDatabaseURI:    "postgres://auth@127.0.0.1:1/devhealth",
				authconfig.EnvSigningKeyFile: "/nonexistent",
				authconfig.EnvSigningKeyID:   "full-census-test",
			}
			if lockout {
				env = compatEnvironment(env)
			} else {
				env = sessionEnvironment(env)
			}
			if owned != "" {
				env[authconfig.EnvUsersOwned] = owned
			}
			cfg, err := authconfig.Load(authconfig.Spec{LookupEnv: lookupFrom(env)})
			if err != nil {
				t.Fatal(err)
			}
			session, err := sessionRoutes(cfg, logger, pool, schema, nil, mustLimits(t))
			if err != nil {
				t.Fatal(err)
			}
			credentials, _, err := credentialRoutes(cfg, logger, pool, schema, mustLimits(t), nil)
			if err != nil {
				t.Fatal(err)
			}
			want := map[string]bool{}
			if owned != "true" {
				for _, key := range notOwned {
					want[key] = true
				}
			}
			if !lockout {
				want["POST "+loginPattern] = true
			}
			got := map[string]bool{}
			for _, route := range append(append([]httpapi.Route{}, session...), credentials...) {
				if route.Unthrottled {
					got[route.Method+" "+route.Pattern] = true
				}
			}
			label := "owned=" + owned + " lockout=" + map[bool]string{false: "unset", true: "set"}[lockout]
			if len(got) != len(want) {
				t.Fatalf("%s: unthrottled %v; want exactly %v", label, got, want)
			}
			for key := range want {
				if !got[key] {
					t.Fatalf("%s: %s is not unthrottled; got %v", label, key, got)
				}
			}
		}
	}
}
