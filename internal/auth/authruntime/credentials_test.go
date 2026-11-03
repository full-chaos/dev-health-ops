package authruntime

import (
	"bytes"
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
	"github.com/full-chaos/dev-health-ops/internal/mail"
	"github.com/full-chaos/dev-health-ops/internal/storage/postgres/authschema"
)

func credentialConfig(t *testing.T, owned string) authconfig.Config {
	t.Helper()
	env := compatEnvironment(map[string]string{
		authconfig.EnvDatabaseURI:    "postgres://auth@127.0.0.1:1/devhealth",
		authconfig.EnvSigningKeyFile: "/nonexistent",
		authconfig.EnvSigningKeyID:   "credentials-test",
	})
	if owned != "" {
		env[authconfig.EnvUsersOwned] = owned
	}
	cfg, err := authconfig.Load(authconfig.Spec{LookupEnv: lookupFrom(env)})
	if err != nil {
		t.Fatal(err)
	}
	return cfg
}

func TestCredentialRoutesOwnedUnchanged(t *testing.T) {
	pool, err := pgxpool.New(context.Background(), "postgres://auth@127.0.0.1:1/devhealth")
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	schema, err := authschema.NewValidatedIdentifier("auth")
	if err != nil {
		t.Fatal(err)
	}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	routes, components, err := credentialRoutes(credentialConfig(t, "true"), logger, pool, schema, mustLimits(t), nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(routes) != 6 || len(components) != 1 {
		t.Fatalf("routes %d components %d; want 6 and 1", len(routes), len(components))
	}
	if err := components[0].Shutdown(context.Background()); err != nil {
		t.Fatalf("link-mail shutdown: %v", err)
	}
	if err := components[0].Shutdown(context.Background()); err != nil {
		t.Fatalf("second shutdown: %v", err)
	}
}

func TestTheConsoleProviderNeverLogsTheRecipient(t *testing.T) {
	var logs bytes.Buffer
	previous := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&logs, nil)))
	defer slog.SetDefault(previous)
	t.Setenv("EMAIL_PROVIDER", "console")
	provider, err := mail.NewSenderFromEnv(nil)
	if err != nil {
		t.Fatal(err)
	}
	if provider.Name() != "console" {
		t.Skip("the test environment selects another provider")
	}
	if err := quietProvider(provider).Send(context.Background(), mail.Message{To: "probe-address@example.com", Subject: "s", HTML: "h"}); err != nil {
		t.Fatal(err)
	}
	if logs.Len() == 0 || bytes.Contains(logs.Bytes(), []byte("probe-address")) {
		t.Fatalf("console provider output %q: want a line without the address", logs.String())
	}
}

type probeCounters struct {
	t     *testing.T
	calls int
}

func (p *probeCounters) Increment(context.Context, httpapi.Hit) (int64, error) {
	p.calls++
	p.t.Error("a not-owned credential route reached the limiter")
	return 0, nil
}

func (p *probeCounters) Peek(context.Context, httpapi.Hit) (int64, error) {
	p.calls++
	p.t.Error("a not-owned credential route reached the limiter")
	return 0, nil
}

func (p *probeCounters) Backend() string { return "probe" }

func TestCredentialRoutesNotOwnedWriteNothingAndNeedNoMailProvider(t *testing.T) {
	probe := &probeCounters{t: t}
	limits, err := authapi.NewLimits(probe, testAccessSecret)
	if err != nil {
		t.Fatal(err)
	}
	schema, err := authschema.NewValidatedIdentifier("auth")
	if err != nil {
		t.Fatal(err)
	}
	t.Setenv("MAIL_PROVIDER", "a-provider-that-does-not-exist")
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	for name, owned := range map[string]string{"unset": "", "false": "false"} {
		routes, components, err := credentialRoutes(credentialConfig(t, owned), logger, nil, schema, limits, nil)
		if err != nil || len(components) != 0 || len(routes) != 6 {
			t.Fatalf("%s: routes %d components %d err %v; want 6, 0, nil", name, len(routes), len(components), err)
		}
		for _, route := range routes {
			rec := httptest.NewRecorder()
			route.Handler.ServeHTTP(rec, httptest.NewRequest(route.Method, route.Pattern, strings.NewReader("{}")))
			if rec.Code != http.StatusServiceUnavailable || rec.Header().Get("Retry-After") != "1" {
				t.Fatalf("%s %s %s: status %d Retry-After %q; want 503 and 1", name, route.Method, route.Pattern, rec.Code, rec.Header().Get("Retry-After"))
			}
		}
	}
	if probe.calls != 0 {
		t.Fatalf("limiter store hit %d times", probe.calls)
	}
}

func TestNotOwnedCredentialRoutesAnswer503ThroughTheServerUnderLoad(t *testing.T) {
	schema, err := authschema.NewValidatedIdentifier("auth")
	if err != nil {
		t.Fatal(err)
	}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	for name, owned := range map[string]string{"unset": "", "false": "false"} {
		cfg := credentialConfig(t, owned)
		routes, _, err := credentialRoutes(cfg, logger, nil, schema, mustLimits(t), nil)
		if err != nil {
			t.Fatal(err)
		}
		release, entered := make(chan struct{}), make(chan struct{}, 1)
		routes = append(routes, httpapi.Route{Method: http.MethodGet, Pattern: "/v1/test-hold", Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			entered <- struct{}{}
			<-release
		})})
		options := apiServerOptions(cfg, logger, routes)
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
			for _, route := range routes[:6] {
				req := httptest.NewRequest(route.Method, route.Pattern, strings.NewReader("{}"))
				req.ContentLength = 1 << 30
				rec := httptest.NewRecorder()
				server.Handler().ServeHTTP(rec, req)
				if rec.Code != http.StatusServiceUnavailable || rec.Header().Get("Retry-After") != "1" {
					t.Fatalf("%s round %d %s %s: %d Retry-After %q; want 503 and 1", name, round, route.Method, route.Pattern, rec.Code, rec.Header().Get("Retry-After"))
				}
			}
		}
		close(release)
		<-done
	}
}

func TestOnlyTheSixNotOwnedCredentialRoutesAreUnthrottled(t *testing.T) {
	want := map[string]bool{
		"GET /api/v1/auth/verify": true, "POST /api/v1/auth/resend-verification": true,
		"POST /api/v1/auth/forgot-password": true, "POST /api/v1/auth/reset-password": true,
		"POST /api/v1/auth/change-password": true, "POST /api/v1/auth/register": true,
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
	census := func(label string, routes []httpapi.Route, expect bool) {
		t.Helper()
		got := map[string]bool{}
		for _, route := range routes {
			if route.Unthrottled {
				got[route.Method+" "+route.Pattern] = true
			}
		}
		if !expect {
			if len(got) != 0 {
				t.Fatalf("%s: unthrottled routes %v; want none", label, got)
			}
			return
		}
		if len(got) != len(want) {
			t.Fatalf("%s: unthrottled %v; want exactly %v", label, got, want)
		}
		for key := range want {
			if !got[key] {
				t.Fatalf("%s: %s is not unthrottled", label, key)
			}
		}
	}
	for name, owned := range map[string]string{"unset": "", "false": "false", "true": "true"} {
		cfg := credentialConfig(t, owned)
		session, err := sessionRoutes(cfg, logger, pool, schema, nil, mustLimits(t))
		if err != nil {
			t.Fatal(err)
		}
		credentials, _, err := credentialRoutes(cfg, logger, pool, schema, mustLimits(t), nil)
		if err != nil {
			t.Fatal(err)
		}
		census(name+" session", session, false)
		census(name+" credentials", credentials, owned != "true")
	}
	census("parity set", parityRoutes(t), false)
}
