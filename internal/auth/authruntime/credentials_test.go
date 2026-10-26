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
	routes, components, err := credentialRoutes(credentialConfig(t, "true"), logger, pool, schema, mustLimits(t))
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
		routes, components, err := credentialRoutes(credentialConfig(t, owned), logger, nil, schema, limits)
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
