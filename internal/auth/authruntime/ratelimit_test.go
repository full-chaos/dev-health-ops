package authruntime

import (
	"bytes"
	"context"
	"io"
	"log/slog"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/auth/authconfig"
	"github.com/full-chaos/dev-health-ops/internal/auth/httpapi"
	"github.com/full-chaos/dev-health-ops/internal/platform/health"
	"github.com/full-chaos/dev-health-ops/internal/storage/postgres/authschema"
)

func limitConfig(t *testing.T, env map[string]string) authconfig.Config {
	t.Helper()
	env[authconfig.EnvDatabaseURI] = "postgres://auth@127.0.0.1:1/devhealth"
	env[authconfig.EnvSigningKeyFile] = "/nonexistent"
	env[authconfig.EnvSigningKeyID] = "limits-test"
	cfg, err := authconfig.Load(authconfig.Spec{LookupEnv: lookupFrom(env)})
	if err != nil {
		t.Fatal(err)
	}
	return cfg
}

func discard() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

// Roll N+1 changes no mount set: no limited route, so no store, no client and
// no readiness check, with or without a URI in the environment.
func TestNoStoreIsBuiltWhileNoLimitedRouteMounts(t *testing.T) {
	for name, env := range map[string]map[string]string{
		"dormant, no URI":     {},
		"dormant, URI set":    {authconfig.EnvValkeyURI: testValkeyURI},
		"session routes only": sessionEnvironment(map[string]string{}),
		"session routes, URI absent": func() map[string]string {
			e := sessionEnvironment(map[string]string{})
			delete(e, authconfig.EnvValkeyURI)
			return e
		}(),
		"discovery": {authconfig.EnvTokenIssuerURL: "https://auth.example.com"},
	} {
		cfg := limitConfig(t, env)
		registry := health.NewRegistry(time.Second)
		limits, components, err := limitStore(cfg, discard(), registry)
		if err != nil || limits != nil || len(components) != 0 {
			t.Errorf("%s: limits %v, %d components, err %v; want none", name, limits, len(components), err)
		}
	}
}

// Nothing dials at startup: an unreachable store leaves the process up, and
// the first count answers a store error, which the routes turn into a closed 500.
func TestTheStoreDialsOnFirstUseAndAnUnreachableOneIsAnErrorNotAPass(t *testing.T) {
	cfg := limitConfig(t, compatEnvironment(map[string]string{}))
	registry := health.NewRegistry(time.Second)
	started := time.Now()
	limits, components, err := limitStore(cfg, discard(), registry)
	if err != nil || limits == nil || len(components) != 1 {
		t.Fatalf("limits %v, %d components, err %v", limits, len(components), err)
	}
	store, ok := components[0].(*lazyStore)
	if !ok {
		t.Fatalf("component %T, want the lazy store", components[0])
	}
	if time.Since(started) > time.Second {
		t.Fatal("building the store took a dial's time")
	}
	if store.Backend() != "redis" {
		t.Fatalf("backend %q", store.Backend())
	}
	if got := registry.RequiredCount(); got != 1 {
		t.Fatalf("%d required checks, want the one valkey check", got)
	}
	if ready := registry.CheckRequired(context.Background()); ready.Ready || len(ready.Failed) != 1 || ready.Failed[0] != CheckValkey {
		t.Fatalf("readiness %+v, want it failed closed on %q", ready, CheckValkey)
	}
	hit := httpapi.Hit{Limit: httpapi.Limit{ID: "auth_svc_login_ip", Count: 1, Window: time.Minute}, Key: "k", Path: "p"}
	if _, err := store.Increment(context.Background(), hit); err == nil {
		t.Fatal("a count against an unreachable store succeeded")
	}
	if _, err := store.Peek(context.Background(), hit); err == nil {
		t.Fatal("a peek against an unreachable store succeeded")
	}
	if err := components[0].Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
}

// The store-error counter is on the operator /metrics the alert reads: every
// limiter series is present at zero once the limited routes are built, and a
// service with no limited route publishes none of them.
func TestTheStoreErrorCounterIsOnTheServiceScrape(t *testing.T) {
	env := compatEnvironment(map[string]string{authconfig.EnvUsersOwned: "true", authconfig.EnvSocialLinkRoutes: "true"})
	cfg := limitConfig(t, env)
	pool, err := pgxpool.New(context.Background(), "postgres://auth@127.0.0.1:1/devhealth")
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	schema, err := authschema.NewValidatedIdentifier("auth")
	if err != nil {
		t.Fatal(err)
	}
	registry := health.NewRegistry(time.Second)
	limits, components, err := limitStore(cfg, discard(), registry)
	if err != nil || limits == nil {
		t.Fatalf("limits %v, err %v", limits, err)
	}
	defer components[0].Shutdown(context.Background())
	if _, err := sessionRoutes(cfg, discard(), pool, schema, nil, limits); err != nil {
		t.Fatal(err)
	}
	_, credentialComponents, err := credentialRoutes(cfg, discard(), pool, schema, limits)
	if err != nil {
		t.Fatal(err)
	}
	for _, component := range credentialComponents {
		defer component.Shutdown(context.Background())
	}
	var scrape bytes.Buffer
	if err := registry.WriteMetrics(&scrape); err != nil {
		t.Fatal(err)
	}
	ids := []string{
		"auth_svc_login_ip", "auth_svc_login_ip_account", "auth_svc_verify", "auth_svc_resend_verification",
		"auth_svc_resend_verification_ip", "auth_svc_forgot_password", "auth_svc_forgot_password_ip",
		"auth_svc_reset_ip", "auth_svc_reset_ip_token", "auth_svc_change_password", "auth_svc_social_link_ip_principal",
	}
	for _, id := range ids {
		sample := regexp.MustCompile(`(?m)^dev_health_api_rate_limit_store_errors_total\{limit="` + id + `"\} 0$`)
		if !sample.MatchString(scrape.String()) {
			t.Errorf("the scrape carries no zero sample for limit %s", id)
		}
	}

	quiet := health.NewRegistry(time.Second)
	if limits, _, err := limitStore(limitConfig(t, sessionEnvironment(map[string]string{})), discard(), quiet); err != nil || limits != nil {
		t.Fatalf("session routes only: limits %v, err %v", limits, err)
	}
	var none bytes.Buffer
	if err := quiet.WriteMetrics(&none); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(none.String(), "rate_limit_store_errors") {
		t.Error("a service with no limited route publishes the store-error counter")
	}
}
