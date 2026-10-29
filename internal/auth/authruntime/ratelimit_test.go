package authruntime

import (
	"bytes"
	"context"
	"io"
	"log/slog"
	"net"
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
		"dormant, no URI":  {},
		"dormant, URI set": {authconfig.EnvValkeyURI: testValkeyURI},
		"discovery":        {authconfig.EnvTokenIssuerURL: "https://auth.example.com"},
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
// The compatibility refresh route counts in the store, so session routes alone build it.
func TestSessionRoutesAloneBuildTheStoreForTheRefreshLimit(t *testing.T) {
	registry := health.NewRegistry(time.Second)
	limits, components, err := limitStore(limitConfig(t, sessionEnvironment(map[string]string{})), discard(), registry)
	if err != nil || limits == nil || len(components) != 1 {
		t.Fatalf("limits %v, %d components, err %v; want the store", limits, len(components), err)
	}
	defer components[0].Shutdown(context.Background())
	if registry.RequiredCount() != 1 {
		t.Fatalf("%d required checks, want the valkey check", registry.RequiredCount())
	}
}

func TestTheStoreDialsOnFirstUseAndAnUnreachableOneIsAnErrorNotAPass(t *testing.T) {
	// The store's address is a listener the test owns. Building the store must not dial it: after the build the test dials a probe
	// connection, and the first connection the listener accepts must be the probe (the accept queue is in connection order).
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	accepted := make(chan net.Conn, 16)
	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			accepted <- conn
		}
	}()
	env := compatEnvironment(map[string]string{})
	env[authconfig.EnvValkeyURI] = "redis://auth-ratelimit:test-only-password@" + listener.Addr().String() + "/1"
	cfg := limitConfig(t, env)
	registry := health.NewRegistry(time.Second)
	limits, components, err := limitStore(cfg, discard(), registry)
	if err != nil || limits == nil || len(components) != 1 {
		t.Fatalf("limits %v, %d components, err %v", limits, len(components), err)
	}
	store, ok := components[0].(*lazyStore)
	if !ok {
		t.Fatalf("component %T, want the lazy store", components[0])
	}
	probe, err := net.Dial("tcp", listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer probe.Close()
	// The wait below is a guard for the failure path only; a passing run never reaches it.
	select {
	case first := <-accepted:
		if first.RemoteAddr().String() != probe.LocalAddr().String() {
			t.Fatal("building the store dialled the server: the store must dial on first use")
		}
		_ = first.Close()
	case <-time.After(30 * time.Second):
		t.Fatal("the probe connection was never accepted")
	}
	if store.client != nil || store.store != nil {
		t.Fatal("building the store opened a client: the store must open on first use")
	}
	// From here the store's address is a closed port: unreachable.
	_ = listener.Close()
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
	_, credentialComponents, err := credentialRoutes(cfg, discard(), pool, schema, limits, nil)
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
		"auth_svc_refresh",
	}
	for _, id := range ids {
		sample := regexp.MustCompile(`(?m)^dev_health_api_rate_limit_store_errors_total\{limit="` + id + `"\} \d+$`)
		if !sample.MatchString(scrape.String()) {
			t.Errorf("the scrape carries no zero sample for limit %s", id)
		}
	}

	quiet := health.NewRegistry(time.Second)
	if limits, _, err := limitStore(limitConfig(t, map[string]string{}), discard(), quiet); err != nil || limits != nil {
		t.Fatalf("no session routes: limits %v, err %v", limits, err)
	}
	var none bytes.Buffer
	if err := quiet.WriteMetrics(&none); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(none.String(), "rate_limit_store_errors") {
		t.Error("a service with no limited route publishes the store-error counter")
	}
}

// Roll 18 ships the code with the store's Secret absent and no limited route
// mounted (prod mounts the JWKS and the discovery document only): the service
// must boot, hold no store component, and require no "valkey" readiness check.
func TestTheDormantMountSetBootsWithoutTheStoreSecret(t *testing.T) {
	boot := func(env map[string]string) (names []string, required int) {
		t.Helper()
		env[authconfig.EnvDatabaseURI] = "postgres://auth@127.0.0.1:1/devhealth"
		env[authconfig.EnvSigningKeyFile] = "/nonexistent"
		env[authconfig.EnvSigningKeyID] = "boot-test"
		env[authconfig.EnvAPIAddress] = "127.0.0.1:0"
		env[authconfig.EnvOperatorAddress] = "127.0.0.1:1"
		env[authconfig.EnvDatabaseConnectTimeout] = "1s"
		env[authconfig.EnvTokenIssuerURL] = "https://auth.example.com"
		cfg, err := authconfig.Load(authconfig.Spec{LookupEnv: lookupFrom(env)})
		if err != nil {
			t.Fatalf("load: %v", err)
		}
		components, err := configureWithRoutes(context.Background(), cfg, discard(), nil)
		if err != nil {
			t.Fatalf("configure: %v", err)
		}
		t.Cleanup(func() {
			for _, component := range components {
				_ = component.Shutdown(context.Background())
			}
		})
		for _, component := range components {
			names = append(names, component.Name())
			if gate, ok := component.(health.Gate); ok {
				required = gate.Registry.RequiredCount()
			}
		}
		return names, required
	}
	names, required := boot(map[string]string{})
	for _, name := range names {
		if strings.Contains(name, "valkey") {
			t.Errorf("a dormant service built the component %q", name)
		}
	}
	if required == 0 {
		t.Fatal("no readiness gate found: the boot did not reach the end of configure")
	}
	// A value in AUTH_VALKEY_URI beside no limited route changes nothing.
	withURI := map[string]string{authconfig.EnvValkeyURI: testValkeyURI}
	namesURI, requiredURI := boot(withURI)
	for _, name := range namesURI {
		if strings.Contains(name, "valkey") {
			t.Errorf("a service with no limited route built the component %q", name)
		}
	}
	if requiredURI != required {
		t.Errorf("a service with no limited route requires %d checks, the dormant one %d: the valkey check must be required only while a limited route mounts", requiredURI, required)
	}
	// With a limited route the store IS required: one more check and one component.
	limited := compatEnvironment(map[string]string{})
	namesLimited, requiredLimited := boot(limited)
	if requiredLimited != required+1 {
		t.Errorf("a service with a limited route requires %d checks, want %d", requiredLimited, required+1)
	}
	found := false
	for _, name := range namesLimited {
		found = found || name == "auth-valkey"
	}
	if !found {
		t.Error("a service with a limited route has no store component")
	}
}
