package authruntime

import (
	"context"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/auth/authconfig"
	"github.com/full-chaos/dev-health-ops/internal/auth/httpapi"
	"github.com/full-chaos/dev-health-ops/internal/platform/health"
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
