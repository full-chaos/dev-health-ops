package authruntime

import (
	"context"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/auth/authconfig"
	"github.com/full-chaos/dev-health-ops/internal/storage/postgres/authschema"
)

func feedFixture(t *testing.T, env map[string]string) (authconfig.Config, *pgxpool.Pool, authschema.ValidatedIdentifier) {
	t.Helper()
	cfg := limitConfig(t, env)
	pool, err := pgxpool.New(context.Background(), "postgres://auth@127.0.0.1:1/devhealth")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	schema, err := authschema.NewValidatedIdentifier("auth")
	if err != nil {
		t.Fatal(err)
	}
	return cfg, pool, schema
}

// Dark by default: with the switch off (or unset) nothing is built, nothing dials, and the outbox loop keeps the nil
// publisher it has today (publishing disabled, the reaper runs).
func TestTheRevisionFeedOffBuildsNothing(t *testing.T) {
	for name, env := range map[string]map[string]string{
		"unset":               {authconfig.EnvValkeyURI: testValkeyURI},
		"unset, no URI":       {},
		"off":                 {authconfig.EnvValkeyURI: testValkeyURI, authconfig.EnvRevisionFeed: "off"},
		"off, session routes": sessionEnvironment(map[string]string{authconfig.EnvRevisionFeed: "off"}),
	} {
		t.Run(name, func(t *testing.T) {
			cfg, pool, schema := feedFixture(t, env)
			publisher, components, err := revisionFeed(cfg, discard(), pool, schema)
			if err != nil || publisher != nil || len(components) != 0 {
				t.Fatalf("publisher %v, %d components, err %v; want none", publisher, len(components), err)
			}
		})
	}
}

// On: a publisher with the Valkey sink and one subscriber component, built without dialling (an unreachable Valkey
// leaves the process up, as the rate-limit store does).
func TestTheRevisionFeedOnBuildsAPublisherAndASubscriberWithoutDialling(t *testing.T) {
	cfg, pool, schema := feedFixture(t, map[string]string{
		authconfig.EnvValkeyURI: "redis://auth-service-ratelimit:SENTINEL_PW@127.0.0.1:1/1", authconfig.EnvRevisionFeed: "on",
	})
	publisher, components, err := revisionFeed(cfg, discard(), pool, schema)
	if err != nil {
		t.Fatal(err)
	}
	if publisher == nil {
		t.Fatal("no publisher with the feed on")
	}
	if len(components) != 1 || components[0].Name() != "auth-revision-feed" {
		t.Fatalf("components %v, want the auth-revision-feed subscriber", components)
	}
	if err := components[0].Shutdown(context.Background()); err != nil {
		t.Fatalf("shutdown before start: %v", err)
	}
	if strings.Contains(components[0].Name(), "SENTINEL") {
		t.Fatal("the component name carries the URI")
	}
}
