package authruntime

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	valkeygo "github.com/valkey-io/valkey-go"

	"github.com/full-chaos/dev-health-ops/internal/auth/authconfig"
	"github.com/full-chaos/dev-health-ops/internal/auth/restorefence"
	"github.com/full-chaos/dev-health-ops/internal/storage/postgres/authschema"
	"github.com/full-chaos/dev-health-ops/internal/storage/valkey"
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
	registered := false
	for _, in := range restorefence.RegisteredFloorInputs() {
		registered = registered || in.Name() == "valkey-revision-feed"
	}
	if !registered {
		t.Fatal("the feed is an outbox consumer and did not register its restore-fence floor input")
	}
	if err := components[0].Shutdown(context.Background()); err != nil {
		t.Fatalf("shutdown before start: %v", err)
	}
	if strings.Contains(components[0].Name(), "SENTINEL") {
		t.Fatal("the component name carries the URI")
	}
}

// ACP ruling on PR B (C2): the floor input registers only when the feed is on, and the off path dials nothing.
func TestTheFloorInputIsRegisteredOnlyWhenTheFeedIsOnAndNothingDialsAtBuild(t *testing.T) {
	var dials atomic.Int64
	dial := func(context.Context, valkey.Config) (valkeygo.Client, error) {
		dials.Add(1)
		return nil, errors.New("no valkey in this test")
	}
	count := func() int {
		n := 0
		for _, in := range restorefence.RegisteredFloorInputs() {
			if in.Name() == "valkey-revision-feed" {
				n++
			}
		}
		return n
	}
	for name, env := range map[string]map[string]string{
		"unset": {authconfig.EnvValkeyURI: testValkeyURI},
		"off":   {authconfig.EnvValkeyURI: testValkeyURI, authconfig.EnvRevisionFeed: "off"},
	} {
		cfg, pool, schema := feedFixture(t, env)
		before := count()
		publisher, components, err := revisionFeedWith(cfg, discard(), pool, schema, dial)
		if err != nil || publisher != nil || len(components) != 0 {
			t.Fatalf("%s: publisher %v, %d components, err %v", name, publisher, len(components), err)
		}
		if count() != before {
			t.Fatalf("%s: a floor input was registered with the feed off", name)
		}
		if dials.Load() != 0 {
			t.Fatalf("%s: %d dials with the feed off", name, dials.Load())
		}
	}
	cfg, pool, schema := feedFixture(t, map[string]string{authconfig.EnvValkeyURI: testValkeyURI, authconfig.EnvRevisionFeed: "on"})
	before := count()
	_, components, err := revisionFeedWith(cfg, discard(), pool, schema, dial)
	if err != nil || len(components) != 1 {
		t.Fatalf("on: %d components, err %v", len(components), err)
	}
	if count() != before+1 {
		t.Fatalf("on: the floor input was registered %d times, want once", count()-before)
	}
	if dials.Load() != 0 {
		t.Fatalf("on: %d dials while building (the client dials on first use)", dials.Load())
	}
}
