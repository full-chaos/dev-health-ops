package authruntime

import (
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/auth/authconfig"
	"github.com/full-chaos/dev-health-ops/internal/auth/repoidentityprojection"
	"github.com/full-chaos/dev-health-ops/internal/platform/health"
)

// Dark by default: off builds no reader and registers no floor input, whatever the push switch says.
func TestTheRepoIdentityProjectionOffBuildsNothing(t *testing.T) {
	for name, env := range map[string]map[string]string{
		"unset":   {},
		"off":     {authconfig.EnvRepoIdentityProjection: "off"},
		"push on": {authconfig.EnvPushProjection: "on"},
	} {
		cfg, pool, schema := feedFixture(t, env)
		before := floorInputCount(repoidentityprojection.FloorInputName)
		components, err := repoIdentityProjection(cfg, discard(), pool, schema, health.NewRegistry(time.Second), nil)
		if err != nil || len(components) != 0 {
			t.Fatalf("%s: %d components, err %v; want none", name, len(components), err)
		}
		if floorInputCount(repoidentityprojection.FloorInputName) != before {
			t.Fatalf("%s: a floor input was registered with the projection off", name)
		}
	}
}

// On: one reader component, and its restore-fence floor input registered once, however many times it is built.
func TestTheRepoIdentityProjectionOnBuildsOneReaderAndRegistersItsFloorInputOnce(t *testing.T) {
	cfg, pool, schema := feedFixture(t, map[string]string{authconfig.EnvRepoIdentityProjection: "on"})
	components, err := repoIdentityProjection(cfg, discard(), pool, schema, health.NewRegistry(time.Second), nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(components) != 1 || components[0].Name() != "auth-repository-identity-projection" {
		t.Fatalf("components %v, want the auth-repository-identity-projection reader", components)
	}
	if floorInputCount(repoidentityprojection.FloorInputName) != 1 {
		t.Fatal("the reader is an outbox consumer and did not register its restore-fence floor input once")
	}
	if _, err := repoIdentityProjection(cfg, discard(), pool, schema, health.NewRegistry(time.Second), nil); err != nil {
		t.Fatal(err)
	}
	if floorInputCount(repoidentityprojection.FloorInputName) != 1 {
		t.Fatal("a second build registered the floor input again")
	}
}
