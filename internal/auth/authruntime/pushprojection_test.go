package authruntime

import (
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/auth/authconfig"
	"github.com/full-chaos/dev-health-ops/internal/auth/pushprojection"
	"github.com/full-chaos/dev-health-ops/internal/auth/restorefence"
	"github.com/full-chaos/dev-health-ops/internal/platform/health"
)

func floorInputCount(name string) int {
	n := 0
	for _, in := range restorefence.RegisteredFloorInputs() {
		if in.Name() == name {
			n++
		}
	}
	return n
}

// Dark by default: off builds no reader and registers no floor input.
func TestThePushProjectionOffBuildsNothing(t *testing.T) {
	for name, env := range map[string]map[string]string{
		"unset": {},
		"off":   {authconfig.EnvPushProjection: "off"},
	} {
		cfg, pool, schema := feedFixture(t, env)
		before := floorInputCount(pushprojection.FloorInputName)
		components, err := pushProjection(cfg, discard(), pool, schema, health.NewRegistry(), nil)
		if err != nil || len(components) != 0 {
			t.Fatalf("%s: %d components, err %v; want none", name, len(components), err)
		}
		if floorInputCount(pushprojection.FloorInputName) != before {
			t.Fatalf("%s: a floor input was registered with the projection off", name)
		}
	}
}

// On: one reader component, and its restore-fence floor input registered once, however many times it is built.
func TestThePushProjectionOnBuildsOneReaderAndRegistersItsFloorInputOnce(t *testing.T) {
	cfg, pool, schema := feedFixture(t, map[string]string{authconfig.EnvPushProjection: "on"})
	components, err := pushProjection(cfg, discard(), pool, schema, health.NewRegistry(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(components) != 1 || components[0].Name() != "auth-push-projection" {
		t.Fatalf("components %v, want the auth-push-projection reader", components)
	}
	if floorInputCount(pushprojection.FloorInputName) != 1 {
		t.Fatal("the reader is an outbox consumer and did not register its restore-fence floor input once")
	}
	if _, err := pushProjection(cfg, discard(), pool, schema, health.NewRegistry(), nil); err != nil {
		t.Fatal(err)
	}
	if floorInputCount(pushprojection.FloorInputName) != 1 {
		t.Fatal("a second build registered the floor input again")
	}
}
