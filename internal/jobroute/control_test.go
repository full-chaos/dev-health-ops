package jobroute

import (
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/jobruntime"
)

func TestAllowedRouteIsBoundedByCheckedInTargetAndRollback(t *testing.T) {
	descriptor := jobruntime.Descriptor{Route: "river_canary", RollbackRoute: "celery"}
	for _, route := range []string{"river_canary", "celery"} {
		if !allowed(descriptor, route) {
			t.Fatalf("route %q rejected", route)
		}
	}
	for _, route := range []string{"", "river", "shadow", "removed"} {
		if allowed(descriptor, route) {
			t.Fatalf("route %q accepted", route)
		}
	}
}

func TestPostgresRiverQuiescerRejectsUnsafeConfiguration(t *testing.T) {
	if _, err := NewPostgresRiverQuiescer(nil, "river"); err == nil {
		t.Fatal("nil pool accepted")
	}
}
