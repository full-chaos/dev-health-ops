package riverstore_test

import (
	"strings"
	"testing"

	riverstore "github.com/full-chaos/dev-health-ops/internal/storage/river"
)

// TestMigrateRefusesMalformedRouteKinds pins the option guard: a kind the
// registry grammar would refuse, or a repeated kind, stops the run before it
// touches the database.
func TestMigrateRefusesMalformedRouteKinds(t *testing.T) {
	base := riverstore.MigrationOptions{Schema: "river", DomainRole: "route_domain", QueueRole: "route_queue"}
	long := "a." + strings.Repeat("b", 95)
	for _, tc := range []struct {
		name  string
		kinds []string
		ok    bool
	}{
		{"none", nil, true},
		{"empty list", []string{}, true},
		{"canonical", []string{"system.heartbeat", "sync.provider_unit"}, true},
		{"at bound", []string{long[:96]}, true},
		{"past bound", []string{long}, false},
		{"empty kind", []string{""}, false},
		{"no dot", []string{"heartbeat"}, false},
		{"upper case", []string{"System.Heartbeat"}, false},
		{"injection", []string{"a.b'); DROP TABLE x; --"}, false},
		{"duplicate", []string{"system.heartbeat", "system.heartbeat"}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			options := base
			options.NativeRiverRoutes = tc.kinds
			err := riverstore.ValidateMigrationOptions(options)
			if tc.ok != (err == nil) {
				t.Fatalf("ValidateMigrationOptions(%q) err=%v", tc.kinds, err)
			}
		})
	}
}
