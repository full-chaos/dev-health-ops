package rivermigrate

import (
	"sort"
	"testing"

	jobsv1 "github.com/full-chaos/dev-health-ops/contracts/jobs/v1"
	"github.com/full-chaos/dev-health-ops/internal/jobcontract"
)

const checkedInJobContractRoot = "../../contracts/jobs/v1"

// TestEveryGoOnlyKindHasASeededRoute guards the roll: a go_only kind has no
// Python route, so its worker_job_routes row exists only if the migrate leg
// seeds it. The seeder list is exactly what execute() hands to the River
// migration (NativeRiverRouteKinds over the embedded migration policy). A
// go_only kind missing from it is unknown to the relay and crash-loops the Go
// workers ("job route is unknown") at the next roll.
func TestEveryGoOnlyKindHasASeededRoute(t *testing.T) {
	registry, err := jobcontract.LoadRegistry(checkedInJobContractRoot)
	if err != nil {
		t.Fatal(err)
	}
	seeded, err := jobcontract.NativeRiverRouteKinds(jobsv1.MigrationState)
	if err != nil {
		t.Fatal(err)
	}
	seededSet := make(map[string]struct{}, len(seeded))
	for _, kind := range seeded {
		seededSet[kind] = struct{}{}
	}

	var goOnly, missing []string
	for _, job := range registry.Jobs {
		if !bool(job.GoOnly) {
			continue
		}
		goOnly = append(goOnly, job.Kind)
		if _, ok := seededSet[job.Kind]; !ok {
			missing = append(missing, job.Kind)
		}
	}
	if len(goOnly) == 0 {
		t.Fatal("registry holds no go_only kind: the guard would pass on nothing")
	}
	sort.Strings(missing)
	for _, kind := range missing {
		t.Errorf("go_only kind %q has no seeder entry: dev-health-worker-migrate would not seed its route and the Go workers would crash-loop with \"job route is unknown\"; add it to contracts/jobs/v1/migration-state.json with route river and rollback_route none", kind)
	}
	t.Logf("go_only kinds checked: %v; seeded river kinds: %d", goOnly, len(seeded))
}
