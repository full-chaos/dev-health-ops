package repouser

import (
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/jobs/metrics/changefailure"
)

func TestApplyChangeFailureSetsTheDayValueAndStoresOnlyRepositoriesWithCounts(t *testing.T) {
	measured := uuid.MustParse("11111111-1111-4111-8111-111111111111")
	unknown := uuid.MustParse("22222222-2222-4222-8222-222222222222")
	notApplicable := uuid.MustParse("33333333-3333-4333-8333-333333333333")
	deployOnly := uuid.MustParse("44444444-4444-4444-8444-444444444444")
	// retracted already has a stored row for the day and nothing to count now.
	retracted := uuid.MustParse("55555555-5555-4555-8555-555555555555")
	day := time.Date(2026, 8, 3, 15, 30, 0, 0, time.UTC)
	computedAt := time.Date(2026, 8, 4, 1, 0, 0, 0, time.UTC)

	result := Result{RepoMetrics: []RepoMetric{{RepoID: measured}, {RepoID: unknown}, {RepoID: notApplicable}}}
	ApplyChangeFailure(&result, day, map[uuid.UUID]changefailure.Counts{
		measured:   {Deployments: 4, FailedHeuristic: 1, IncidentsDirect: 1},
		unknown:    {Deployments: 4},
		deployOnly: {Deployments: 2, FailedNative: 2, IncidentsViaDeployment: 1},
		// An entry with nothing to count must not become a stored row.
		notApplicable: {},
	}, []uuid.UUID{retracted, measured}, computedAt)

	rates := map[uuid.UUID]*float64{}
	for _, row := range result.RepoMetrics {
		rates[row.RepoID] = row.ChangeFailureRateIncident
	}
	if got := rates[measured]; got == nil || *got != 0.25 {
		t.Errorf("measured repository: %v, want 0.25", got)
	}
	if rates[unknown] != nil || rates[notApplicable] != nil {
		t.Errorf("unknown %v / not applicable %v, want both nil (never 0)", rates[unknown], rates[notApplicable])
	}
	// A repository with counts and no commit or pull-request row gets a
	// change-failure row only: no repo_metrics_daily row is made up for it.
	if len(result.RepoMetrics) != 3 {
		t.Errorf("%d repo metric rows, want the 3 that Compute produced", len(result.RepoMetrics))
	}
	stored := map[uuid.UUID]ChangeFailureDaily{}
	for _, row := range result.ChangeFailure {
		stored[row.RepoID] = row
	}
	if len(stored) != 4 {
		t.Fatalf("stored change-failure rows for %d repositories, want 4 (three with counts, one retraction; the one that never had a row has none)", len(stored))
	}
	// A repository that had a row and has nothing to count gets a row of
	// zeros with the run's computed_at, so the old counts stop being the
	// newest. A repository that still has counts keeps them.
	if row, ok := stored[retracted]; !ok || row.Counts != (changefailure.Counts{}) || !row.ComputedAt.Equal(computedAt) {
		t.Errorf("retraction row = %+v (present %v), want zeros at the run's computed_at", row, ok)
	}
	if stored[measured].Counts != (changefailure.Counts{Deployments: 4, FailedHeuristic: 1, IncidentsDirect: 1}) {
		t.Errorf("a stored repository that still has counts = %+v, want its counts, not zeros", stored[measured].Counts)
	}
	if _, ok := stored[notApplicable]; ok {
		t.Error("a repository with no deployment and no incident got a zero-filled row")
	}
	row := stored[deployOnly]
	if row.Counts != (changefailure.Counts{Deployments: 2, FailedNative: 2, IncidentsViaDeployment: 1}) ||
		!row.Day.Equal(time.Date(2026, 8, 3, 0, 0, 0, 0, time.UTC)) || !row.ComputedAt.Equal(computedAt) {
		t.Errorf("deploy-only row = %+v", row)
	}
}
