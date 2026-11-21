package syncdispatchruntime

import (
	"slices"
	"testing"
	"time"

	scheduledsync "github.com/full-chaos/dev-health-ops/internal/scheduler/sync"
)

// CHAOS-14468: while a run has a non-chunked candidate unit in a bucket with cap >= 2, its new chunked units take at
// most cap-1-activeChunked slots. Each cell is one dispatch pass of one run on a fake env.

type chunkShareBucket struct {
	active, activeBackfill, activeChunked int
	older                                 []olderRun
}

func chunkShareUnit(id, status, provider, costClass, dataset string, availableAt *time.Time) dispatchGuardUnit {
	return dispatchGuardUnit{id: id, status: status, provider: provider, costClass: costClass, updatedAt: admissionNow,
		availableAt: availableAt, datasetKey: dataset, chunked: guardUnitChunked(provider, dataset)}
}

func plannedMedium(id, dataset string) dispatchGuardUnit {
	return chunkShareUnit(id, syncRunUnitStatusPlanned, "github", "medium", dataset, nil)
}

func chunkSharePass(t *testing.T, self runOrder, costClass string, units []dispatchGuardUnit, state chunkShareBucket) []string {
	t.Helper()
	env := guardEnv{
		lockBuckets: func([]dispatchBucket) error { return nil },
		totalCap:    func() int { return 0 },
		activeCount: func(dispatchBucket) (int, int, int, error) {
			return state.active, state.activeBackfill, state.activeChunked, nil
		},
		emit:      func(dispatchBucket, int, int, int, int, int, int) {},
		self:      self,
		olderRuns: func(dispatchBucket) ([]olderRun, error) { return state.older, nil },
	}
	decision, err := decideDispatchGuard(sliceSource(units), env, admissionOrg, admissionNow)
	if err != nil {
		t.Fatal(err)
	}
	var admitted []string
	for _, unit := range units {
		if decision.authorizedUnitIDs[unit.id] && !slices.Contains(decision.cappedUnitIDs, unit.id) {
			admitted = append(admitted, unit.id)
		}
	}
	return admitted
}

func assertAdmittedIDs(t *testing.T, name string, got []string, want ...string) {
	t.Helper()
	if !slices.Equal(got, want) {
		t.Fatalf("%s: admitted %v, want %v", name, got, want)
	}
}

func shareScheduledRun() runOrder { return scheduledRun(admissionRunC, admissionNow.Add(-time.Minute)) }

// M1: two long cicd units ahead of the short units by id in an empty medium bucket: one long unit and one short unit.
func TestChunkedShareKeepsASlotForTheRunsShortUnits(t *testing.T) {
	units := []dispatchGuardUnit{plannedMedium("l0", "cicd"), plannedMedium("l1", "cicd"), plannedMedium("s0", "prs"), plannedMedium("s1", "commits")}
	assertAdmittedIDs(t, "empty medium bucket", chunkSharePass(t, shareScheduledRun(), "medium", units, chunkShareBucket{}), "l0", "s0")
}

// M2: the share counts the chunked units already active in the bucket, not only this pass's.
func TestChunkedShareCountsActiveChunkedUnits(t *testing.T) {
	units := []dispatchGuardUnit{plannedMedium("l1", "cicd"), plannedMedium("s0", "prs")}
	got := chunkSharePass(t, shareScheduledRun(), "medium", units, chunkShareBucket{active: 1, activeChunked: 1})
	assertAdmittedIDs(t, "l0 active from an earlier pass, one free slot", got, "s0")
}

// M3 and card cell (ii): a run with only chunked units waiting has no limit and takes both slots.
func TestChunkedShareHasNoLimitWithoutAShortUnit(t *testing.T) {
	units := []dispatchGuardUnit{plannedMedium("l0", "cicd"), plannedMedium("l1", "cicd"), plannedMedium("l2", "cicd")}
	assertAdmittedIDs(t, "only long units", chunkSharePass(t, shareScheduledRun(), "medium", units, chunkShareBucket{}), "l0", "l1")
}

// M4: a cap 1 bucket has no reserve: the chunked tests unit keeps the one heavy slot by id, alone or beside commit-stats.
func TestChunkedShareDoesNotApplyToACapOneBucket(t *testing.T) {
	tests := chunkShareUnit("h0", syncRunUnitStatusPlanned, "github", "heavy", "tests", nil)
	stats := chunkShareUnit("h1", syncRunUnitStatusPlanned, "github", "heavy", "commit-stats", nil)
	assertAdmittedIDs(t, "tests beside commit-stats", chunkSharePass(t, shareScheduledRun(), "heavy", []dispatchGuardUnit{tests, stats}, chunkShareBucket{}), "h0")
	assertAdmittedIDs(t, "tests alone", chunkSharePass(t, shareScheduledRun(), "heavy", []dispatchGuardUnit{tests}, chunkShareBucket{}), "h0")
}

// C1 and M5: (a) a short unit this pass cannot admit (retrying with a future available_at, the state a budget or
// cooldown deferral writes) keeps no slot: the long units fill both free slots. (b) a due retrying short unit is a
// candidate of the pass and keeps the slot.
func TestChunkedShareKeepsTheSlotOnlyForACandidateShortUnit(t *testing.T) {
	future := admissionNow.Add(time.Minute)
	past := admissionNow.Add(-time.Second)
	notDue := chunkShareUnit("s0", syncRunUnitStatusRetrying, "github", "medium", "prs", &future)
	due := chunkShareUnit("s0", syncRunUnitStatusRetrying, "github", "medium", "prs", &past)
	units := []dispatchGuardUnit{plannedMedium("l0", "cicd"), plannedMedium("l1", "cicd"), notDue}
	assertAdmittedIDs(t, "(a) retrying, not due", chunkSharePass(t, shareScheduledRun(), "medium", units, chunkShareBucket{}), "l0", "l1")
	units[2] = due
	assertAdmittedIDs(t, "(b) retrying, due", chunkSharePass(t, shareScheduledRun(), "medium", units, chunkShareBucket{}), "l0", "s0")
}

// C2: the capped chunked unit takes the freed slot on the first pass after the run's last planned short unit was
// admitted and ended (the wake pass of that unit's end, CHAOS-10013).
func TestChunkedShareDrainsToTheCappedChunkedUnit(t *testing.T) {
	both := []dispatchGuardUnit{plannedMedium("l1", "cicd"), plannedMedium("s0", "prs")}
	got := chunkSharePass(t, shareScheduledRun(), "medium", both, chunkShareBucket{active: 1, activeChunked: 1})
	assertAdmittedIDs(t, "pass with s0 planned", got, "s0")
	got = chunkSharePass(t, shareScheduledRun(), "medium", both[:1], chunkShareBucket{active: 2, activeChunked: 1})
	assertAdmittedIDs(t, "pass while s0 runs", got)
	got = chunkSharePass(t, shareScheduledRun(), "medium", both[:1], chunkShareBucket{active: 1, activeChunked: 1})
	assertAdmittedIDs(t, "wake pass after s0 ended", got, "l1")
}

// C1c: the budget step denied the kept-for short unit (the deferral writer leaves it retrying with a future
// available_at). The next pass inside that deferral gives the slot to the capped chunked unit. A pass at the unit's own
// available_at (the run's self-armed re-arm lands there) finds it due again and keeps the slot for it: the named bound.
func TestChunkedShareGivesTheSlotBackAfterABudgetDeferral(t *testing.T) {
	deferredUntil := admissionNow.Add(time.Minute)
	dueAgain := admissionNow
	active := chunkShareBucket{active: 1, activeChunked: 1}
	units := []dispatchGuardUnit{plannedMedium("l1", "cicd"), plannedMedium("s0", "prs")}
	assertAdmittedIDs(t, "pass that keeps the slot for s0", chunkSharePass(t, shareScheduledRun(), "medium", units, active), "s0")
	units[1] = chunkShareUnit("s0", syncRunUnitStatusRetrying, "github", "medium", "prs", &deferredUntil)
	assertAdmittedIDs(t, "next pass, s0 deferred by the budget step", chunkSharePass(t, shareScheduledRun(), "medium", units, active), "l1")
	units[1] = chunkShareUnit("s0", syncRunUnitStatusRetrying, "github", "medium", "prs", &dueAgain)
	assertAdmittedIDs(t, "pass at s0's available_at", chunkSharePass(t, shareScheduledRun(), "medium", units, active), "s0")
}

// Card cell (i): with a live backfill unit in the bucket both shares apply and the smaller bound wins.
func TestChunkedShareWithALiveBackfillUnit(t *testing.T) {
	units := []dispatchGuardUnit{plannedMedium("l0", "cicd"), plannedMedium("s0", "prs")}
	backfillSelf := backfillRun(admissionRunB, admissionNow.Add(-time.Hour))
	got := chunkSharePass(t, backfillSelf, "medium", units, chunkShareBucket{active: 1, activeBackfill: 1})
	assertAdmittedIDs(t, "backfill run, live non-chunked backfill unit: backfill share 0 < chunked share 1", got)
	got = chunkSharePass(t, shareScheduledRun(), "medium", units, chunkShareBucket{active: 1, activeBackfill: 1, activeChunked: 1})
	assertAdmittedIDs(t, "scheduled run, live chunked backfill unit: chunked share 0", got, "s0")
	got = chunkSharePass(t, backfillSelf, "medium", units, chunkShareBucket{})
	assertAdmittedIDs(t, "backfill run, empty bucket: both shares 1", got, "l0")
}

// Card cell (iii): short units of two runs. An older live run with a short unit waiting gets its slot first (CHAOS-8880);
// the newer run's chunked unit takes the other slot, and its own short unit waits for the older run's.
func TestChunkedShareWithShortUnitsOfTwoRuns(t *testing.T) {
	older := liveOlder(scheduledRun(admissionRunA, admissionNow.Add(-time.Hour)), 1)
	units := []dispatchGuardUnit{plannedMedium("l0", "cicd"), plannedMedium("l1", "cicd"), plannedMedium("s0", "prs")}
	got := chunkSharePass(t, shareScheduledRun(), "medium", units, chunkShareBucket{older: []olderRun{older}})
	assertAdmittedIDs(t, "newer run, older run has one short unit waiting", got, "l0")
	got = chunkSharePass(t, shareScheduledRun(), "medium", units[1:], chunkShareBucket{active: 2, activeChunked: 1})
	assertAdmittedIDs(t, "newer run, older run's short unit and l0 run", got)
	got = chunkSharePass(t, shareScheduledRun(), "medium", units[1:], chunkShareBucket{active: 1, activeChunked: 1})
	assertAdmittedIDs(t, "newer run, older run's short unit ended", got, "s0")
}

// C3: the flag is the executor's Descriptor Chunked flag; an unknown key is not chunked (no limit). The active read's
// key list agrees with the flag for every stored key.
func TestGuardUnitChunkedVocabulary(t *testing.T) {
	for _, tc := range []struct {
		provider, dataset string
		want              bool
	}{
		{"github", "cicd", true}, {"github", "tests", true}, {"github", "files", true},
		{"gitlab", "cicd", true}, {"gitlab", "tests", true}, {"gitlab", "files", false},
		{"github", "prs", false}, {"github", "pr-reviews", false}, {"github", "pr-comments", false},
		{"github", "commits", false}, {"github", "commit-stats", false}, {"github", "deployments", false},
		{"github", "security", false}, {"github", "work-items", false}, {"github", "work-item-labels", false},
		{"github", "not-a-dataset", false}, {"github", "", false}, {"", "cicd", false}, {"jira", "cicd", false},
	} {
		if got := guardUnitChunked(tc.provider, tc.dataset); got != tc.want {
			t.Errorf("guardUnitChunked(%q, %q) = %v, want %v", tc.provider, tc.dataset, got, tc.want)
		}
		if got := slices.Contains(chunkedDatasetKeys(tc.provider), tc.dataset); got != tc.want {
			t.Errorf("chunkedDatasetKeys(%q) holds %q = %v, want %v", tc.provider, tc.dataset, got, tc.want)
		}
	}
}

// C3: the dataset keys the planner writes for the chunked routes resolve to chunked through the same flag.
func TestPlannedChunkedRouteKeysAreChunked(t *testing.T) {
	for _, tc := range []struct {
		provider string
		datasets []string
	}{
		{"github", []string{"cicd"}}, {"github", []string{"tests"}}, {"github", []string{"files"}},
		{"gitlab", []string{"cicd"}}, {"gitlab", []string{"tests"}},
	} {
		input := scheduledsync.PlannerInput{
			OrgID: "org-14468", IntegrationID: "integration-14468", Mode: scheduledsync.SyncModeIncremental, Now: admissionNow,
			Sources: []scheduledsync.PlanSource{{ID: "source-1", ExternalID: "one", Provider: tc.provider, FullName: "one"}},
		}
		for _, dataset := range tc.datasets {
			input.Datasets = append(input.Datasets, scheduledsync.PlanDataset{Key: dataset})
		}
		units, err := scheduledsync.BuildScheduledPlan(input)
		if err != nil {
			t.Fatalf("%s %v: %v", tc.provider, tc.datasets, err)
		}
		if len(units) == 0 {
			t.Fatalf("%s %v: no unit planned", tc.provider, tc.datasets)
		}
		for _, unit := range units {
			if !guardUnitChunked(unit.Provider, unit.Dataset) {
				t.Errorf("%s %v: planned key %q is not chunked", tc.provider, tc.datasets, unit.Dataset)
			}
		}
	}
}
