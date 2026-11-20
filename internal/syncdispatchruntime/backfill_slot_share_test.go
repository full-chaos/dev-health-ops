package syncdispatchruntime

import (
	"bytes"
	"context"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/syncdispatchruntime/synclog"
)

// CHAOS-14211: backfill units hold at most cap-1 slots of a bucket, so one slot always stays for non-backfill work.
// Each cell is one dispatch pass of one run on a fake env: the active counts are what the bucket holds when the pass
// starts.

type slotShareBucket struct {
	active, activeBackfill int
	older                  []olderRun
}

func slotSharePass(t *testing.T, self runOrder, costClass string, waiting int, state slotShareBucket) (admitted, headroom int) {
	t.Helper()
	bucket := dispatchBucket{orgID: admissionOrg, provider: "github", costClass: costClass}
	env := guardEnv{
		lockBuckets: func([]dispatchBucket) error { return nil },
		totalCap:    func() int { return 0 },
		activeCount: func(dispatchBucket) (int, int, error) { return state.active, state.activeBackfill, nil },
		emit:        func(dispatchBucket, int, int, int, int, int, int) {},
		self:        self,
		olderRuns: func(dispatchBucket) ([]olderRun, error) {
			// olderRunsInBucketSQL lists older backfill runs only to a backfill run.
			var older []olderRun
			for _, run := range state.older {
				if self.backfill || !run.backfill {
					older = append(older, run)
				}
			}
			return older, nil
		},
	}
	decision, err := decideDispatchGuard(sliceSource(plannedUnits("u", waiting, "github", costClass)), env, admissionOrg, admissionNow)
	if err != nil {
		t.Fatal(err)
	}
	return waiting - len(decision.cappedUnitIDs), decision.slotHeadroom[bucket]
}

var (
	shareBackfill    = backfillRun(admissionRunB, admissionNow.Add(-time.Hour))
	shareIncremental = scheduledRun(admissionRunC, admissionNow.Add(-time.Minute))
)

func assertAdmitted(t *testing.T, name string, got, want int) {
	t.Helper()
	if got != want {
		t.Fatalf("%s: admitted %d, want %d", name, got, want)
	}
}

// (a) Two backfill units and one incremental unit wait in an empty medium bucket (cap 2): in either pass order, one
// backfill unit and the incremental unit run.
func TestBackfillShareLeavesASlotForIncrementalInEitherOrder(t *testing.T) {
	got, _ := slotSharePass(t, shareBackfill, "medium", 2, slotShareBucket{})
	assertAdmitted(t, "backfill pass first, empty bucket", got, 1)
	got, _ = slotSharePass(t, shareIncremental, "medium", 1, slotShareBucket{active: 1, activeBackfill: 1})
	assertAdmitted(t, "incremental pass after it", got, 1)

	got, _ = slotSharePass(t, shareIncremental, "medium", 1, slotShareBucket{})
	assertAdmitted(t, "incremental pass first, empty bucket", got, 1)
	got, _ = slotSharePass(t, shareBackfill, "medium", 2, slotShareBucket{active: 1})
	assertAdmitted(t, "backfill pass after it", got, 1)
}

// (b) Only backfill waits: one slot stays free, and the budget surplus headroom of the pass is 0, not 1.
func TestBackfillShareHoldsWhenOnlyBackfillWaits(t *testing.T) {
	got, headroom := slotSharePass(t, shareBackfill, "medium", 1, slotShareBucket{})
	assertAdmitted(t, "one backfill unit, empty bucket", got, 1)
	if headroom != 0 {
		t.Fatalf("slot headroom = %d, want 0: the surplus path must not admit backfill into the kept slot", headroom)
	}
	got, _ = slotSharePass(t, shareBackfill, "medium", 2, slotShareBucket{active: 1, activeBackfill: 1})
	assertAdmitted(t, "second backfill pass", got, 0)
}

// (c) No backfill: incremental work still uses every slot.
func TestBackfillShareLeavesIncrementalAdmissionUnchanged(t *testing.T) {
	got, _ := slotSharePass(t, shareIncremental, "medium", 3, slotShareBucket{})
	assertAdmitted(t, "three incremental units, empty bucket", got, 2)
	got, _ = slotSharePass(t, shareIncremental, "medium", 1, slotShareBucket{active: 1, activeBackfill: 1})
	assertAdmitted(t, "incremental unit beside one backfill unit", got, 1)
}

// (d) The prod shape: two backfill units already hold both medium slots when the new code starts. Nothing is admitted
// until one of them ends (a running unit keeps its slot); then the incremental unit gets the free slot and a waiting
// backfill never takes it.
func TestBackfillShareProdShapeTwoBackfillUnitsActive(t *testing.T) {
	full := slotShareBucket{active: 2, activeBackfill: 2}
	got, _ := slotSharePass(t, shareBackfill, "medium", 2, full)
	assertAdmitted(t, "backfill pass, both slots held by backfill", got, 0)
	got, _ = slotSharePass(t, shareIncremental, "medium", 1, full)
	assertAdmitted(t, "incremental pass, both slots held by backfill", got, 0)

	oneEnded := slotShareBucket{active: 1, activeBackfill: 1}
	got, _ = slotSharePass(t, shareBackfill, "medium", 2, oneEnded)
	assertAdmitted(t, "backfill pass after one backfill unit ended", got, 0)
	got, _ = slotSharePass(t, shareIncremental, "medium", 1, oneEnded)
	assertAdmitted(t, "incremental pass after one backfill unit ended", got, 1)
}

// (e) A cap 1 bucket has no reserve: backfill keeps the one slot, as before.
func TestBackfillShareDoesNotApplyToACapOneBucket(t *testing.T) {
	got, _ := slotSharePass(t, shareBackfill, "heavy", 2, slotShareBucket{})
	assertAdmitted(t, "backfill pass, heavy bucket (cap 1)", got, 1)
	got, _ = slotSharePass(t, shareIncremental, "heavy", 2, slotShareBucket{})
	assertAdmitted(t, "incremental pass, heavy bucket (cap 1)", got, 1)

	t.Setenv("SYNC_UNIT_CONCURRENCY_PER_BUCKET", "1")
	got, _ = slotSharePass(t, shareBackfill, "medium", 2, slotShareBucket{})
	assertAdmitted(t, "backfill pass, medium bucket clamped to cap 1", got, 1)
}

// A light bucket (cap 4) gives backfill at most 3 slots.
func TestBackfillShareIsCapMinusOneForALightBucket(t *testing.T) {
	got, _ := slotSharePass(t, shareBackfill, "light", 4, slotShareBucket{active: 2, activeBackfill: 2})
	assertAdmitted(t, "backfill pass, 2 of 4 light slots held by backfill", got, 1)
	got, _ = slotSharePass(t, shareIncremental, "light", 4, slotShareBucket{active: 2, activeBackfill: 2})
	assertAdmitted(t, "incremental pass, 2 of 4 light slots held by backfill", got, 2)
	got, _ = slotSharePass(t, shareBackfill, "light", 4, slotShareBucket{})
	assertAdmitted(t, "backfill pass, empty light bucket", got, 3)
}

// The backfill share goes to an older live backfill run first; an older incremental run does not take it.
func TestBackfillShareGoesToTheOlderBackfillRunFirst(t *testing.T) {
	newer := backfillRun(admissionRunB, admissionNow.Add(-time.Minute))
	olderBackfill := liveOlder(backfillRun(admissionRunA, admissionNow.Add(-time.Hour)), 1)
	got, _ := slotSharePass(t, newer, "medium", 1, slotShareBucket{older: []olderRun{olderBackfill}})
	assertAdmitted(t, "newer backfill, older backfill run waits", got, 0)

	olderIncremental := liveOlder(scheduledRun(admissionRunA, admissionNow.Add(-time.Hour)), 1)
	got, _ = slotSharePass(t, newer, "medium", 1, slotShareBucket{older: []olderRun{olderIncremental}})
	assertAdmitted(t, "newer backfill, older incremental run waits", got, 1)
}

// The capped line names the run's mode and trigger.
func TestConcurrencyCappedLineNamesTheRunMode(t *testing.T) {
	var buf bytes.Buffer
	logger := synclog.New(slog.New(slog.NewTextHandler(&buf, nil)))
	logConcurrencyCapped(context.Background(), logger, admissionRunB, 2, guardDecision{
		reasonCode: guardReasonCodeUnitConcurrencyCap, runMode: "backfill", runTriggeredBy: "backfill",
	})
	line := buf.String()
	for _, want := range []string{"reason=unit_concurrency_cap", "mode=backfill", "triggered_by=backfill", "capped_count=2"} {
		if !strings.Contains(line, want) {
			t.Fatalf("capped line %q lacks %q", line, want)
		}
	}
}
