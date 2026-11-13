package syncdispatchruntime

import (
	"context"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/syncrunrollup"
	"github.com/jackc/pgx/v5/pgconn"
)

// CHAOS-8880: free slots of a bucket go to the oldest live run first. These cells pin the policy (holdBackFor), its
// use in the guard decision (decideDispatchGuard on a fake env) and the wake of the run a pass held slots for.

var admissionNow = time.Date(2026, 11, 13, 12, 0, 0, 0, time.UTC)

const (
	admissionRunA = "00000000-0000-4000-8000-0000000088a1"
	admissionRunB = "00000000-0000-4000-8000-0000000088b2"
	admissionRunC = "00000000-0000-4000-8000-0000000088c3"
)

func scheduledRun(id string, createdAt time.Time) runOrder {
	return runOrder{id: id, createdAt: createdAt}
}

func backfillRun(id string, createdAt time.Time) runOrder {
	return runOrder{id: id, createdAt: createdAt, backfill: true}
}

func liveOlder(order runOrder, waiting int) olderRun {
	return olderRun{runOrder: order, progressAt: admissionNow.Add(-time.Second), waiting: waiting}
}

type holdBackCase struct {
	self      runOrder
	older     []olderRun
	freeSlots int
	wantHeld  int
	wantWake  string
}

func assertHoldBack(t *testing.T, name string, tc holdBackCase) {
	t.Helper()
	held, wake := holdBackFor(tc.self, tc.older, tc.freeSlots, admissionNow, time.Duration(600)*time.Second)
	if held != tc.wantHeld || wake != tc.wantWake {
		t.Fatalf("%s: holdBackFor = (%d, %q), want (%d, %q)", name, held, wake, tc.wantHeld, tc.wantWake)
	}
}

func TestHoldBackLeavesFreeSlotsToAnOlderLiveRun(t *testing.T) {
	assertHoldBack(t, "older scheduled run waits with 3 units, 2 free slots", holdBackCase{
		self:      scheduledRun(admissionRunB, admissionNow.Add(-time.Minute)),
		older:     []olderRun{liveOlder(scheduledRun(admissionRunA, admissionNow.Add(-time.Hour)), 3)},
		freeSlots: 2, wantHeld: 2, wantWake: admissionRunA,
	})
}

func TestHoldBackKeepsOnlyWhatOlderRunsCanUse(t *testing.T) {
	assertHoldBack(t, "older run waits with 1 unit, 2 free slots", holdBackCase{
		self:      scheduledRun(admissionRunB, admissionNow.Add(-time.Minute)),
		older:     []olderRun{liveOlder(scheduledRun(admissionRunA, admissionNow.Add(-time.Hour)), 1)},
		freeSlots: 2, wantHeld: 1, wantWake: admissionRunA,
	})
}

func TestHoldBackSumsOlderRunsAndWakesTheOldest(t *testing.T) {
	assertHoldBack(t, "two older runs wait with 1 unit each, 8 free slots", holdBackCase{
		self: scheduledRun(admissionRunC, admissionNow.Add(-time.Minute)),
		older: []olderRun{
			liveOlder(scheduledRun(admissionRunB, admissionNow.Add(-time.Hour)), 1),
			liveOlder(scheduledRun(admissionRunA, admissionNow.Add(-2*time.Hour)), 1),
		},
		freeSlots: 8, wantHeld: 2, wantWake: admissionRunA,
	})
}

// Policy B': an older scheduled (or manual/webhook) run holds back a newer backfill.
func TestOlderScheduledRunHoldsBackANewerBackfill(t *testing.T) {
	assertHoldBack(t, "older scheduled run, newer backfill decides", holdBackCase{
		self:      backfillRun(admissionRunB, admissionNow.Add(-time.Minute)),
		older:     []olderRun{liveOlder(scheduledRun(admissionRunA, admissionNow.Add(-time.Hour)), 3)},
		freeSlots: 2, wantHeld: 2, wantWake: admissionRunA,
	})
}

// Policy B': an older backfill holds back a newer backfill.
func TestOlderBackfillHoldsBackANewerBackfill(t *testing.T) {
	assertHoldBack(t, "older backfill, newer backfill decides", holdBackCase{
		self:      backfillRun(admissionRunB, admissionNow.Add(-time.Minute)),
		older:     []olderRun{liveOlder(backfillRun(admissionRunA, admissionNow.Add(-time.Hour)), 3)},
		freeSlots: 2, wantHeld: 2, wantWake: admissionRunA,
	})
}

// Policy B': a backfill never holds back a non-backfill run, however old it is.
func TestOlderBackfillGivesNoHoldBackToANewerScheduledRun(t *testing.T) {
	assertHoldBack(t, "older backfill, newer scheduled run decides", holdBackCase{
		self:      scheduledRun(admissionRunB, admissionNow.Add(-time.Minute)),
		older:     []olderRun{liveOlder(backfillRun(admissionRunA, admissionNow.Add(-time.Hour)), 3)},
		freeSlots: 2, wantHeld: 0, wantWake: "",
	})
}

// A backfill that cannot hold the deciding run back does not hide an older run behind it that can.
func TestBackfillSkippedOlderScheduledRunStillHolds(t *testing.T) {
	assertHoldBack(t, "oldest scheduled run behind a skipped backfill", holdBackCase{
		self: scheduledRun(admissionRunC, admissionNow.Add(-time.Minute)),
		older: []olderRun{
			liveOlder(backfillRun(admissionRunB, admissionNow.Add(-2*time.Hour)), 5),
			liveOlder(scheduledRun(admissionRunA, admissionNow.Add(-time.Hour)), 1),
		},
		freeSlots: 2, wantHeld: 1, wantWake: admissionRunA,
	})
}

// C1: the order is total on (createdAt, id). Two runs with the same createdAt never hold each other back: exactly the
// one with the greater id holds back.
func TestEqualCreatedAtIsBrokenByID(t *testing.T) {
	same := admissionNow.Add(-time.Hour)
	low, high := scheduledRun(admissionRunA, same), scheduledRun(admissionRunB, same)
	assertHoldBack(t, "higher id decides, lower id waits", holdBackCase{
		self: high, older: []olderRun{liveOlder(low, 3)}, freeSlots: 2, wantHeld: 2, wantWake: admissionRunA,
	})
	assertHoldBack(t, "lower id decides, higher id waits", holdBackCase{
		self: low, older: []olderRun{liveOlder(high, 3)}, freeSlots: 2, wantHeld: 0, wantWake: "",
	})
}

func TestNewerRunsNeverHoldBack(t *testing.T) {
	assertHoldBack(t, "a newer run waits", holdBackCase{
		self:      scheduledRun(admissionRunA, admissionNow.Add(-time.Hour)),
		older:     []olderRun{liveOlder(scheduledRun(admissionRunB, admissionNow.Add(-time.Minute)), 3)},
		freeSlots: 2, wantHeld: 0, wantWake: "",
	})
}

func TestNoFreeSlotsHoldsNothing(t *testing.T) {
	assertHoldBack(t, "bucket full", holdBackCase{
		self:      scheduledRun(admissionRunB, admissionNow.Add(-time.Minute)),
		older:     []olderRun{liveOlder(scheduledRun(admissionRunA, admissionNow.Add(-time.Hour)), 3)},
		freeSlots: 0, wantHeld: 0, wantWake: "",
	})
}

// C3: the liveness bound. Progress just inside the bound still holds slots back.
func TestLivenessBoundHoldsJustInsideTheBound(t *testing.T) {
	bound := 600 * time.Second
	older := olderRun{runOrder: scheduledRun(admissionRunA, admissionNow.Add(-time.Hour)), progressAt: admissionNow.Add(-bound + time.Microsecond), waiting: 3}
	held, wake := holdBackFor(scheduledRun(admissionRunB, admissionNow.Add(-time.Minute)), []olderRun{older}, 2, admissionNow, bound)
	if held != 2 || wake != admissionRunA {
		t.Fatalf("progress 1us inside the bound: holdBackFor = (%d, %q), want (2, %q)", held, wake, admissionRunA)
	}
}

// C3: progress exactly at the bound no longer holds slots back.
func TestLivenessBoundReleasesExactlyAtTheBound(t *testing.T) {
	bound := 600 * time.Second
	older := olderRun{runOrder: scheduledRun(admissionRunA, admissionNow.Add(-time.Hour)), progressAt: admissionNow.Add(-bound), waiting: 3}
	held, wake := holdBackFor(scheduledRun(admissionRunB, admissionNow.Add(-time.Minute)), []olderRun{older}, 2, admissionNow, bound)
	if held != 0 || wake != "" {
		t.Fatalf("progress exactly at the bound: holdBackFor = (%d, %q), want (0, \"\")", held, wake)
	}
}

func TestAdmissionLivenessBoundIsTwiceTheCountdownMax(t *testing.T) {
	t.Setenv("SYNC_DISPATCH_REDISPATCH_COUNTDOWN", "10")
	t.Setenv("SYNC_DISPATCH_REDISPATCH_COUNTDOWN_MAX", "300")
	if got := admissionLivenessBound(); got != 600*time.Second {
		t.Fatalf("admissionLivenessBound = %v, want 600s (2 x SYNC_DISPATCH_REDISPATCH_COUNTDOWN_MAX)", got)
	}
}

// guardCall is one environment call of a decision, in order.
type guardCall struct {
	kind   string
	bucket dispatchBucket
}

func admissionGuardEnv(self runOrder, active map[dispatchBucket]int, older map[dispatchBucket][]olderRun) (guardEnv, *[]guardCall) {
	calls := &[]guardCall{}
	env := guardEnv{
		lockBuckets: func(buckets []dispatchBucket) error {
			for _, bucket := range buckets {
				*calls = append(*calls, guardCall{kind: "lock", bucket: bucket})
			}
			return nil
		},
		totalCap: func() int { return 0 },
		activeCount: func(bucket dispatchBucket) (int, error) {
			*calls = append(*calls, guardCall{kind: "active", bucket: bucket})
			return active[bucket], nil
		},
		emit: func(dispatchBucket, int, int, int, int, int) {},
		self: self,
		olderRuns: func(bucket dispatchBucket) ([]olderRun, error) {
			*calls = append(*calls, guardCall{kind: "older", bucket: bucket})
			return older[bucket], nil
		},
	}
	return env, calls
}

func plannedUnits(prefix string, n int, provider, costClass string) []dispatchGuardUnit {
	units := make([]dispatchGuardUnit, 0, n)
	for i := 0; i < n; i++ {
		units = append(units, dispatchGuardUnit{
			id: prefix + string(rune('a'+i)), status: syncRunUnitStatusPlanned, provider: provider, costClass: costClass, updatedAt: admissionNow,
		})
	}
	return units
}

func sliceSource(units []dispatchGuardUnit) guardUnitSource {
	return func(fold func(dispatchGuardUnit) error) error {
		for _, unit := range units {
			if err := fold(unit); err != nil {
				return err
			}
		}
		return nil
	}
}

const admissionOrg = "org-8880"

func TestGuardLeavesFreeSlotsToAnOlderLiveRun(t *testing.T) {
	bucket := dispatchBucket{orgID: admissionOrg, provider: "github", costClass: "medium"}
	self := scheduledRun(admissionRunB, admissionNow.Add(-time.Minute))
	older := map[dispatchBucket][]olderRun{bucket: {liveOlder(scheduledRun(admissionRunA, admissionNow.Add(-time.Hour)), 3)}}
	env, _ := admissionGuardEnv(self, nil, older)
	decision, err := decideDispatchGuard(sliceSource(plannedUnits("b", 3, "github", "medium")), env, admissionOrg, admissionNow)
	if err != nil {
		t.Fatal(err)
	}
	if len(decision.cappedUnitIDs) != 3 || decision.slotHeadroom[bucket] != 0 {
		t.Fatalf("medium bucket (cap 2), older live run waits with 3: capped=%d headroom=%d, want all 3 held back and headroom 0",
			len(decision.cappedUnitIDs), decision.slotHeadroom[bucket])
	}
	if !reflect.DeepEqual(decision.heldBackRunIDs, []string{admissionRunA}) {
		t.Fatalf("heldBackRunIDs = %v, want [%s]", decision.heldBackRunIDs, admissionRunA)
	}
}

func TestGuardHoldsBackOnlyWhatTheOlderRunCanUse(t *testing.T) {
	bucket := dispatchBucket{orgID: admissionOrg, provider: "github", costClass: "medium"}
	self := scheduledRun(admissionRunB, admissionNow.Add(-time.Minute))
	older := map[dispatchBucket][]olderRun{bucket: {liveOlder(scheduledRun(admissionRunA, admissionNow.Add(-time.Hour)), 1)}}
	env, _ := admissionGuardEnv(self, nil, older)
	decision, err := decideDispatchGuard(sliceSource(plannedUnits("b", 3, "github", "medium")), env, admissionOrg, admissionNow)
	if err != nil {
		t.Fatal(err)
	}
	if len(decision.cappedUnitIDs) != 2 {
		t.Fatalf("medium bucket (cap 2), older run waits with 1: capped=%d, want 2 (one slot left to this run)", len(decision.cappedUnitIDs))
	}
}

// Control: no older run, the decision is the one main makes.
func TestGuardWithoutOlderRunsIsUnchanged(t *testing.T) {
	bucket := dispatchBucket{orgID: admissionOrg, provider: "github", costClass: "medium"}
	env, _ := admissionGuardEnv(scheduledRun(admissionRunB, admissionNow), nil, nil)
	decision, err := decideDispatchGuard(sliceSource(plannedUnits("b", 3, "github", "medium")), env, admissionOrg, admissionNow)
	if err != nil {
		t.Fatal(err)
	}
	if len(decision.cappedUnitIDs) != 1 || decision.slotHeadroom[bucket] != 0 || len(decision.heldBackRunIDs) != 0 {
		t.Fatalf("no older run: capped=%d headroom=%d heldBack=%v, want 1, 0, none", len(decision.cappedUnitIDs), decision.slotHeadroom[bucket], decision.heldBackRunIDs)
	}
}

// Control: an older run of another bucket holds nothing back here.
func TestGuardIgnoresOlderRunsOfOtherBuckets(t *testing.T) {
	other := dispatchBucket{orgID: admissionOrg, provider: "github", costClass: "light"}
	older := map[dispatchBucket][]olderRun{other: {liveOlder(scheduledRun(admissionRunA, admissionNow.Add(-time.Hour)), 3)}}
	env, _ := admissionGuardEnv(scheduledRun(admissionRunB, admissionNow), nil, older)
	decision, err := decideDispatchGuard(sliceSource(plannedUnits("b", 3, "github", "medium")), env, admissionOrg, admissionNow)
	if err != nil {
		t.Fatal(err)
	}
	if len(decision.cappedUnitIDs) != 1 {
		t.Fatalf("older run waits only in the light bucket: capped=%d in medium, want 1", len(decision.cappedUnitIDs))
	}
}

// C2: the older-run read is made for exactly the buckets whose active count is read, after the bucket locks, and
// next to that count.
func TestGuardReadsOlderRunsUnderTheBucketLockNextToTheActiveCount(t *testing.T) {
	medium := dispatchBucket{orgID: admissionOrg, provider: "github", costClass: "medium"}
	light := dispatchBucket{orgID: admissionOrg, provider: "github", costClass: "light"}
	env, calls := admissionGuardEnv(scheduledRun(admissionRunB, admissionNow), nil, nil)
	units := append(plannedUnits("m", 2, "github", "medium"), plannedUnits("l", 2, "github", "light")...)
	if _, err := decideDispatchGuard(sliceSource(units), env, admissionOrg, admissionNow); err != nil {
		t.Fatal(err)
	}
	lastLock := -1
	activeAt := map[dispatchBucket]int{}
	olderAt := map[dispatchBucket]int{}
	for index, call := range *calls {
		switch call.kind {
		case "lock":
			lastLock = index
		case "active":
			activeAt[call.bucket] = index
		case "older":
			olderAt[call.bucket] = index
		}
	}
	for _, bucket := range []dispatchBucket{medium, light} {
		older, ok := olderAt[bucket]
		if !ok {
			t.Fatalf("bucket %v: no older-run read; calls=%v", bucket, *calls)
		}
		if older < lastLock {
			t.Fatalf("bucket %v: older-run read at call %d, before the last bucket lock at %d; calls=%v", bucket, older, lastLock, *calls)
		}
		if older != activeAt[bucket]+1 {
			t.Fatalf("bucket %v: older-run read at call %d, want right after its active count at %d; calls=%v", bucket, older, activeAt[bucket], *calls)
		}
	}
	if len(olderAt) != len(activeAt) {
		t.Fatalf("older-run reads for %d buckets, active counts for %d; calls=%v", len(olderAt), len(activeAt), *calls)
	}
}

// C2: authorizeRun wires the older-run read to the guard's own transaction, the one that holds the bucket locks and
// reads the active count.
func TestAuthorizeRunReadsOlderRunsInTheGuardTransaction(t *testing.T) {
	raw, err := os.ReadFile("dispatch_guard.go")
	if err != nil {
		t.Fatal(err)
	}
	source := string(raw)
	start := strings.Index(source, "func authorizeRun(")
	if start < 0 {
		t.Fatal("dispatch_guard.go: authorizeRun not found; update this test's marker")
	}
	end := strings.Index(source[start:], "\n}\n")
	if end < 0 {
		t.Fatal("dispatch_guard.go: end of authorizeRun not found")
	}
	body := source[start : start+end]
	field := strings.Index(body, "olderRuns:")
	if field < 0 {
		t.Fatalf("authorizeRun does not set guardEnv.olderRuns:\n%s", body)
	}
	closure := body[field:]
	if end := strings.Index(closure, "},"); end >= 0 {
		closure = closure[:end]
	}
	if !strings.Contains(closure, "(ctx, tx,") {
		t.Fatalf("guardEnv.olderRuns is not read through authorizeRun's tx:\n%s", closure)
	}
	if !strings.Contains(body, "self:") {
		t.Fatalf("authorizeRun does not set guardEnv.self:\n%s", body)
	}
}

type countingExecer struct {
	runIDs []string
}

func (execer *countingExecer) Exec(_ context.Context, sql string, arguments ...any) (pgconn.CommandTag, error) {
	if sql == syncrunrollup.WakeDispatchSQL && len(arguments) >= 2 {
		if runID, ok := arguments[1].(string); ok {
			execer.runIDs = append(execer.runIDs, runID)
		}
	}
	return pgconn.NewCommandTag("INSERT 0 1"), nil
}

// C3: one wake per distinct held-back run, through the deduped wakeup statement.
func TestWakeHeldBackRunsArmsOneWakePerDistinctRun(t *testing.T) {
	execer := &countingExecer{}
	woken := wakeHeldBackRuns(context.Background(), execer, []string{admissionRunA, admissionRunA, admissionRunB}, admissionNow)
	if woken != 2 || !reflect.DeepEqual(execer.runIDs, []string{admissionRunA, admissionRunB}) {
		t.Fatalf("wakeHeldBackRuns: woken=%d statements for %v, want 2 for [%s %s]", woken, execer.runIDs, admissionRunA, admissionRunB)
	}
}

func TestWakeHeldBackRunsWithNoneIssuesNothing(t *testing.T) {
	execer := &countingExecer{}
	if woken := wakeHeldBackRuns(context.Background(), execer, nil, admissionNow); woken != 0 || len(execer.runIDs) != 0 {
		t.Fatalf("no held-back run: woken=%d statements=%v, want none", woken, execer.runIDs)
	}
}
