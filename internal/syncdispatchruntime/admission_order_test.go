package syncdispatchruntime

import (
	"os"
	"strings"
	"testing"
	"time"
)

// CHAOS-8880 / CHAOS-14250: free slots of a bucket go to the oldest eligible run first, through the bucket sweep's
// reservations: a pass counts another run's live reservation as a used slot and retires its own. These cells pin the
// guard decision (decideDispatchGuard on a fake env) and its wiring to the guard transaction.

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

func admissionGuardEnv(self runOrder, active map[dispatchBucket]int, reserved map[dispatchBucket]int) (guardEnv, *[]guardCall) {
	calls := &[]guardCall{}
	env := guardEnv{
		lockBuckets: func(buckets []dispatchBucket) error {
			for _, bucket := range buckets {
				*calls = append(*calls, guardCall{kind: "lock", bucket: bucket})
			}
			return nil
		},
		totalCap: func() int { return 0 },
		activeCount: func(bucket dispatchBucket) (int, int, error) {
			*calls = append(*calls, guardCall{kind: "active", bucket: bucket})
			return active[bucket], 0, nil
		},
		emit: func(dispatchBucket, int, int, int, int, int, int) {},
		self: self,
		reservations: func(bucket dispatchBucket) (int, int, error) {
			*calls = append(*calls, guardCall{kind: "reserved", bucket: bucket})
			return reserved[bucket], 0, nil
		},
		retireReservations: func() error {
			*calls = append(*calls, guardCall{kind: "retire"})
			return nil
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

func TestGuardLeavesReservedSlotsToTheReservedRun(t *testing.T) {
	bucket := dispatchBucket{orgID: admissionOrg, provider: "github", costClass: "medium"}
	env, _ := admissionGuardEnv(scheduledRun(admissionRunB, admissionNow.Add(-time.Minute)), nil, map[dispatchBucket]int{bucket: 2})
	decision, err := decideDispatchGuard(sliceSource(plannedUnits("b", 3, "github", "medium")), env, admissionOrg, admissionNow)
	if err != nil {
		t.Fatal(err)
	}
	if len(decision.cappedUnitIDs) != 3 || decision.slotHeadroom[bucket] != 0 {
		t.Fatalf("medium bucket (cap 2), both slots reserved for another run: capped=%d headroom=%d, want all 3 and 0",
			len(decision.cappedUnitIDs), decision.slotHeadroom[bucket])
	}
}

func TestGuardHoldsBackOnlyTheReservedSlots(t *testing.T) {
	bucket := dispatchBucket{orgID: admissionOrg, provider: "github", costClass: "medium"}
	env, _ := admissionGuardEnv(scheduledRun(admissionRunB, admissionNow.Add(-time.Minute)), nil, map[dispatchBucket]int{bucket: 1})
	decision, err := decideDispatchGuard(sliceSource(plannedUnits("b", 3, "github", "medium")), env, admissionOrg, admissionNow)
	if err != nil {
		t.Fatal(err)
	}
	if len(decision.cappedUnitIDs) != 2 {
		t.Fatalf("medium bucket (cap 2), one slot reserved for another run: capped=%d, want 2", len(decision.cappedUnitIDs))
	}
}

// Control: no reservation, the decision is the one main makes.
func TestGuardWithoutReservationsIsUnchanged(t *testing.T) {
	bucket := dispatchBucket{orgID: admissionOrg, provider: "github", costClass: "medium"}
	env, _ := admissionGuardEnv(scheduledRun(admissionRunB, admissionNow), nil, nil)
	decision, err := decideDispatchGuard(sliceSource(plannedUnits("b", 3, "github", "medium")), env, admissionOrg, admissionNow)
	if err != nil {
		t.Fatal(err)
	}
	if len(decision.cappedUnitIDs) != 1 || decision.slotHeadroom[bucket] != 0 {
		t.Fatalf("no reservation: capped=%d headroom=%d, want 1, 0", len(decision.cappedUnitIDs), decision.slotHeadroom[bucket])
	}
}

// Control: a reservation of another bucket holds nothing back here.
func TestGuardIgnoresReservationsOfOtherBuckets(t *testing.T) {
	other := dispatchBucket{orgID: admissionOrg, provider: "github", costClass: "light"}
	env, _ := admissionGuardEnv(scheduledRun(admissionRunB, admissionNow), nil, map[dispatchBucket]int{other: 3})
	decision, err := decideDispatchGuard(sliceSource(plannedUnits("b", 3, "github", "medium")), env, admissionOrg, admissionNow)
	if err != nil {
		t.Fatal(err)
	}
	if len(decision.cappedUnitIDs) != 1 {
		t.Fatalf("reservations only in the light bucket: capped=%d in medium, want 1", len(decision.cappedUnitIDs))
	}
}

// The reservation read is made for exactly the buckets whose active count is read, after the bucket locks, and next
// to that count; the run's own reservations are retired once, after the locks and before any count.
func TestGuardReadsReservationsUnderTheBucketLockNextToTheActiveCount(t *testing.T) {
	medium := dispatchBucket{orgID: admissionOrg, provider: "github", costClass: "medium"}
	light := dispatchBucket{orgID: admissionOrg, provider: "github", costClass: "light"}
	env, calls := admissionGuardEnv(scheduledRun(admissionRunB, admissionNow), nil, nil)
	units := append(plannedUnits("m", 2, "github", "medium"), plannedUnits("l", 2, "github", "light")...)
	if _, err := decideDispatchGuard(sliceSource(units), env, admissionOrg, admissionNow); err != nil {
		t.Fatal(err)
	}
	lastLock, retireAt, retires, firstActive := -1, -1, 0, len(*calls)
	activeAt := map[dispatchBucket]int{}
	reservedAt := map[dispatchBucket]int{}
	for index, call := range *calls {
		switch call.kind {
		case "lock":
			lastLock = index
		case "retire":
			retireAt = index
			retires++
		case "active":
			activeAt[call.bucket] = index
			firstActive = min(firstActive, index)
		case "reserved":
			reservedAt[call.bucket] = index
		}
	}
	if retires != 1 || retireAt < lastLock || retireAt > firstActive {
		t.Fatalf("own reservations retired %d times at call %d, want once after the last lock (%d) and before the first count (%d); calls=%v",
			retires, retireAt, lastLock, firstActive, *calls)
	}
	for _, bucket := range []dispatchBucket{medium, light} {
		reserved, ok := reservedAt[bucket]
		if !ok {
			t.Fatalf("bucket %v: no reservation read; calls=%v", bucket, *calls)
		}
		if reserved != activeAt[bucket]+1 {
			t.Fatalf("bucket %v: reservation read at call %d, want right after its active count at %d; calls=%v", bucket, reserved, activeAt[bucket], *calls)
		}
	}
	if len(reservedAt) != len(activeAt) {
		t.Fatalf("reservation reads for %d buckets, active counts for %d; calls=%v", len(reservedAt), len(activeAt), *calls)
	}
}

// authorizeRun wires the reservation read and the retire to the guard's own transaction, the one that holds the
// bucket locks and reads the active count.
func TestAuthorizeRunReadsReservationsInTheGuardTransaction(t *testing.T) {
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
	for _, field := range []string{"reservations:", "retireReservations:"} {
		at := strings.Index(body, field)
		if at < 0 {
			t.Fatalf("authorizeRun does not set guardEnv.%s\n%s", field, body)
		}
		closure := body[at:]
		if end := strings.Index(closure, "},"); end >= 0 {
			closure = closure[:end]
		}
		if !strings.Contains(closure, "(ctx, tx,") {
			t.Fatalf("guardEnv.%s is not run through authorizeRun's tx:\n%s", field, closure)
		}
	}
}
