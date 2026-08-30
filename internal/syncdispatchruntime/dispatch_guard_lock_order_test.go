package syncdispatchruntime

import (
	"os"
	"strings"
	"testing"
)

// TestAuthorizeRunAcquiresBucketLocksBeforeTheHardCapCheck is a structural
// regression guard for codex round 6's finding (CHAOS-4586).
//
// authorizeRun's total-unit-cap check used to return a "denied"
// guardDecision BEFORE the function ever reached acquireBucketAdvisoryLocks
// (which sat further down, only on the way to computing per-bucket
// concurrency decisions). denyRun's hasActive branch then bulk-updates the
// run's sync_run_units (failPlannedUnits/failStaleDispatchingUnits, both
// plain WHERE sync_run_id=$1 predicates matching potentially many rows,
// with no explicit row-lock order of their own) with NO bucket lock held
// at all -- UnreclaimableSweep.Step's own bucket lock (round 5) only
// protects against a dispatch pass that reaches acquireBucketAdvisoryLocks;
// it protects nothing against a hard-cap denial that returns before that.
//
// Fixed by moving the candidatesByBucket/deferredBuckets/allBuckets
// computation and the acquireBucketAdvisoryLocks call to BEFORE the
// total-cap check, making it unconditional for every authorizeRun exit
// path (that computation depends only on units/orgID/now, nothing computed
// after it, so the reorder changes no computed value, only when the
// already-idempotent lock acquisition happens). This scans the real source
// text of authorizeRun so a future edit cannot silently move the lock back
// after the cap check without this test noticing -- even before a slow
// integration/concurrency test would ever catch it. Verified to fail (via
// a temporary local revert-and-rerun, restored and confirmed clean via git
// diff --stat) if the reorder is undone.
func TestAuthorizeRunAcquiresBucketLocksBeforeTheHardCapCheck(t *testing.T) {
	const file = "dispatch_guard.go"
	raw, err := os.ReadFile(file)
	if err != nil {
		t.Fatal(err)
	}
	source := string(raw)

	funcMarker := "func authorizeRun("
	funcStart := strings.Index(source, funcMarker)
	if funcStart < 0 {
		t.Fatalf("%s: could not find %q -- has authorizeRun's signature changed? update this test's marker", file, funcMarker)
	}
	body := source[funcStart:]

	lockIndex := strings.Index(body, "acquireBucketAdvisoryLocks(ctx, tx,")
	if lockIndex < 0 {
		t.Fatalf("%s: authorizeRun no longer calls acquireBucketAdvisoryLocks directly -- "+
			"has the bucket-locking mechanism changed shape? update this test's marker", file)
	}

	capCheckMarker := "if len(units) > totalCap {"
	capCheckIndex := strings.Index(body, capCheckMarker)
	if capCheckIndex < 0 {
		t.Fatalf("%s: could not find %q -- has the total-unit-cap check changed shape? update this test's marker", file, capCheckMarker)
	}

	if lockIndex >= capCheckIndex {
		t.Fatalf("%s: acquireBucketAdvisoryLocks (source offset %d relative to authorizeRun) must come BEFORE "+
			"the total-unit-cap check (offset %d), not after -- a run over cap must already hold this "+
			"transaction's bucket locks before denyRun's bulk hasActive writes, reopening the codex "+
			"round-6 deadlock against a concurrent same-bucket dispatch pass otherwise (CHAOS-4586)",
			file, lockIndex, capCheckIndex)
	}
}
