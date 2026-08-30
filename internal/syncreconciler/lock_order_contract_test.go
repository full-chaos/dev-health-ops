package syncreconciler

import (
	"os"
	"strings"
	"testing"
)

// TestReconcilerStepsSortCandidatesBeforeAnyPerCandidateWrite is a
// structural regression guard for codex round 3's finding (CHAOS-4586).
//
// Round 2 fixed a cross-reconciler-replica deadlock (LeaseRepair.Step
// orders its candidates by lease_expires_at, UnreclaimableSweep.Step by
// created_at; two concurrent replicas touching overlapping runs could lock
// them in opposite order) by pre-locking every candidate's run before any
// per-candidate write. Round 3 caught that this pre-lock inverts the lock
// order to run-before-unit against every single-run terminal writer
// elsewhere in the codebase (PostgresRepository.Fail,
// terminalizeUnroutableUnits, etc.), which all lock their sync_run_units
// row first and then call syncrunrollup.Bump -- an ABBA deadlock waiting to
// happen the other way.
//
// Round 4 caught a THIRD reachable inversion: sorting candidates by run id
// (round 3) still lets a SECOND candidate in the SAME run write its unit
// row while this transaction already holds that run's lock from the FIRST
// candidate's Bump call (which holds the lock for the rest of the
// transaction) -- run-before-unit for that one candidate, the same
// inversion round 3 fixed, reachable a different way. Fixed by
// lockSyncRunUnitsAscending: every candidate's unit row locked, up front,
// before this transaction ever touches a run lock at all.
//
// Round 5 caught a FOURTH gap: UnreclaimableSweep.Step never took the
// per-(orgID, provider, costClass) advisory lock dispatch's AuthorizeRun
// and LeaseRepair.Step already take before touching any sync_run_units
// row -- so dispatch's claimUnits (a bulk multi-row UPDATE with no
// row-lock order of its own) and this sweep's own round-4 unit locks
// (ascending by id) could each hold one contested unit and wait on the
// other. Fixed by acquireUnreclaimableBucketLocks, taking the SAME
// advisory lock (same formula, sorted) before ANY row is touched, so the
// two paths can never even be in a position to contend on a row lock.
//
// The actual fix, all four rounds combined: UnreclaimableSweep.Step now
// (0) takes the shared bucket advisory lock (acquireUnreclaimableBucketLocks,
// round 5; LeaseRepair.Step already had its own from before these codex
// rounds), THEN (1) locks every candidate's unit row up front, ascending
// by unit id (lockSyncRunUnitsAscending, round 4), THEN (2) sorts its OWN
// candidates by ascending sync_run_id (round 3), THEN (3) keeps the
// existing write-then-Bump sequence per candidate. This scans the real
// source text of both Step functions so a future edit cannot silently
// drop any of these, or reorder the phases, without this test noticing --
// even before a slow/flaky concurrency integration test would ever catch
// it. A dynamic proof (the real interleaved call sequence, not source
// text) of phases 1-3 for LeaseRepair.Step lives in
// TestLeaseRepairStepLocksUnitsBeforeRunsAndVisitsRunsInAscendingOrder.
func TestReconcilerStepsSortCandidatesBeforeAnyPerCandidateWrite(t *testing.T) {
	tests := []struct {
		file             string
		stepFunc         string
		bucketLockMarker string // optional: the shared advisory-lock call, if this Step has one of its own (round 5)
		writeMarker      string // the first per-candidate write call in the loop
	}{
		{
			file:        "lease_repair.go",
			stepFunc:    "func (repair *LeaseRepair) Step(",
			writeMarker: "markExpiredLeaseRetrying(ctx, tx, candidate",
		},
		{
			file:             "unreclaimable_sweep.go",
			stepFunc:         "func (sweep *UnreclaimableSweep) Step(",
			bucketLockMarker: "acquireUnreclaimableBucketLocks(ctx, tx,",
			writeMarker:      "sweep.terminalize(ctx, tx, candidate",
		},
	}
	for _, test := range tests {
		t.Run(test.file, func(t *testing.T) {
			raw, err := os.ReadFile(test.file)
			if err != nil {
				t.Fatal(err)
			}
			source := string(raw)
			stepStart := strings.Index(source, test.stepFunc)
			if stepStart < 0 {
				t.Fatalf("%s: could not find %q -- has the Step signature changed? update this test's marker", test.file, test.stepFunc)
			}
			body := source[stepStart:]

			lockUnitsIndex := strings.Index(body, "lockSyncRunUnitsAscending(ctx, tx,")
			if lockUnitsIndex < 0 {
				t.Fatalf("%s: Step no longer locks every candidate's unit row up front -- "+
					"this reopens the codex round-4 same-run-multiple-candidates deadlock (CHAOS-4586): "+
					"a second candidate in the same run could write its unit row while this transaction "+
					"already holds that run's lock from an earlier candidate's Bump call", test.file)
			}

			if test.bucketLockMarker != "" {
				bucketLockIndex := strings.Index(body, test.bucketLockMarker)
				if bucketLockIndex < 0 {
					t.Fatalf("%s: could not find %q -- Step no longer takes the shared bucket advisory lock "+
						"before touching sync_run_units, reopening the codex round-5 deadlock against dispatch's "+
						"claimUnits (CHAOS-4586)", test.file, test.bucketLockMarker)
				}
				if bucketLockIndex >= lockUnitsIndex {
					t.Fatalf("%s: the bucket advisory lock (offset %d relative to Step) must come BEFORE "+
						"the unit-row locks (offset %d) -- it exists to keep this pass and a concurrent "+
						"dispatch pass from ever touching an overlapping row at the same time at all",
						test.file, bucketLockIndex, lockUnitsIndex)
				}
			}

			sortIndex := strings.Index(body, "sort.SliceStable(candidates")
			if sortIndex < 0 {
				t.Fatalf("%s: Step no longer sorts candidates by ascending sync_run_id before its write loop -- "+
					"this reopens the codex round-2/round-3 cross-reconciler deadlock (CHAOS-4586): "+
					"LeaseRepair.Step and UnreclaimableSweep.Step must visit any overlapping run set in "+
					"the SAME order as each other", test.file)
			}

			writeIndex := strings.Index(body, test.writeMarker)
			if writeIndex < 0 {
				t.Fatalf("%s: could not find %q -- has the per-candidate write call changed shape? update this test's marker", test.file, test.writeMarker)
			}

			if lockUnitsIndex >= sortIndex {
				t.Fatalf("%s: locking every candidate's unit row (offset %d relative to Step) must come BEFORE "+
					"the candidate sort (offset %d) -- both must happen before any write regardless of their "+
					"relative order, but keeping unit-locking first documents that it is the one that must "+
					"never move after a write has already started", test.file, lockUnitsIndex, sortIndex)
			}

			if sortIndex >= writeIndex {
				t.Fatalf("%s: the candidate sort (source offset %d relative to Step) must come BEFORE "+
					"the first per-candidate write (offset %d), not after -- sorting after writes have "+
					"already started defeats the whole point", test.file, sortIndex, writeIndex)
			}
		})
	}
}
