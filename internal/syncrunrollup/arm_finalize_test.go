package syncrunrollup

import (
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

// recoveryDirs are the packages whose terminalizing writers are RECOVERY
// paths: they move a unit to a terminal status from outside the unit's own
// execution, so nothing downstream of the handler runs afterwards to notice
// the run may now be finishable.
//
// That is the distinction this file guards, and it is why the scope is these
// packages rather than every terminal writer. providersync's per-unit commit
// has always armed the finalizer one line after its Bump;
// syncdispatchruntime's denial paths terminalize the whole RUN directly in the
// same statement, so there is nothing left to finalize. The two seams in
// internal/syncreconciler -- UnreclaimableSweep.terminalize and
// LeaseRepair.Step -- did neither, and that gap is what left run
// 115e6246-6e8c-5f53-a2c4-f6b109daba68 at status='dispatching' with 63 of 63
// units terminal and nothing in the system able to close it.
var recoveryDirs = []string{"../syncreconciler"}

var (
	bumpCall        = regexp.MustCompile(`syncrunrollup\.Bump\(`)
	armFinalizeCall = regexp.MustCompile(`syncrunrollup\.ArmFinalize\(`)
)

// armFinalizeProximityLines bounds how far after a Bump the arm may sit.
//
// A number, not "anywhere in the function", because "anywhere" is satisfied by
// a DIFFERENT candidate's arm in the same loop body -- the exact false pass
// the sibling rollup registry's own doc comment warns about for its
// seamCovered claims. 40 lines comfortably covers a Bump, its error branch and
// an explanatory comment; it does not reach across a loop iteration.
const armFinalizeProximityLines = 40

// TestEveryRecoveryBumpArmsTheFinalizer is the class-level guard, in the shape
// CHAOS-4586 established for Bump itself: the fix covers the defect by
// MECHANISM, not one call site.
//
// Bump keeps completed_units/failed_units live and writes nothing else -- never
// sync_runs.status, never completed_at. A run reaches a terminal status only
// through finalize_sync_run, and the finalizer only re-evaluates a run whose
// dispatch-outbox row has been re-armed. A recovery writer that Bumps without
// arming therefore produces a run whose counters say it is finished and whose
// status says it is still dispatching, permanently.
//
// SCOPE, stated honestly rather than overclaimed: this checks the two
// syncreconciler seams only. It is NOT a whole-codebase registry of arm
// coverage the way crossPackageRollupSeamRegistry is for Bump coverage --
// building that means deciding, per site, whether a writer that terminalizes
// the whole run in one statement needs an arm at all, and an inaccurate
// coverage claim is worse than an admitted gap.
//
// RED CONTROL: deleting either ArmFinalize call in internal/syncreconciler
// fails this test naming the file and line of the orphaned Bump.
func TestEveryRecoveryBumpArmsTheFinalizer(t *testing.T) {
	t.Parallel()
	bumps := 0
	for _, dir := range recoveryDirs {
		entries, err := os.ReadDir(dir)
		if err != nil {
			t.Fatalf("cannot read %s: %v", dir, err)
		}
		for _, entry := range entries {
			name := entry.Name()
			// Test files are excluded: a fake or an assertion legitimately
			// names Bump without arming anything.
			if entry.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
				continue
			}
			path := filepath.Join(dir, name)
			body, err := os.ReadFile(path)
			if err != nil {
				t.Fatalf("cannot read %s: %v", path, err)
			}
			lines := strings.Split(string(body), "\n")
			for index, line := range lines {
				if !bumpCall.MatchString(line) {
					continue
				}
				bumps++
				end := min(index+armFinalizeProximityLines, len(lines))
				window := strings.Join(lines[index:end], "\n")
				if !armFinalizeCall.MatchString(window) {
					t.Errorf(
						"%s:%d calls syncrunrollup.Bump with no syncrunrollup.ArmFinalize within %d lines.\n"+
							"Bump keeps the COUNTERS live and nothing else -- it never writes sync_runs.status "+
							"or completed_at. A recovery writer that terminalizes a run's last non-terminal unit "+
							"and does not re-arm finalize_sync_run leaves that run open forever with every unit "+
							"terminal (measured: run 115e6246, 63/63 terminal, still 'dispatching').",
						path, index+1, armFinalizeProximityLines,
					)
				}
			}
		}
	}
	// The scan must have found something. Without this, a refactor that
	// renamed the package alias, moved both seams, or simply broke the regex
	// turns this test into one that cannot fail -- the vacuous-guard class
	// this repository has been bitten by more than once.
	if bumps < 2 {
		t.Fatalf("found %d syncrunrollup.Bump call sites in %v, want at least 2 "+
			"(UnreclaimableSweep.terminalize and LeaseRepair.Step) -- this guard is not looking at anything",
			bumps, recoveryDirs)
	}
}

// TestArmFinalizeSQLKeepsItsLoadBearingClauses pins the four properties whose
// removal each turns this statement into a different, wrong one. Asserted by
// exact text so a silent deletion fails here rather than in production.
func TestArmFinalizeSQLKeepsItsLoadBearingClauses(t *testing.T) {
	t.Parallel()
	// The conflict target. Anything else and a second row is inserted for a
	// run that already has one, which the unique constraint refuses.
	if !strings.Contains(ArmFinalizeSQL, "ON CONFLICT (sync_run_id, kind)") {
		t.Fatal("ArmFinalizeSQL lost its (sync_run_id, kind) conflict target")
	}
	// The kind. This statement is finalize-only; the post_sync wakeup is a
	// separate literal in syncdispatchruntime by deliberate choice.
	if !strings.Contains(ArmFinalizeSQL, "'finalize_sync_run'") {
		t.Fatal("ArmFinalizeSQL no longer arms finalize_sync_run")
	}
	// The feature_disabled exception, on EVERY column it guards. A run
	// terminated by the entitlement gate parks its row deliberately; re-arming
	// it puts that run back in the finalizer's queue forever. Five columns
	// carry the predicate (status, available_at, dispatched_at, last_error,
	// and the claim pair); counting them is what catches a "simplification"
	// that collapses the repeated CASE and silently drops one.
	if got := strings.Count(ArmFinalizeSQL, "'feature_disabled'"); got != 6 {
		t.Fatalf("ArmFinalizeSQL mentions 'feature_disabled' %d times, want 6 "+
			"(status, available_at, dispatched_at, last_error, claim_token, claim_expires_at) -- "+
			"a dropped guard re-arms a deliberately parked run forever", got)
	}
	// A LIVE claim is preserved. Without this the arm steals a token from a
	// dispatcher mid-flight and two finalizers can run for one run.
	if !strings.Contains(ArmFinalizeSQL, "claim_expires_at > EXCLUDED.updated_at") {
		t.Fatal("ArmFinalizeSQL no longer preserves a live claim; a concurrent finalizer's token would be stolen")
	}
	// Availability moves EARLIER, never later: LEAST, not GREATEST. A
	// GREATEST here would push an already-due finalizer further out on every
	// unit that terminalizes, which is a liveness bug that looks like a typo.
	if !strings.Contains(ArmFinalizeSQL, "LEAST(public.sync_dispatch_outbox.available_at, EXCLUDED.available_at)") {
		t.Fatal("ArmFinalizeSQL no longer takes the EARLIER availability")
	}
	if strings.Contains(ArmFinalizeSQL, "GREATEST(public.sync_dispatch_outbox.available_at") {
		t.Fatal("ArmFinalizeSQL delays availability instead of advancing it")
	}
}
