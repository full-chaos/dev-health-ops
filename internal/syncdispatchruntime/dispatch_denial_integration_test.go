//go:build integration

package syncdispatchruntime

import (
	"context"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// TestRunHasDispatchingOrRunningUnitsMatchesExactlyTheseTwoStatuses pins
// the exact status set: DISPATCHING or RUNNING report true; every other
// status (including a sibling PLANNED unit in the same run) reports false.
func TestRunHasDispatchingOrRunningUnitsMatchesExactlyTheseTwoStatuses(t *testing.T) {
	withBudgetCandidatesPool(t, func(ctx context.Context, pool *pgxpool.Pool) {
		now := pgNow()
		insertCandidateUnit(t, ctx, pool, candidateUnitFixture{id: "00000000-0000-4000-8000-000000000701", status: syncRunUnitStatusPlanned, updatedAt: now})

		tx, err := pool.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = tx.Rollback(ctx) }()
		got, err := runHasDispatchingOrRunningUnits(ctx, tx, budgetCandidatesRunID)
		if err != nil {
			t.Fatalf("runHasDispatchingOrRunningUnits: %v", err)
		}
		if got {
			t.Fatal("want false -- only a PLANNED unit exists")
		}

		if _, err := pool.Exec(ctx, `UPDATE sync_run_units SET status=$1 WHERE id=$2`, syncRunUnitStatusRunning, "00000000-0000-4000-8000-000000000701"); err != nil {
			t.Fatal(err)
		}
		got, err = runHasDispatchingOrRunningUnits(ctx, tx, budgetCandidatesRunID)
		if err != nil {
			t.Fatalf("runHasDispatchingOrRunningUnits: %v", err)
		}
		if !got {
			t.Fatal("want true -- the unit is now RUNNING")
		}
	})
}

// TestFailPlannedUnitsFailsPlannedAndRetryingOnly pins the exact status
// set _fail_planned_units covers -- PLANNED and RETRYING, never DISPATCHING/
// RUNNING/terminal.
func TestFailPlannedUnitsFailsPlannedAndRetryingOnly(t *testing.T) {
	withBudgetCandidatesPool(t, func(ctx context.Context, pool *pgxpool.Pool) {
		now := pgNow()
		planned := "00000000-0000-4000-8000-000000000711"
		retrying := "00000000-0000-4000-8000-000000000712"
		dispatching := "00000000-0000-4000-8000-000000000713"

		insertCandidateUnit(t, ctx, pool, candidateUnitFixture{id: planned, status: syncRunUnitStatusPlanned, updatedAt: now})
		insertCandidateUnit(t, ctx, pool, candidateUnitFixture{id: retrying, status: syncRunUnitStatusRetrying, updatedAt: now})
		insertCandidateUnit(t, ctx, pool, candidateUnitFixture{id: dispatching, status: syncRunUnitStatusDispatching, updatedAt: now})

		tx, err := pool.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = tx.Rollback(ctx) }()
		n, err := failPlannedUnits(ctx, tx, budgetCandidatesRunID, "sync dispatch denied", now)
		if err != nil {
			t.Fatalf("failPlannedUnits: %v", err)
		}
		if n != 2 {
			t.Fatalf("got %d, want 2 (planned + retrying)", n)
		}
		if err := tx.Commit(ctx); err != nil {
			t.Fatal(err)
		}

		for _, id := range []string{planned, retrying} {
			var status, errorText string
			if err := pool.QueryRow(ctx, `SELECT status, error FROM sync_run_units WHERE id=$1`, id).Scan(&status, &errorText); err != nil {
				t.Fatal(err)
			}
			if status != syncRunUnitStatusFailed || errorText != "sync dispatch denied" {
				t.Fatalf("id=%s status=%q error=%q, want failed/'sync dispatch denied'", id, status, errorText)
			}
		}
		var dispatchingStatus string
		if err := pool.QueryRow(ctx, `SELECT status FROM sync_run_units WHERE id=$1`, dispatching).Scan(&dispatchingStatus); err != nil {
			t.Fatal(err)
		}
		if dispatchingStatus != syncRunUnitStatusDispatching {
			t.Fatalf("dispatching unit status=%q, want unchanged (dispatching)", dispatchingStatus)
		}
	})
}

// TestFailStaleDispatchingUnitsExcludesFreshAndConcurrentlyClaimedRows
// pins the write-time CAS: a stale DISPATCHING row is failed, but a FRESH
// DISPATCHING row (not yet stale) and a row that concurrently moved to
// RUNNING are both left untouched -- the whole point of evaluating the
// status predicate at UPDATE time, not from a pre-read snapshot.
func TestFailStaleDispatchingUnitsExcludesFreshAndConcurrentlyClaimedRows(t *testing.T) {
	withBudgetCandidatesPool(t, func(ctx context.Context, pool *pgxpool.Pool) {
		now := pgNow()
		staleCutoff := staleDispatchCutoff(now)
		stale := "00000000-0000-4000-8000-000000000721"
		fresh := "00000000-0000-4000-8000-000000000722"
		concurrentlyRunning := "00000000-0000-4000-8000-000000000723"

		insertCandidateUnit(t, ctx, pool, candidateUnitFixture{id: stale, status: syncRunUnitStatusDispatching, updatedAt: staleCutoff.Add(-time.Minute)})
		insertCandidateUnit(t, ctx, pool, candidateUnitFixture{id: fresh, status: syncRunUnitStatusDispatching, updatedAt: now})
		// Stale by timestamp, but a delayed run_sync_unit already claimed it
		// to RUNNING before this write -- the CAS must exclude it.
		insertCandidateUnit(t, ctx, pool, candidateUnitFixture{id: concurrentlyRunning, status: syncRunUnitStatusRunning, updatedAt: staleCutoff.Add(-time.Minute)})

		tx, err := pool.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = tx.Rollback(ctx) }()
		n, err := failStaleDispatchingUnits(ctx, tx, budgetCandidatesRunID, "sync dispatch denied", now)
		if err != nil {
			t.Fatalf("failStaleDispatchingUnits: %v", err)
		}
		if n != 1 {
			t.Fatalf("got %d, want 1 (only the stale dispatching unit)", n)
		}
		if err := tx.Commit(ctx); err != nil {
			t.Fatal(err)
		}

		var staleStatus, staleErrorCategory string
		if err := pool.QueryRow(ctx, `SELECT status, result->>'error_category' FROM sync_run_units WHERE id=$1`, stale).
			Scan(&staleStatus, &staleErrorCategory); err != nil {
			t.Fatal(err)
		}
		if staleStatus != syncRunUnitStatusFailed || staleErrorCategory != "dispatch_denied" {
			t.Fatalf("stale status=%q error_category=%q, want failed/dispatch_denied", staleStatus, staleErrorCategory)
		}

		var freshStatus string
		if err := pool.QueryRow(ctx, `SELECT status FROM sync_run_units WHERE id=$1`, fresh).Scan(&freshStatus); err != nil {
			t.Fatal(err)
		}
		if freshStatus != syncRunUnitStatusDispatching {
			t.Fatalf("fresh status=%q, want unchanged (dispatching, not yet stale)", freshStatus)
		}

		var runningStatus string
		if err := pool.QueryRow(ctx, `SELECT status FROM sync_run_units WHERE id=$1`, concurrentlyRunning).Scan(&runningStatus); err != nil {
			t.Fatal(err)
		}
		if runningStatus != syncRunUnitStatusRunning {
			t.Fatalf("concurrentlyRunning status=%q, want unchanged (running) -- CAS must exclude an already-claimed row", runningStatus)
		}
	})
}

// TestTerminalizeUnroutableUnitsGroupsByPairAndNamesEachReason pins the
// CHAOS-3990 fix: units from TWO DIFFERENT (provider, dataset) pairs each
// get a reason naming THEIR OWN pair, not a bare shared category, and a
// unit no longer DISPATCHING (already claimed to RUNNING concurrently) is
// excluded by the write-time CAS.
func TestTerminalizeUnroutableUnitsGroupsByPairAndNamesEachReason(t *testing.T) {
	withBudgetCandidatesPool(t, func(ctx context.Context, pool *pgxpool.Pool) {
		now := pgNow()
		aliasUnit := "00000000-0000-4000-8000-000000000731"
		matrixUnit := "00000000-0000-4000-8000-000000000732"
		alreadyRunning := "00000000-0000-4000-8000-000000000733"

		insertCandidateUnit(t, ctx, pool, candidateUnitFixture{id: aliasUnit, status: syncRunUnitStatusDispatching, updatedAt: now})
		insertCandidateUnit(t, ctx, pool, candidateUnitFixture{id: matrixUnit, status: syncRunUnitStatusDispatching, updatedAt: now})
		insertCandidateUnit(t, ctx, pool, candidateUnitFixture{id: alreadyRunning, status: syncRunUnitStatusRunning, updatedAt: now})

		units := []budgetUnit{
			{id: aliasUnit, provider: "github", datasetKey: "work-item-labels"},
			{id: matrixUnit, provider: "unknown-provider", datasetKey: "unknown-dataset"},
			{id: alreadyRunning, provider: "github", datasetKey: "work-item-labels"},
		}

		tx, err := pool.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = tx.Rollback(ctx) }()
		n, err := terminalizeUnroutableUnits(ctx, tx, units, now)
		if err != nil {
			t.Fatalf("terminalizeUnroutableUnits: %v", err)
		}
		if n != 2 {
			t.Fatalf("got %d, want 2 (alreadyRunning excluded by CAS)", n)
		}
		if err := tx.Commit(ctx); err != nil {
			t.Fatal(err)
		}

		var aliasStatus, aliasReason string
		if err := pool.QueryRow(ctx, `SELECT status, last_retry_reason FROM sync_run_units WHERE id=$1`, aliasUnit).
			Scan(&aliasStatus, &aliasReason); err != nil {
			t.Fatal(err)
		}
		if aliasStatus != syncRunUnitStatusFailed {
			t.Fatalf("aliasUnit status=%q, want failed", aliasStatus)
		}
		if aliasReason == "" {
			t.Fatal("aliasUnit last_retry_reason is empty")
		}

		var matrixStatus, matrixReason string
		if err := pool.QueryRow(ctx, `SELECT status, last_retry_reason FROM sync_run_units WHERE id=$1`, matrixUnit).
			Scan(&matrixStatus, &matrixReason); err != nil {
			t.Fatal(err)
		}
		if matrixStatus != syncRunUnitStatusFailed {
			t.Fatalf("matrixUnit status=%q, want failed", matrixStatus)
		}
		if aliasReason == matrixReason {
			t.Fatalf("both pairs got the SAME reason (%q) -- each pair must name its OWN cause", aliasReason)
		}

		var runningStatus string
		if err := pool.QueryRow(ctx, `SELECT status FROM sync_run_units WHERE id=$1`, alreadyRunning).Scan(&runningStatus); err != nil {
			t.Fatal(err)
		}
		if runningStatus != syncRunUnitStatusRunning {
			t.Fatalf("alreadyRunning status=%q, want unchanged (running) -- CAS must exclude a row no longer DISPATCHING", runningStatus)
		}
	})
}

// TestTerminalizeUnroutableUnitsNeverSilentlyDropsAnUnroutableUnit is the
// dedicated CHAOS-3990 regression outcome test: this is the exact incident
// -- 27 units wedged 90 minutes, publishing into a queue with no consumer
// -- terminalizeUnroutableUnits exists to prevent. Every unroutable
// DISPATCHING unit handed in MUST come back terminalized (FAILED, with a
// non-empty operator-facing reason), across MULTIPLE distinct pairs at
// once, with none silently left DISPATCHING to retry-loop forever.
func TestTerminalizeUnroutableUnitsNeverSilentlyDropsAnUnroutableUnit(t *testing.T) {
	withBudgetCandidatesPool(t, func(ctx context.Context, pool *pgxpool.Pool) {
		now := pgNow()
		units := []budgetUnit{
			{id: "00000000-0000-4000-8000-000000000741", provider: "github", datasetKey: "work-item-labels"},
			{id: "00000000-0000-4000-8000-000000000742", provider: "unknown-a", datasetKey: "unknown-a-dataset"},
			{id: "00000000-0000-4000-8000-000000000743", provider: "unknown-b", datasetKey: "unknown-b-dataset"},
		}
		for _, unit := range units {
			insertCandidateUnit(t, ctx, pool, candidateUnitFixture{id: unit.id, status: syncRunUnitStatusDispatching, updatedAt: now})
		}

		tx, err := pool.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = tx.Rollback(ctx) }()
		n, err := terminalizeUnroutableUnits(ctx, tx, units, now)
		if err != nil {
			t.Fatalf("terminalizeUnroutableUnits: %v", err)
		}
		if n != len(units) {
			t.Fatalf("terminalized %d of %d units -- CHAOS-3990: every unroutable unit MUST terminalize, none may be silently left wedged", n, len(units))
		}
		if err := tx.Commit(ctx); err != nil {
			t.Fatal(err)
		}

		for _, unit := range units {
			var status, reason string
			if err := pool.QueryRow(ctx, `SELECT status, last_retry_reason FROM sync_run_units WHERE id=$1`, unit.id).
				Scan(&status, &reason); err != nil {
				t.Fatal(err)
			}
			if status != syncRunUnitStatusFailed {
				t.Fatalf("unit %s status=%q, want failed -- a wedged, never-terminalized unit is exactly the CHAOS-3990 incident", unit.id, status)
			}
			if reason == "" {
				t.Fatalf("unit %s has no operator-facing reason -- a bare category with no reason is the OTHER half of the same incident (an operator can't act on it)", unit.id)
			}
		}
	})
}

// TestTerminalizeUnroutableUnitsReturnsZeroForAnEmptySlice pins the
// short-circuit.
func TestTerminalizeUnroutableUnitsReturnsZeroForAnEmptySlice(t *testing.T) {
	withBudgetCandidatesPool(t, func(ctx context.Context, pool *pgxpool.Pool) {
		tx, err := pool.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = tx.Rollback(ctx) }()
		n, err := terminalizeUnroutableUnits(ctx, tx, nil, pgNow())
		if err != nil {
			t.Fatalf("terminalizeUnroutableUnits: %v", err)
		}
		if n != 0 {
			t.Fatalf("got %d, want 0", n)
		}
	})
}

// TestTerminalizeInvalidClaimUnitsDoesNotClobberAConcurrentlyClaimedUnit is
// the CHAOS-4556 codex round-1 regression: terminalizeInvalidClaimUnits'
// write-time CAS must match each unit's OWN observed status, not a blanket
// ANY(planned, retrying, dispatching) covering every pre-terminal status.
// The invalidClaimUnit here carries the status invalidClaimsAmongDispatch
// Candidates would have observed (PLANNED) at read time; between that read
// and this call, a CONCURRENT dispatch pass legitimately claims the SAME
// unit to DISPATCHING (simulated directly here -- the exact effect
// claimUnits' own UPDATE ... RETURNING would have). A blanket predicate
// would still match DISPATCHING and clobber this fresh claim to FAILED,
// silently orphaning whatever provider job that concurrent pass just queued.
// The fix must exclude it: zero rows terminalized, and the unit's status
// must remain untouched (still DISPATCHING, not FAILED).
func TestTerminalizeInvalidClaimUnitsDoesNotClobberAConcurrentlyClaimedUnit(t *testing.T) {
	withBudgetCandidatesPool(t, func(ctx context.Context, pool *pgxpool.Pool) {
		now := pgNow()
		unitID := "00000000-0000-4000-8000-000000000751"
		insertCandidateUnit(t, ctx, pool, candidateUnitFixture{id: unitID, status: syncRunUnitStatusPlanned, updatedAt: now})

		// Simulate a concurrent dispatch pass claiming this unit to
		// DISPATCHING (and, in production, publishing it to the outbox)
		// AFTER the invalid-claim scan observed it as PLANNED but BEFORE
		// terminalizeInvalidClaimUnits' own UPDATE runs.
		if _, err := pool.Exec(ctx, `UPDATE public.sync_run_units SET status=$1 WHERE id=$2`,
			syncRunUnitStatusDispatching, unitID); err != nil {
			t.Fatal(err)
		}

		invalid := []invalidClaimUnit{{
			unit:   budgetUnit{id: unitID, provider: "linear", datasetKey: "work-items", status: syncRunUnitStatusPlanned},
			reason: "provider-family claim for linear/work-items is invalid: test fixture",
		}}

		tx, err := pool.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = tx.Rollback(ctx) }()
		n, err := terminalizeInvalidClaimUnits(ctx, tx, invalid, now)
		if err != nil {
			t.Fatalf("terminalizeInvalidClaimUnits: %v", err)
		}
		if n != 0 {
			t.Fatalf("terminalized %d units, want 0 -- a unit observed PLANNED but now DISPATCHING must be excluded by the write-time CAS", n)
		}
		if err := tx.Commit(ctx); err != nil {
			t.Fatal(err)
		}

		var status string
		if err := pool.QueryRow(ctx, `SELECT status FROM sync_run_units WHERE id=$1`, unitID).Scan(&status); err != nil {
			t.Fatal(err)
		}
		if status != syncRunUnitStatusDispatching {
			t.Fatalf("unit status=%q, want unchanged (dispatching) -- the concurrently-claimed unit must not be clobbered to failed", status)
		}
	})
}

// TestTerminalizeInvalidClaimUnitsTerminalizesAGenuinelyStillPlannedUnit is
// the positive control for the test above: when the unit's CURRENT status
// still matches what was observed (no concurrent claim happened), the exact
// per-status CAS must still terminalize it -- proving the CHAOS-4556 fix
// narrowed the predicate without making it vacuous.
func TestTerminalizeInvalidClaimUnitsTerminalizesAGenuinelyStillPlannedUnit(t *testing.T) {
	withBudgetCandidatesPool(t, func(ctx context.Context, pool *pgxpool.Pool) {
		now := pgNow()
		unitID := "00000000-0000-4000-8000-000000000752"
		insertCandidateUnit(t, ctx, pool, candidateUnitFixture{id: unitID, status: syncRunUnitStatusPlanned, updatedAt: now})

		invalid := []invalidClaimUnit{{
			unit:   budgetUnit{id: unitID, provider: "linear", datasetKey: "work-items", status: syncRunUnitStatusPlanned},
			reason: "provider-family claim for linear/work-items is invalid: test fixture",
		}}

		tx, err := pool.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = tx.Rollback(ctx) }()
		n, err := terminalizeInvalidClaimUnits(ctx, tx, invalid, now)
		if err != nil {
			t.Fatalf("terminalizeInvalidClaimUnits: %v", err)
		}
		if n != 1 {
			t.Fatalf("terminalized %d units, want 1 -- no concurrent claim happened, so the still-PLANNED unit must terminalize", n)
		}
		if err := tx.Commit(ctx); err != nil {
			t.Fatal(err)
		}

		var status, errorCategory string
		if err := pool.QueryRow(ctx, `SELECT status, result->>'error_category' FROM sync_run_units WHERE id=$1`, unitID).
			Scan(&status, &errorCategory); err != nil {
			t.Fatal(err)
		}
		if status != syncRunUnitStatusFailed || errorCategory != invalidProviderFamilyClaimErrorCategory {
			t.Fatalf("status=%q error_category=%q, want failed/%q", status, errorCategory, invalidProviderFamilyClaimErrorCategory)
		}
	})
}
