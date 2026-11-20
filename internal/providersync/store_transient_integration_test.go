//go:build integration

package providersync

import (
	"context"
	"errors"
	"fmt"
	"io"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/syncrunrollup"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/domainroledb"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// storeFault is a fault trigger installed on one table: it fires its action on
// the first `budget` rows it sees, then lets every later row pass. The counter
// is a sequence, so a fire survives the rollback the fire causes.
type storeFault struct {
	table  string
	timing string // "BEFORE" or "DEFERRED" (a constraint trigger that fires at COMMIT)
	action string // "deadlock", "serialization" or "terminate"
	budget int
}

var storeFaultActions = map[string]string{
	"deadlock":      `RAISE EXCEPTION 'injected deadlock' USING ERRCODE = 'deadlock_detected';`,
	"serialization": `RAISE EXCEPTION 'injected serialization failure' USING ERRCODE = 'serialization_failure';`,
	"terminate":     `PERFORM pg_terminate_backend(pg_backend_pid());`,
}

func installStoreFault(t *testing.T, ctx context.Context, pool *pgxpool.Pool, fault storeFault) {
	t.Helper()
	action, ok := storeFaultActions[fault.action]
	if !ok || (fault.table != "sync_runs" && fault.table != "sync_run_units") {
		t.Fatalf("unknown fault %+v", fault)
	}
	trigger := fmt.Sprintf(`CREATE TRIGGER store_fault_14371 BEFORE UPDATE ON public.%s
FOR EACH ROW EXECUTE FUNCTION public.store_fault_14371_fire()`, fault.table)
	if fault.timing == "DEFERRED" {
		trigger = fmt.Sprintf(`CREATE CONSTRAINT TRIGGER store_fault_14371 AFTER UPDATE ON public.%s
DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION public.store_fault_14371_fire()`, fault.table)
	}
	for _, statement := range []string{
		`CREATE SEQUENCE public.store_fault_14371_seen`,
		fmt.Sprintf(`CREATE FUNCTION public.store_fault_14371_fire() RETURNS trigger LANGUAGE plpgsql AS $fault$
BEGIN
  IF nextval('public.store_fault_14371_seen') <= %d THEN
    %s
  END IF;
  IF TG_OP = 'DELETE' THEN RETURN OLD; END IF;
  RETURN NEW;
END $fault$`, fault.budget, action),
		trigger,
	} {
		if _, err := pool.Exec(ctx, statement); err != nil {
			t.Fatal(err)
		}
	}
}

// storeFaultSeen is how many rows reached the fault trigger: one per attempt
// of the write when the trigger sits on a table the write updates once.
func storeFaultSeen(t *testing.T, ctx context.Context, pool *pgxpool.Pool) int {
	t.Helper()
	var seen int64
	var called bool
	if err := pool.QueryRow(ctx, `SELECT last_value, is_called FROM public.store_fault_14371_seen`).Scan(&seen, &called); err != nil {
		t.Fatal(err)
	}
	if !called {
		return 0
	}
	return int(seen)
}

// countRetrySleeps replaces the jittered wait (C3: no wall clock in a cell).
func countRetrySleeps(repository *PostgresRepository) *int {
	sleeps := new(int)
	repository.storeRetrySleep = func(context.Context, int) error {
		*sleeps++
		return nil
	}
	return sleeps
}

type unitState struct {
	status    string
	leaseNull bool
	completed int
	failed    int
	ledger    int
}

func readUnitState(t *testing.T, ctx context.Context, pool *pgxpool.Pool, unitID string) unitState {
	t.Helper()
	var state unitState
	if err := pool.QueryRow(ctx, `
SELECT unit.status, unit.lease_owner IS NULL, run.completed_units, run.failed_units,
       (SELECT count(*) FROM public.sync_executed_proof_ledger
         WHERE provider = btrim(lower(unit.provider)) AND dataset_key = btrim(lower(unit.dataset_key)))
FROM public.sync_run_units AS unit
JOIN public.sync_runs AS run ON run.id = unit.sync_run_id
WHERE unit.id = $1`, unitID).Scan(
		&state.status, &state.leaseNull, &state.completed, &state.failed, &state.ledger,
	); err != nil {
		t.Fatal(err)
	}
	return state
}

func wakeArmedCount() uint64 {
	return syncrunrollup.DispatchWakeCount(syncrunrollup.DispatchWakePathProviderUnit, syncrunrollup.DispatchWakeOutcomeArmed)
}

var storeTransientNow = time.Date(2026, time.July, 23, 12, 0, 0, 0, time.UTC)

// S1, S3, S4 and the C4 unit_update site: a Complete whose transaction is
// refused transiently once is retried in place and commits once. C1: the
// rollup, the executed-proof ledger and the dispatch wake count the committed
// outcome once, not once per attempt.
func TestCompleteRetriesATransientRefusalInPlace(t *testing.T) {
	for _, testCase := range []struct {
		name  string
		fault storeFault
	}{
		{"deadlock_at_bump", storeFault{table: "sync_runs", timing: "BEFORE", action: "deadlock", budget: 1}},
		{"deadlock_at_unit_update", storeFault{table: "sync_run_units", timing: "BEFORE", action: "deadlock", budget: 1}},
		{"serialization_at_commit", storeFault{table: "sync_run_units", timing: "DEFERRED", action: "serialization", budget: 1}},
		{"connection_lost_at_bump", storeFault{table: "sync_runs", timing: "BEFORE", action: "terminate", budget: 1}},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			withDispatchWakeFixture(t, func(ctx context.Context, pool *pgxpool.Pool, repository *PostgresRepository) {
				seedRunUnit(t, ctx, pool, "planned")
				sleeps := countRetrySleeps(repository)
				claim := claimFixtureUnit(t, ctx, repository, firstUnitID, storeTransientNow)
				installStoreFault(t, ctx, pool, testCase.fault)
				armedBefore := wakeArmedCount()

				err := repository.Complete(ctx, claim, map[string]any{"records": 1}, nil,
					storeTransientNow, storeTransientNow.Add(2*time.Second))
				if err != nil {
					t.Fatalf("Complete()=%v, want the retried write to commit", err)
				}
				if seen := storeFaultSeen(t, ctx, pool); seen != 2 {
					t.Fatalf("fault trigger saw %d rows, want 2 (one refused attempt, one committed)", seen)
				}
				if *sleeps != 1 {
					t.Fatalf("retry waits=%d, want 1", *sleeps)
				}
				state := readUnitState(t, ctx, pool, firstUnitID)
				if state.status != "success" || !state.leaseNull || state.completed != 1 || state.failed != 0 ||
					state.ledger != 1 {
					t.Fatalf("after Complete: %+v, want success, lease cleared, completed_units 1, one ledger row", state)
				}
				if armed := wakeArmedCount() - armedBefore; armed != 1 {
					t.Fatalf("dispatch wake armed %d times, want once", armed)
				}
			})
		})
	}
}

// S2: a refusal that outlasts the in-place budget is reported as the
// transient store class, never as an invalid configuration, and the unit's
// transaction is rolled back (the handler then releases it).
func TestCompleteTransientRefusalPastTheBudgetIsStoreTransient(t *testing.T) {
	withDispatchWakeFixture(t, func(ctx context.Context, pool *pgxpool.Pool, repository *PostgresRepository) {
		sleeps := countRetrySleeps(repository)
		claim := claimFixtureUnit(t, ctx, repository, firstUnitID, storeTransientNow)
		installStoreFault(t, ctx, pool, storeFault{table: "sync_runs", timing: "BEFORE", action: "deadlock", budget: 100})

		err := repository.Complete(ctx, claim, map[string]any{"records": 1}, nil,
			storeTransientNow, storeTransientNow.Add(2*time.Second))
		var transient *StoreTransientError
		if !errors.As(err, &transient) || !errors.Is(err, ErrStoreTransient) || errors.Is(err, ErrInvalidConfiguration) {
			t.Fatalf("Complete()=%v, want a StoreTransientError and not ErrInvalidConfiguration", err)
		}
		if transient.Write != "complete" || transient.Step != "bump" || transient.SQLState != "40P01" {
			t.Fatalf("transient=%+v, want write=complete step=bump sqlstate=40P01", *transient)
		}
		if seen := storeFaultSeen(t, ctx, pool); seen != storeWriteAttempts {
			t.Fatalf("fault trigger saw %d rows, want %d attempts", seen, storeWriteAttempts)
		}
		if *sleeps != storeWriteAttempts-1 {
			t.Fatalf("retry waits=%d, want %d", *sleeps, storeWriteAttempts-1)
		}
		if state := readUnitState(t, ctx, pool, firstUnitID); state.status != "running" || state.completed != 0 || state.ledger != 0 {
			t.Fatalf("after the refused Complete: %+v, want the unit still running and nothing counted", state)
		}
		if err := repository.ReleaseForRetry(ctx, claim, storeTransientNow.Add(3*time.Second)); err != nil {
			t.Fatalf("ReleaseForRetry()=%v, want the unit released from its slot", err)
		}
		if state := readUnitState(t, ctx, pool, firstUnitID); state.status != "dispatching" || !state.leaseNull {
			t.Fatalf("after release: %+v, want dispatching with no lease", state)
		}
	})
}

// S5 / S6: the same for failTx, the write whose refusal used to leave the
// unit running until its lease expired.
func TestFailRetriesATransientRefusalInPlace(t *testing.T) {
	withDispatchWakeFixture(t, func(ctx context.Context, pool *pgxpool.Pool, repository *PostgresRepository) {
		seedRunUnit(t, ctx, pool, "planned")
		sleeps := countRetrySleeps(repository)
		claim := claimFixtureUnit(t, ctx, repository, firstUnitID, storeTransientNow)
		installStoreFault(t, ctx, pool, storeFault{table: "sync_runs", timing: "BEFORE", action: "deadlock", budget: 1})
		armedBefore := wakeArmedCount()

		if err := repository.Fail(ctx, claim, "provider_error", storeTransientNow, storeTransientNow.Add(2*time.Second)); err != nil {
			t.Fatalf("Fail()=%v, want the retried write to commit", err)
		}
		if seen := storeFaultSeen(t, ctx, pool); seen != 2 || *sleeps != 1 {
			t.Fatalf("fault trigger saw %d rows, waits=%d, want 2 and 1", seen, *sleeps)
		}
		// failTx writes no ledger row; the rollup counts the failure once.
		state := readUnitState(t, ctx, pool, firstUnitID)
		if state.status != "failed" || !state.leaseNull || state.failed != 1 || state.completed != 0 || state.ledger != 0 {
			t.Fatalf("after Fail: %+v, want failed, lease cleared, failed_units 1, no ledger row", state)
		}
		if armed := wakeArmedCount() - armedBefore; armed != 1 {
			t.Fatalf("dispatch wake armed %d times, want once", armed)
		}
	})
}

func TestFailTransientRefusalPastTheBudgetIsStoreTransient(t *testing.T) {
	withDispatchWakeFixture(t, func(ctx context.Context, pool *pgxpool.Pool, repository *PostgresRepository) {
		countRetrySleeps(repository)
		claim := claimFixtureUnit(t, ctx, repository, firstUnitID, storeTransientNow)
		installStoreFault(t, ctx, pool, storeFault{table: "sync_runs", timing: "BEFORE", action: "deadlock", budget: 100})

		err := repository.Fail(ctx, claim, "provider_error", storeTransientNow, storeTransientNow.Add(2*time.Second))
		var transient *StoreTransientError
		if !errors.As(err, &transient) || transient.Write != "fail" || transient.Step != "bump" ||
			errors.Is(err, ErrInvalidConfiguration) {
			t.Fatalf("Fail()=%v, want a StoreTransientError write=fail step=bump", err)
		}
		if seen := storeFaultSeen(t, ctx, pool); seen != storeWriteAttempts {
			t.Fatalf("fault trigger saw %d rows, want %d", seen, storeWriteAttempts)
		}
		if state := readUnitState(t, ctx, pool, firstUnitID); state.status != "running" || state.failed != 0 {
			t.Fatalf("after the refused Fail: %+v, want the unit still running", state)
		}
	})
}

// S7: CompleteLinearWorkItemFamily shares the in-place retry.
func TestCompleteLinearWorkItemFamilyRetriesATransientRefusalInPlace(t *testing.T) {
	withDispatchWakeFixture(t, func(ctx context.Context, pool *pgxpool.Pool, repository *PostgresRepository) {
		sleeps := countRetrySleeps(repository)
		claim, result := seedLinearFamilyCompletion(t, ctx, pool, repository)
		installStoreFault(t, ctx, pool, storeFault{table: "sync_runs", timing: "BEFORE", action: "deadlock", budget: 1})

		completedAt := storeTransientNow.Add(30 * time.Second)
		if err := repository.CompleteLinearWorkItemFamily(ctx, claim, result, storeTransientNow, completedAt); err != nil {
			t.Fatalf("CompleteLinearWorkItemFamily()=%v, want the retried write to commit", err)
		}
		if seen := storeFaultSeen(t, ctx, pool); seen != 2 || *sleeps != 1 {
			t.Fatalf("fault trigger saw %d rows, waits=%d, want 2 and 1", seen, *sleeps)
		}
		if state := readUnitState(t, ctx, pool, claim.ID); state.status != "success" || state.completed != 1 {
			t.Fatalf("after the family completion: %+v, want success counted once", state)
		}
		var watermarks int
		if err := pool.QueryRow(ctx, `
SELECT count(*) FROM public.sync_watermarks
WHERE org_id = 'org-acme' AND last_synced_at IS NOT NULL`).Scan(&watermarks); err != nil {
			t.Fatal(err)
		}
		if watermarks != 5 {
			t.Fatalf("watermarks=%d, want the five family aliases", watermarks)
		}
	})
}

func seedLinearFamilyCompletion(
	t *testing.T, ctx context.Context, pool *pgxpool.Pool, repository *PostgresRepository,
) (Claim, LinearWorkItemsCompletionResult) {
	t.Helper()
	unitID := uuid.NewString()
	flags := `{`
	first := true
	for flag, enabled := range allLinearFamilyFlags(true) {
		if !first {
			flags += `,`
		}
		first = false
		flags += fmt.Sprintf(`%q:%t`, flag, enabled)
	}
	flags += `}`
	if _, err := pool.Exec(ctx, `
INSERT INTO public.integration_datasets (id, org_id, integration_id, dataset_key, options, is_enabled)
VALUES ($1, 'org-acme', $2, 'work-items', '{}'::jsonb, TRUE)`, uuid.NewString(), firstIntegrationID); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `
INSERT INTO public.sync_run_units (
    id, org_id, sync_run_id, integration_id, source_id, provider,
    dataset_key, cost_class, mode, since_at, before_at, status,
    processor_flags, updated_at, created_at, attempts
) VALUES (
    $1, 'org-acme', $2, $3, $4, 'linear', 'work-items', 'medium',
    'incremental', '2026-07-22T12:00:00Z', '2026-07-23T12:00:00Z',
    'dispatching', $5::jsonb, $6, NOW(), 0)`,
		unitID, firstRunID, firstIntegrationID, firstSourceID, flags, storeTransientNow); err != nil {
		t.Fatal(err)
	}
	claim := claimFixtureUnit(t, ctx, repository, unitID, storeTransientNow)
	_, route := linearTypedLifecycleFixture(t)
	route.Watermark = claim.BeforeAt
	route.Result.Evidence = route.Evidence
	audits := make([]LinearWorkItemEffectAudit, 0, len(route.Effects))
	for _, effect := range route.Effects {
		audits = append(audits, LinearWorkItemEffectAudit{
			Destination: effect.Destination, Rows: len(effect.Rows),
			ContentDigest: effect.ContentDigest, Readback: LinearAliasReadbackExact,
		})
	}
	result, err := buildLinearWorkItemsCompletionResult(claim, route, audits, *claim.BeforeAt)
	if err != nil {
		t.Fatal(err)
	}
	return claim, result
}

// lostAckAfterCommit is the commit seam for C2: the REAL Complete/failTx
// transaction commits, then the caller sees a lost connection, so the
// outcome-unknown branch must read the unit back.
func lostAckAfterCommit(calls *int) func(context.Context, pgx.Tx) error {
	return func(ctx context.Context, tx pgx.Tx) error {
		*calls++
		if err := tx.Commit(ctx); err != nil {
			return err
		}
		return io.ErrUnexpectedEOF
	}
}

// S8 (C2): a write that committed although its acknowledgement was lost is
// read back from the row the real writer produced (completeUnitSQL /
// failUnitSQL stamp last_heartbeat_at = completedAt), reported as success,
// and not written a second time.
func TestUnitWriteWithALostCommitAcknowledgementReadsBackTheCommit(t *testing.T) {
	for _, write := range []string{"complete", "fail"} {
		t.Run(write, func(t *testing.T) {
			withDispatchWakeFixture(t, func(ctx context.Context, pool *pgxpool.Pool, repository *PostgresRepository) {
				seedRunUnit(t, ctx, pool, "planned")
				sleeps := countRetrySleeps(repository)
				commits := 0
				repository.commitTx = lostAckAfterCommit(&commits)
				claim := claimFixtureUnit(t, ctx, repository, firstUnitID, storeTransientNow)
				armedBefore := wakeArmedCount()
				completedAt := storeTransientNow.Add(2 * time.Second)

				var err error
				wantStatus, wantCompleted, wantFailed := "success", 1, 0
				if write == "complete" {
					err = repository.Complete(ctx, claim, map[string]any{"records": 1}, nil, storeTransientNow, completedAt)
				} else {
					err = repository.Fail(ctx, claim, "provider_error", storeTransientNow, completedAt)
					wantStatus, wantCompleted, wantFailed = "failed", 0, 1
				}
				if err != nil {
					t.Fatalf("%s()=%v, want the read-back to find the commit", write, err)
				}
				if commits != 1 || *sleeps != 0 {
					t.Fatalf("commits=%d waits=%d, want one commit and no retry", commits, *sleeps)
				}
				state := readUnitState(t, ctx, pool, firstUnitID)
				if state.status != wantStatus || state.completed != wantCompleted || state.failed != wantFailed {
					t.Fatalf("after %s: %+v, want %s counted once", write, state, wantStatus)
				}
				if armed := wakeArmedCount() - armedBefore; armed != 1 {
					t.Fatalf("dispatch wake armed %d times, want once (from the read-back)", armed)
				}
			})
		})
	}
}

// C2 negative: the write did NOT commit and the unit's expired lease was
// recovered by another owner (Claim's expired-lease recovery) before the
// read-back: the answer is ErrLeaseLost, never a success and never a retry.
func TestUnitWriteWithALostCommitAfterLeaseRecoveryIsLeaseLost(t *testing.T) {
	withDispatchWakeFixture(t, func(ctx context.Context, pool *pgxpool.Pool, repository *PostgresRepository) {
		sleeps := countRetrySleeps(repository)
		claim := claimFixtureUnit(t, ctx, repository, firstUnitID, storeTransientNow)
		commits := 0
		repository.commitTx = func(ctx context.Context, tx pgx.Tx) error {
			commits++
			if err := tx.Rollback(ctx); err != nil {
				return err
			}
			recovered, err := repository.Claim(ctx, ClaimRequest{
				UnitID: firstUnitID, OrgID: "org-acme", Owner: uuid.NewString(),
				Now: storeTransientNow.Add(5 * time.Minute), LeaseDuration: time.Minute, AllowExpiredRecovery: true,
			})
			if err != nil || !recovered.Recovered {
				return fmt.Errorf("lease recovery premise: recovered=%v err=%v", recovered.Recovered, err)
			}
			return io.ErrUnexpectedEOF
		}
		err := repository.Complete(ctx, claim, map[string]any{"records": 1}, nil,
			storeTransientNow, storeTransientNow.Add(2*time.Second))
		if !errors.Is(err, ErrLeaseLost) {
			t.Fatalf("Complete()=%v, want ErrLeaseLost", err)
		}
		if commits != 1 || *sleeps != 0 {
			t.Fatalf("commits=%d waits=%d, want one attempt and no retry", commits, *sleeps)
		}
		if state := readUnitState(t, ctx, pool, firstUnitID); state.status != "running" || state.completed != 0 {
			t.Fatalf("after the lost commit: %+v, want the recovered unit running and nothing counted", state)
		}
	})
}

// The read-back is the one new statement: it runs under the worker's domain
// role (NOSUPERUSER, the go-river-migrate grants) without a denial.
func TestUnitWriteReadBackRunsUnderTheDomainRole(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	venue := domainroledb.Start(t, ctx, "store14371")
	venue.RequireReady(t, ctx)
	seedProviderSyncFixture(t, ctx, venue.Admin)
	failingUnitID := seedRunUnit(t, ctx, venue.Admin, "dispatching")
	repository, err := NewPostgresRepository(venue.Domain)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(repository.Close)
	commits := 0
	repository.commitTx = lostAckAfterCommit(&commits)
	completedAt := storeTransientNow.Add(2 * time.Second)
	if err := repository.Complete(ctx, claimFixtureUnit(t, ctx, repository, firstUnitID, storeTransientNow),
		map[string]any{"records": 1}, nil, storeTransientNow, completedAt); err != nil {
		t.Fatalf("Complete()=%v under the domain role", err)
	}
	if err := repository.Fail(ctx, claimFixtureUnit(t, ctx, repository, failingUnitID, storeTransientNow),
		"provider_error", storeTransientNow, completedAt); err != nil {
		t.Fatalf("Fail()=%v under the domain role", err)
	}
	if commits != 2 {
		t.Fatalf("commits=%d, want 2 (premise: both writes took the read-back path)", commits)
	}
	if denied := venue.Denials.All(); len(denied) != 0 {
		t.Fatalf("domain role denied %d statements: %q", len(denied), denied)
	}
}
