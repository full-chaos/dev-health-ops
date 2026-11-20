package syncdispatchruntime

import (
	"context"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/riverqueue/river"

	"github.com/full-chaos/dev-health-ops/internal/syncbudget"
)

// acp lock-1 ruling (CHAOS-14250): the budget estimates of a Dispatch pass run under its bucket locks
// (observeRun/enforceRun after authorizeRun), and a PagerDuty unit's estimate hydrates its credential, which calls out.
// The pass therefore hydrates those credentials before authorizeRun (preHydratePass) and runs the rest of the pass on
// a context that keeps every estimate off the network (syncbudget.WithLockedHydrations).

// passPreHydrator is an estimator that hydrates a pass's credentials ahead of the locks (*InProcessBudgetEstimator).
type passPreHydrator interface {
	PreHydratePass(ctx context.Context, tx pgx.Tx, runID string, now time.Time) *syncbudget.PassHydrations
}

// preHydratePass runs the estimator's pre-step and returns the context the locked section runs on. An estimator with
// no pre-step (a test fake that never hydrates) leaves the context as it is.
func preHydratePass(ctx context.Context, tx pgx.Tx, estimator budgetEstimator, runID string, now time.Time) context.Context {
	hydrator, ok := estimator.(passPreHydrator)
	if !ok {
		return ctx
	}
	return syncbudget.WithLockedHydrations(ctx, hydrator.PreHydratePass(ctx, tx, runID, now))
}

// passPagerDutyGroupsSQL names the (org, run, integration) of every PagerDuty unit the pass's locked estimates can
// read: the run's own units (observeRun's and enforceRun's candidates are among them) and every active unit
// system-wide (activeBudgetConsumption's predicate, with the same arguments).
const passPagerDutyGroupsSQL = `
SELECT DISTINCT org_id, sync_run_id::text, integration_id::text
FROM public.sync_run_units
WHERE provider = 'pagerduty'
  AND (sync_run_id = $1::uuid
       OR (status = $2 AND updated_at > $3)
       OR (status = $4 AND (lease_expires_at IS NULL OR lease_expires_at > $5)))
ORDER BY 1, 2, 3`

// PreHydratePass is the pass's pre-step: it reads the PagerDuty unit groups the pass's locked estimates can read and
// hydrates each group's credential once, with the network, into a new record. It reads inside a savepoint (released
// when no PagerDuty unit is found, else rolled back), so a failed read never aborts the pass's transaction; a failed read leaves
// the record empty, and every locked PagerDuty estimate then takes the guard. The record's tokens must hold until the
// pass's deadline (the job context's, else River's default job timeout).
func (estimator *InProcessBudgetEstimator) PreHydratePass(ctx context.Context, tx pgx.Tx, runID string, now time.Time) *syncbudget.PassHydrations {
	if estimator == nil {
		return syncbudget.NewPassHydrations(nil)
	}
	record := syncbudget.NewPassHydrations(estimator.clock)
	if tx == nil {
		return record
	}
	savepoint, err := tx.Begin(ctx)
	if err != nil {
		return record
	}
	groups, err := passPagerDutyGroups(ctx, savepoint, runID, now)
	if err != nil {
		_ = savepoint.Rollback(ctx)
		return record
	}
	if len(groups) == 0 {
		// The read succeeded and nothing was written: release the savepoint (a pass of a run with no PagerDuty unit
		// adds no rollback to its transaction).
		_ = savepoint.Commit(ctx)
		return record
	}
	// Hydration reads through the savepoint; it is always rolled back (the pre-step writes nothing), so a failed read
	// never aborts the pass's transaction.
	defer func() { _ = savepoint.Rollback(ctx) }()
	until := estimator.clock().Add(river.JobTimeoutDefault)
	if deadline, ok := ctx.Deadline(); ok {
		until = deadline
	}
	loader := estimator.loader
	loader.DB = savepoint
	loader.PreHydratePagerDuty(ctx, record, groups, until)
	return record
}

func passPagerDutyGroups(ctx context.Context, tx pgx.Tx, runID string, now time.Time) ([]syncbudget.PassUnit, error) {
	rows, err := tx.Query(ctx, passPagerDutyGroupsSQL,
		runID, syncRunUnitStatusDispatching, staleDispatchCutoff(now), syncRunUnitStatusRunning, now)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var groups []syncbudget.PassUnit
	for rows.Next() {
		var group syncbudget.PassUnit
		if err := rows.Scan(&group.OrgID, &group.RunID, &group.IntegrationID); err != nil {
			return nil, err
		}
		groups = append(groups, group)
	}
	return groups, rows.Err()
}
