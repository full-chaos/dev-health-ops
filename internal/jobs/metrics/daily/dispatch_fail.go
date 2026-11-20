package daily

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/full-chaos/dev-health-ops/internal/jobruntime"
)

// DispatchEndedRunFailedLogMessage is the WARN line of a dispatch that ended
// its run failed because it could not materialize it.
const DispatchEndedRunFailedLogMessage = "daily metrics dispatch ended the run failed"

// FailDispatchPermanently ends a run that its dispatch could not materialize:
// status 'running', no partition, and no finalize handoff. Nothing else will
// ever run such a run once its last dispatch attempt ends: the partitions it
// would have computed do not exist, and no finalize was handed off. The run
// ends failed / failed, so the day reads unknown until a later run computes it.
//
// A run with a partition belongs to the partition layer, and a run with the
// handoff (FinalizeHandoffKey in the outbox, any status: an empty discovery)
// belongs to its finalize; neither is written. It reports whether it wrote the
// run.
func (store *PostgresStore) FailDispatchPermanently(ctx context.Context, run Run) (bool, error) {
	if !store.valid() || !validUUID(run.ID) {
		return false, ErrUnavailable
	}
	tx, err := store.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return false, ErrUnavailable
	}
	defer func() {
		rollbackCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		defer cancel()
		_ = tx.Rollback(rollbackCtx)
	}()
	if err := store.lockMarkerDayForRun(ctx, tx, run.ID); err != nil {
		return false, err
	}
	var status string
	err = tx.QueryRow(ctx, `
SELECT status FROM public.daily_metrics_runs WHERE id = $1::uuid FOR UPDATE`, run.ID).Scan(&status)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, ErrUnavailable
	}
	if status != "running" {
		return false, nil
	}
	// A second statement, after the lock: under Read Committed it takes a new
	// snapshot, so it sees the partitions or the handoff a materialize that held
	// the run lock committed while this transaction waited for it.
	var hasPartitions, handedOff bool
	if err := tx.QueryRow(ctx, `
SELECT EXISTS (SELECT 1 FROM public.daily_metrics_partitions WHERE run_id = $1::uuid),
  EXISTS (SELECT 1 FROM public.worker_job_outbox WHERE dedupe_key = $2)`,
		run.ID, FinalizeHandoffKey(run.ID)).Scan(&hasPartitions, &handedOff); err != nil {
		return false, ErrUnavailable
	}
	if hasPartitions || handedOff {
		return false, nil
	}
	now := store.now().UTC()
	command, err := tx.Exec(ctx, `
UPDATE public.daily_metrics_runs
SET status = 'failed', finalization_status = 'failed', finalized_at = $1, updated_at = $1
WHERE id = $2::uuid AND status = 'running'`, now, run.ID)
	if err != nil {
		return false, ErrUnavailable
	}
	if command.RowsAffected() != 1 {
		return false, nil
	}
	if err := tx.Commit(ctx); err != nil {
		return false, ErrUnavailable
	}
	store.markerSyncAfterCommit(ctx, run)
	return true, nil
}

// dispatchFailer is the optional store capability behind failUnmaterializedRun.
type dispatchFailer interface {
	FailDispatchPermanently(ctx context.Context, run Run) (bool, error)
}

// failUnmaterializedRun ends the run when this dispatch attempt is its last
// chance to materialize it. It never changes the attempt's own result.
func (handler *Dispatcher) failUnmaterializedRun(
	ctx context.Context, execution *jobruntime.Execution[jobruntime.DailyMetricsDispatchArgs], run Run, step string,
) {
	failer, ok := handler.store.(dispatchFailer)
	if !ok {
		return
	}
	failCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
	defer cancel()
	failed, err := failer.FailDispatchPermanently(failCtx, run)
	logger := execution.Logger
	if logger == nil {
		logger = slog.Default()
	}
	if err != nil {
		logger.Error("daily metrics dispatch terminal write failed",
			"error", err, "run_id", run.ID, "organization_id", run.OrganizationID, "step", step)
		return
	}
	if !failed {
		return
	}
	logger.Warn(DispatchEndedRunFailedLogMessage,
		"run_id", run.ID, "organization_id", run.OrganizationID,
		"target_day", run.TargetDay.Format(dailyTargetDayLayout), "step", step,
		"attempt", execution.Attempt, "max_attempts", execution.Definition.MaxAttempts)
	if handler.touchedDrainer != nil {
		handler.touchedDrainer.DrainTouchedDays(ctx, run.OrganizationID, "e:"+run.ID)
	}
}

// lastDispatchAttempt reports whether River will not run this dispatch again.
// An unset MaxAttempts is never the last attempt.
func lastDispatchAttempt(execution *jobruntime.Execution[jobruntime.DailyMetricsDispatchArgs]) bool {
	return execution.Definition.MaxAttempts > 0 && execution.Attempt >= execution.Definition.MaxAttempts
}
