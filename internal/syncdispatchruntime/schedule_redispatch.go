package syncdispatchruntime

import (
	"context"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// scheduleRedispatch ports _schedule_redispatch verbatim, INCLUDING its
// exception-swallowing. Python's bare `except Exception:` logs
// dispatch_sync_run.redispatch_rearm_failed and never re-raises -- every
// call site in Dispatch() calls this as a fire-and-forget re-arm, and the
// caller-side wrappers Python leaves around some of those calls are
// unreachable dead code precisely BECAUSE this function structurally
// cannot fail its caller. Ported deliberately as a function with no error
// return, not smoothed over as an implementation detail: a Go caller must
// not add its own error handling assuming this can fail the pass.
//
// Opens its OWN transaction rather than accepting the caller's tx --
// matching Python's own separate `with get_postgres_session_sync() as
// session:` block, which is a DIFFERENT session than whatever transaction
// the caller used to reach its own terminal state (Dispatch()'s main
// claim/route/enqueue session has already closed by every call site that
// reaches this).
func scheduleRedispatch(ctx context.Context, pool *pgxpool.Pool, logger *slog.Logger, syncRunID string, availableAt *time.Time, now time.Time) {
	if logger == nil {
		logger = slog.Default()
	}
	countdown := budgetEnvInt("SYNC_DISPATCH_REDISPATCH_COUNTDOWN", 60)
	redispatchAt := now.Add(time.Duration(countdown) * time.Second)
	if availableAt != nil {
		redispatchAt = *availableAt
	}

	fail := func(err error) {
		logger.WarnContext(ctx, "dispatch_sync_run.redispatch_rearm_failed",
			slog.String("sync_run_id", syncRunID), slog.String("error", err.Error()))
	}

	tx, err := pool.Begin(ctx)
	if err != nil {
		fail(err)
		return
	}
	defer func() { _ = tx.Rollback(ctx) }()

	if err := upsertDiscoveryOutboxWakeup(ctx, tx, "", syncRunID, outboxKindDispatchSyncRun, redispatchAt); err != nil {
		fail(err)
		return
	}
	// The second, unconditional-overwrite write Python's own
	// _schedule_redispatch does on top of the LEAST-semantics upsert above:
	// a still-pending, not-yet-claimed row for THIS kind gets its
	// available_at pushed to the newly-computed redispatchAt regardless of
	// which is earlier. Ported as its own explicit statement, not merged
	// into the upsert, to keep the same two-write shape Python has --
	// simplifying to one write would be an improvement, not a port.
	if _, err := tx.Exec(ctx, `
UPDATE public.sync_dispatch_outbox
SET available_at = $2, updated_at = $3
WHERE sync_run_id = $1::uuid AND kind = $4 AND status = 'pending' AND claim_token IS NULL`,
		syncRunID, redispatchAt, now, outboxKindDispatchSyncRun); err != nil {
		fail(err)
		return
	}
	if err := tx.Commit(ctx); err != nil {
		fail(err)
		return
	}
	logger.InfoContext(ctx, "dispatch_sync_run.redispatch_rearmed",
		slog.String("sync_run_id", syncRunID), slog.Int("countdown", countdown), slog.Time("available_at", redispatchAt))
}
