package syncreconciler

import (
	"context"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// CHAOS-4583: TerminalOutboxClose only ever runs FORWARD, on newly-arriving
// rows, one bounded pass per reconciler tick. It does nothing about the
// EXISTING backlog for the three kinds it closes (dispatch_sync_run,
// finalize_sync_run, reference_discovery -- post_sync is out of scope, see
// terminal_outbox_close.go) -- local read 6568/6568 non-terminal across all
// four kinds, prod ~19.7k, both accumulated since 2026-06-29/2026-06-25. This
// file is the one-time (or as-needed) operator-invoked backlog reaper, wired
// to `dev-health-workerctl sync-dispatch-outbox close-backlog`, mirroring
// CHAOS-4548's dry-run-first cleanup verb shape (`providersync
// retire-stale-linear-project-ownership`): --dry-run reports what a real
// pass would do, across every org, without writing anything.
//
// Idempotent: a real pass that finds nothing left to close is a true no-op
// (TerminalOutboxClose.Step's own UPDATE...FROM candidates matches zero
// rows), so running this verb twice in a row is safe and the second run
// reports zero.

// maxBacklogReapPasses bounds the real (non-dry-run) reap to a finite number
// of TerminalOutboxClose.Step batches. The backlog is finite and each pass
// commits durably, so a pass that closes fewer than a full batch means the
// backlog for that tick is exhausted; capping the LOOP (rather than looping
// until literally zero forever) keeps one CLI invocation bounded even if a
// concurrent writer keeps arming new 'dispatched' rows that immediately
// qualify.
const maxBacklogReapPasses = 500

// TerminalOutboxBacklogOutcome is ReapTerminalOutboxBacklog's report.
type TerminalOutboxBacklogOutcome struct {
	DryRun bool `json:"dry_run"`
	// CandidatesByKind is the dry-run preview: an EXACT, unbounded count of
	// every row the real pass would close right now, per kind -- not capped
	// by batchSize the way a single TerminalOutboxClose.Step call would be,
	// since the whole point of a preview is to see the full backlog size
	// before running for real.
	CandidatesByKind map[string]int64 `json:"candidates_by_kind,omitempty"`
	// ClosedByKind and ClosedByOutcome are populated only when dryRun is
	// false: the total rows actually closed, summed across every bounded
	// pass this call ran.
	ClosedByKind    map[string]int64            `json:"closed_by_kind,omitempty"`
	ClosedByOutcome map[string]map[string]int64 `json:"closed_by_outcome,omitempty"`
	// Passes is how many TerminalOutboxClose.Step batches this call actually
	// ran (always 0 for a dry run).
	Passes int `json:"passes"`
	// PassLimitReached is true only if maxBacklogReapPasses was hit while
	// passes were still finding rows to close -- an operator should re-run
	// the verb to continue draining the backlog. False on every dry run and
	// on any real run that converged to zero on its own.
	PassLimitReached bool `json:"pass_limit_reached"`
}

// backlogCandidateCountSQL mirrors the three close*SQLTemplate predicates
// exactly (kind literal, status='dispatched', owner-terminal check, no live
// claim) but with no LIMIT and no FOR UPDATE -- a plain read, safe to run
// against a live production table of any size and safe to run concurrently
// with a real reconciler tick or a real (non-dry-run) reap pass. post_sync is
// deliberately absent -- see TerminalOutboxCloseResult's doc comment
// (terminal_outbox_close.go) for why closing it is unsafe.
const backlogCandidateCountSQL = `
SELECT 'dispatch_sync_run' AS kind, count(*) AS candidates
FROM public.sync_dispatch_outbox AS outbox
JOIN public.sync_runs AS run ON run.id = outbox.sync_run_id
WHERE outbox.kind = 'dispatch_sync_run'
	AND outbox.status = 'dispatched'
	AND run.status IN ('success', 'partial_failed', 'failed')
	AND (outbox.claim_expires_at IS NULL OR outbox.claim_expires_at <= $1)
UNION ALL
SELECT 'finalize_sync_run', count(*)
FROM public.sync_dispatch_outbox AS outbox
JOIN public.sync_runs AS run ON run.id = outbox.sync_run_id
WHERE outbox.kind = 'finalize_sync_run'
	AND outbox.status = 'dispatched'
	AND run.status IN ('success', 'partial_failed', 'failed')
	AND (outbox.claim_expires_at IS NULL OR outbox.claim_expires_at <= $1)
UNION ALL
SELECT 'reference_discovery', count(*)
FROM public.sync_dispatch_outbox AS outbox
JOIN public.sync_run_reference_discoveries AS discovery
	ON discovery.sync_run_id = outbox.sync_run_id
WHERE outbox.kind = 'reference_discovery'
	AND outbox.status = 'dispatched'
	AND discovery.status IN ('success', 'failed')
	AND (outbox.claim_expires_at IS NULL OR outbox.claim_expires_at <= $1)
`

// ReapTerminalOutboxBacklog previews (dryRun) or actually closes (!dryRun)
// the existing sync_dispatch_outbox backlog, across every org. pool must
// carry the coordinator role's grants (the same pool TerminalOutboxClose
// itself requires -- see its doc comment). batchSize bounds each individual
// pass exactly like TerminalOutboxClose.Step's own limit.
func ReapTerminalOutboxBacklog(
	ctx context.Context, pool *pgxpool.Pool, now time.Time, batchSize int, dryRun bool,
) (TerminalOutboxBacklogOutcome, error) {
	if pool == nil || ctx == nil || now.IsZero() ||
		batchSize < minimumStepLimit || batchSize > maximumStepLimit {
		return TerminalOutboxBacklogOutcome{}, ErrInvalidConfiguration
	}
	if err := ctx.Err(); err != nil {
		return TerminalOutboxBacklogOutcome{}, err
	}
	now = now.UTC()

	if dryRun {
		rows, err := pool.Query(ctx, backlogCandidateCountSQL, now)
		if err != nil {
			return TerminalOutboxBacklogOutcome{}, ErrUnavailable
		}
		defer rows.Close()
		candidates := make(map[string]int64, 4)
		for rows.Next() {
			var kind string
			var count int64
			if scanErr := rows.Scan(&kind, &count); scanErr != nil {
				return TerminalOutboxBacklogOutcome{}, ErrUnavailable
			}
			candidates[kind] = count
		}
		if err := rows.Err(); err != nil {
			return TerminalOutboxBacklogOutcome{}, ErrUnavailable
		}
		return TerminalOutboxBacklogOutcome{DryRun: true, CandidatesByKind: candidates}, nil
	}

	closer, err := NewTerminalOutboxClose(pool)
	if err != nil {
		return TerminalOutboxBacklogOutcome{}, err
	}
	outcome := TerminalOutboxBacklogOutcome{
		ClosedByKind:    make(map[string]int64, 4),
		ClosedByOutcome: make(map[string]map[string]int64, 4),
	}
	for pass := 0; pass < maxBacklogReapPasses; pass++ {
		if err := ctx.Err(); err != nil {
			return outcome, err
		}
		result, stepErr := closer.Step(ctx, now, batchSize)
		if stepErr != nil {
			return outcome, stepErr
		}
		outcome.Passes++
		outcome.ClosedByKind["dispatch_sync_run"] += result.Dispatch
		outcome.ClosedByKind["finalize_sync_run"] += result.Finalize
		outcome.ClosedByKind["reference_discovery"] += result.Discovery
		for kind, byOutcome := range result.ClosedByOutcome {
			total := outcome.ClosedByOutcome[kind]
			if total == nil {
				total = make(map[string]int64, len(byOutcome))
				outcome.ClosedByOutcome[kind] = total
			}
			for outcomeName, count := range byOutcome {
				total[outcomeName] += count
			}
		}
		if result.Dispatch+result.Finalize+result.Discovery == 0 {
			return outcome, nil
		}
	}
	outcome.PassLimitReached = true
	return outcome, nil
}
