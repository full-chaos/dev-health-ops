package syncreconciler

import (
	"context"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// runawayDispatchAttempts is the ATTEMPT COUNT at which a dispatch wakeup
// stops being ordinary and starts being a report (CHAOS-4097).
//
// It is derived from the production distribution, not chosen. Read on
// 2026-08-22 across every dispatch_sync_run row in the outbox:
//
//	run_status       transport  rows   max     p99
//	success          celery     3397   2472    72
//	success          river        84     43    43
//	partial_failed   celery      502    615   211
//	partial_failed   river        81  72601 72601   <- CHAOS-4093
//	failed           river        95   6499  6499   <- CHAOS-4093
//
// The p99 of every healthy population is two figures; the CHAOS-4093 rows are
// four and five. 1000 sits an order of magnitude above the worst healthy row
// and an order of magnitude below the cheapest sick one, so it separates the
// two without sitting near either edge -- which is what a threshold has to do
// to still be right when the traffic shape moves.
//
// IT DECIDES NOTHING. Crossing it emits a log line and changes no predicate,
// no window and no write. That is deliberate: a number tuned from one
// observation must never be load-bearing for correctness, and a ceiling that
// SUPPRESSED re-arming would turn a visibility gap into a stalled run the
// first time a legitimately long-lived run crossed it. The bound on the loop
// itself belongs to the sweep (CHAOS-4097 item 1), which removes the stuck
// units the re-arm predicate keeps matching; this only makes the loop
// impossible to miss while it is happening. Nothing alerted on 72601.
const runawayDispatchAttempts = 1000

// runawayDispatchScan caps the report so a widespread degradation cannot turn
// one reconcile pass into an unbounded read or an unbounded burst of log
// lines. Truncation is not silent: the caller is told the report was capped.
const runawayDispatchScan = 20

// RunawayDispatchWakeup is one dispatch outbox row whose attempt count has
// crossed runawayDispatchAttempts while its run is still non-terminal. It
// carries the run rather than the outbox id because the run is what an
// operator can act on, and it carries no error text or payload -- everything
// here is an identifier or a count.
type RunawayDispatchWakeup struct {
	SyncRunID string
	Attempts  int64
}

// MaterializerResult reports rows inserted or re-armed by one bounded
// transaction. Existing pending rows are deliberately not counted because the
// Python reconciler's materialization wrapper leaves them untouched.
type MaterializerResult struct {
	Dispatch  int64
	Finalize  int64
	Discovery int64
	PostSync  int64
	// Runaway is the CHAOS-4097 report. Empty on a healthy pass.
	Runaway []RunawayDispatchWakeup
	// RunawayTruncated says the report hit runawayDispatchScan and there are
	// more rows over the threshold than are listed. A capped report that did
	// not say so would read as "these are all of them", which is the class of
	// quiet under-reporting this whole ticket is about.
	RunawayTruncated bool
}

type materializerBeginFunc func(context.Context) (pgx.Tx, error)

// Materializer reconstructs missing sync-dispatch wakeups from authoritative
// domain state. It is transport-neutral: it neither reads nor mutates transport
// routes, and it never claims or publishes an outbox row.
//
// Its transaction can coexist with a compatibility reconciler because the
// outbox has a unique (sync_run_id, kind) constraint. The first three conflict
// transitions match that compatibility contract; post_sync is deliberately
// stricter and never re-arms an existing row.
type Materializer struct {
	begin materializerBeginFunc
}

func NewMaterializer(pool *pgxpool.Pool) (*Materializer, error) {
	if pool == nil {
		return nil, ErrInvalidConfiguration
	}
	return newMaterializer(pool.Begin)
}

func newMaterializer(begin materializerBeginFunc) (*Materializer, error) {
	if begin == nil {
		return nil, ErrInvalidConfiguration
	}
	return &Materializer{begin: begin}, nil
}

// Step materializes one deterministic candidate window per frozen kind in one
// transaction. staleDispatchCutoff is supplied by command composition so this
// domain component does not duplicate environment policy.
func (materializer *Materializer) Step(
	ctx context.Context,
	now time.Time,
	staleDispatchCutoff time.Time,
	limit int,
) (MaterializerResult, error) {
	if materializer == nil || materializer.begin == nil || ctx == nil || now.IsZero() ||
		staleDispatchCutoff.IsZero() || staleDispatchCutoff.After(now) ||
		limit < minimumStepLimit || limit > maximumStepLimit {
		return MaterializerResult{}, ErrInvalidConfiguration
	}
	if err := ctx.Err(); err != nil {
		return MaterializerResult{}, err
	}
	now = now.UTC()
	staleDispatchCutoff = staleDispatchCutoff.UTC()

	tx, err := materializer.begin(ctx)
	if err != nil || tx == nil {
		return MaterializerResult{}, ErrUnavailable
	}
	defer func() { _ = tx.Rollback(ctx) }()

	result := MaterializerResult{}
	steps := []struct {
		sql   string
		args  []any
		count *int64
	}{
		{materializeDispatchSQL, []any{now, staleDispatchCutoff, limit}, &result.Dispatch},
		{materializeFinalizeSQL, []any{now, limit}, &result.Finalize},
		{materializeDiscoverySQL, []any{now, limit}, &result.Discovery},
		{materializePostSyncSQL, []any{now, limit}, &result.PostSync},
	}
	for _, step := range steps {
		tag, execErr := tx.Exec(ctx, step.sql, step.args...)
		if execErr != nil {
			return MaterializerResult{}, ErrUnavailable
		}
		affected := tag.RowsAffected()
		if affected < 0 || affected > int64(limit) {
			return MaterializerResult{}, ErrUnavailable
		}
		*step.count = affected
	}
	// The report runs in the SAME transaction as the writes above, and after
	// them, so what it reports is the state this pass leaves behind rather
	// than the one it found. It reads only; a failure here must not lose the
	// materialization, so it is not allowed to fail the step.
	runaway, truncated, reportErr := readRunawayDispatchWakeups(ctx, tx)
	if reportErr == nil {
		result.Runaway, result.RunawayTruncated = runaway, truncated
	}
	if err := tx.Commit(ctx); err != nil {
		return MaterializerResult{}, ErrUnavailable
	}
	return result, nil
}

// A run that is already terminal is excluded: its outbox row is inert, and a
// historical high count on a finished run is an archaeology question, not an
// operational one. The 3397 completed celery rows in production would
// otherwise dominate every pass forever.
//
// ORDER BY attempts DESC puts the worst row first, so a capped report is
// still the most useful twenty rows rather than an arbitrary twenty.
//
// # Measured plan, so the next reader does not have to guess (CHAOS-4097)
//
// An adversarial review raised this as a possible full scan and sort of the
// dispatch outbox on every pass. It is not, and the measurement is recorded
// here rather than the reasoning, because the reasoning was wrong in both
// directions. EXPLAIN (ANALYZE, BUFFERS) against production, 2026-08-22:
//
//	Limit … actual time=1.257..1.259 rows=11  Buffers: shared hit=224
//	  Sort  Sort Method: quicksort  Memory: 26kB
//	    Nested Loop
//	      Seq Scan on sync_runs  rows=13, Rows Removed by Filter: 4187
//	      Index Scan using uq_sync_dispatch_outbox_run_kind  loops=13
//	Execution Time: 1.351 ms
//
// The planner drives from sync_runs and probes the outbox through the unique
// (sync_run_id, kind) index, so the outbox is never scanned or sorted whole:
// the sort is over the eleven rows that survive.
//
// The residual cost is the OTHER side, and it is real: the sync_runs scan is
// proportional to all history rather than to the active set, because
// `NOT IN (three terminal statuses)` is not selective enough for the planner
// to reach for ix_sync_runs_status_id. At today's 4200 rows that is 1.35 ms.
// The fix is an indexed access path, not a rewrite of this predicate --
// flipping to a positive `IN` list would be sargable but fail CLOSED, and a
// run status added later would then go unreported, which is the exact
// silence this report exists to end. Tracked as CHAOS-4107 rather than folded
// here: a migration is a different blast radius from a read.
const selectRunawayDispatchWakeupsSQL = `
SELECT outbox.sync_run_id::text, outbox.attempts
FROM public.sync_dispatch_outbox AS outbox
JOIN public.sync_runs AS run ON run.id = outbox.sync_run_id
WHERE outbox.kind = 'dispatch_sync_run'
	AND outbox.attempts >= $1
	AND run.status NOT IN ('success', 'partial_failed', 'failed')
ORDER BY outbox.attempts DESC, outbox.sync_run_id
LIMIT $2
`

func readRunawayDispatchWakeups(
	ctx context.Context, tx pgx.Tx,
) ([]RunawayDispatchWakeup, bool, error) {
	rows, err := tx.Query(
		ctx, selectRunawayDispatchWakeupsSQL,
		int64(runawayDispatchAttempts), runawayDispatchScan+1,
	)
	if err != nil {
		return nil, false, ErrUnavailable
	}
	defer rows.Close()
	report := make([]RunawayDispatchWakeup, 0, runawayDispatchScan)
	truncated := false
	for rows.Next() {
		var wakeup RunawayDispatchWakeup
		if err := rows.Scan(&wakeup.SyncRunID, &wakeup.Attempts); err != nil {
			return nil, false, ErrUnavailable
		}
		// The window asks for one more row than it reports, so "there are
		// more" is observed rather than inferred from a full page.
		if len(report) == runawayDispatchScan {
			truncated = true
			continue
		}
		report = append(report, wakeup)
	}
	if err := rows.Err(); err != nil {
		return nil, false, ErrUnavailable
	}
	if len(report) == 0 {
		return nil, false, nil
	}
	return report, truncated, nil
}

// materializeDispatchSQL mirrors _dispatchable_run_ids followed by
// _materialize_outbox_wakeups and its canonical upsert transition. The
// DISTINCT candidate set is bounded before insertion, and the unique outbox
// key arbitrates concurrent Go/Python writers. For schedule-triggered runs,
// scheduled_sync_occurrences.sync_run_id is the readiness fence: the table's
// completed-state constraint permits that link only in the same coordinator
// transaction that links job_run_id and marks the occurrence completed.
const materializeDispatchSQL = `
WITH candidates AS (
	SELECT DISTINCT run.id, run.org_id
	FROM public.sync_runs AS run
	JOIN public.sync_run_units AS unit ON unit.sync_run_id = run.id
	WHERE run.status NOT IN ('success', 'partial_failed', 'failed')
		AND (
			run.triggered_by <> 'schedule'
			OR EXISTS (
				SELECT 1
				FROM public.scheduled_sync_occurrences AS occurrence
				WHERE occurrence.sync_run_id = run.id
			)
		)
		AND (
			unit.status = 'planned'
			OR (unit.status = 'dispatching' AND unit.updated_at <= $2)
			OR (
				unit.status = 'retrying'
				AND unit.available_at IS NOT NULL
				AND unit.available_at <= $1
			)
		)
	ORDER BY run.id
	LIMIT $3
)
INSERT INTO public.sync_dispatch_outbox (
	id, org_id, sync_run_id, kind, status, available_at, attempts,
	created_at, updated_at
)
SELECT gen_random_uuid(), candidates.org_id, candidates.id,
	'dispatch_sync_run', 'pending', $1, 0, $1, $1
FROM candidates
ON CONFLICT (sync_run_id, kind) DO UPDATE
SET available_at = CASE
		WHEN sync_dispatch_outbox.status = 'dispatched'
			AND sync_dispatch_outbox.last_error = 'feature_disabled'
			THEN sync_dispatch_outbox.available_at
		WHEN EXCLUDED.available_at < sync_dispatch_outbox.available_at
			THEN EXCLUDED.available_at
		ELSE sync_dispatch_outbox.available_at
	END,
	status = CASE
		WHEN sync_dispatch_outbox.status = 'dispatched'
			AND sync_dispatch_outbox.last_error = 'feature_disabled'
			THEN 'dispatched'
		ELSE 'pending'
	END,
	dispatched_at = CASE
		WHEN sync_dispatch_outbox.status = 'dispatched'
			AND sync_dispatch_outbox.last_error = 'feature_disabled'
			THEN sync_dispatch_outbox.dispatched_at
		ELSE NULL
	END,
	dispatched_transport = CASE
		WHEN sync_dispatch_outbox.status = 'dispatched'
			AND sync_dispatch_outbox.last_error = 'feature_disabled'
			THEN sync_dispatch_outbox.dispatched_transport
		ELSE NULL
	END,
	dispatched_route_generation = CASE
		WHEN sync_dispatch_outbox.status = 'dispatched'
			AND sync_dispatch_outbox.last_error = 'feature_disabled'
			THEN sync_dispatch_outbox.dispatched_route_generation
		ELSE NULL
	END,
	transport_job_id = CASE
		WHEN sync_dispatch_outbox.status = 'dispatched'
			AND sync_dispatch_outbox.last_error = 'feature_disabled'
			THEN sync_dispatch_outbox.transport_job_id
		ELSE NULL
	END,
	claim_token = CASE
		WHEN NOT (
			sync_dispatch_outbox.status = 'dispatched'
			AND sync_dispatch_outbox.last_error = 'feature_disabled'
		)
			AND sync_dispatch_outbox.claim_expires_at IS NOT NULL
			AND sync_dispatch_outbox.claim_expires_at > $1
			THEN sync_dispatch_outbox.claim_token
		ELSE NULL
	END,
	claim_expires_at = CASE
		WHEN NOT (
			sync_dispatch_outbox.status = 'dispatched'
			AND sync_dispatch_outbox.last_error = 'feature_disabled'
		)
			AND sync_dispatch_outbox.claim_expires_at IS NOT NULL
			AND sync_dispatch_outbox.claim_expires_at > $1
			THEN sync_dispatch_outbox.claim_expires_at
		ELSE NULL
	END,
	claim_transport = CASE
		WHEN NOT (
			sync_dispatch_outbox.status = 'dispatched'
			AND sync_dispatch_outbox.last_error = 'feature_disabled'
		)
			AND sync_dispatch_outbox.claim_expires_at IS NOT NULL
			AND sync_dispatch_outbox.claim_expires_at > $1
			THEN sync_dispatch_outbox.claim_transport
		ELSE NULL
	END,
	claim_route_generation = CASE
		WHEN NOT (
			sync_dispatch_outbox.status = 'dispatched'
			AND sync_dispatch_outbox.last_error = 'feature_disabled'
		)
			AND sync_dispatch_outbox.claim_expires_at IS NOT NULL
			AND sync_dispatch_outbox.claim_expires_at > $1
			THEN sync_dispatch_outbox.claim_route_generation
		ELSE NULL
	END,
	updated_at = $1
WHERE sync_dispatch_outbox.status <> 'pending'
	AND sync_dispatch_outbox.last_error IS DISTINCT FROM 'feature_disabled'
	AND (
		sync_dispatch_outbox.dispatched_transport IS DISTINCT FROM 'river'
		OR EXISTS (
			SELECT 1
			FROM public.sync_run_units AS unit
			WHERE unit.sync_run_id = sync_dispatch_outbox.sync_run_id
				AND (
					(unit.status = 'dispatching' AND unit.updated_at <= $2)
					OR (
						unit.status = 'retrying'
						AND unit.available_at IS NOT NULL
						AND unit.available_at <= $1
					)
				)
		)
	)
`

const materializeFinalizeSQL = `
WITH candidates AS (
	SELECT run.id, run.org_id
	FROM public.sync_runs AS run
	WHERE run.status NOT IN ('success', 'partial_failed', 'failed')
		AND (
			run.triggered_by <> 'schedule'
			OR EXISTS (
				SELECT 1
				FROM public.scheduled_sync_occurrences AS occurrence
				WHERE occurrence.sync_run_id = run.id
			)
		)
		AND NOT EXISTS (
			SELECT 1
			FROM public.sync_run_units AS unit
			WHERE unit.sync_run_id = run.id
				AND unit.status NOT IN ('success', 'failed')
		)
		AND NOT EXISTS (
			SELECT 1
			FROM public.sync_run_reference_discoveries AS discovery
			WHERE discovery.sync_run_id = run.id
				AND discovery.status IN ('planned', 'retrying', 'running')
		)
	ORDER BY run.created_at, run.id
	LIMIT $2
)
INSERT INTO public.sync_dispatch_outbox (
	id, org_id, sync_run_id, kind, status, available_at, attempts,
	created_at, updated_at
)
SELECT gen_random_uuid(), candidates.org_id, candidates.id,
	'finalize_sync_run', 'pending', $1, 0, $1, $1
FROM candidates
ON CONFLICT (sync_run_id, kind) DO UPDATE
SET available_at = CASE
		WHEN sync_dispatch_outbox.status = 'dispatched'
			AND sync_dispatch_outbox.last_error = 'feature_disabled'
			THEN sync_dispatch_outbox.available_at
		WHEN EXCLUDED.available_at < sync_dispatch_outbox.available_at
			THEN EXCLUDED.available_at
		ELSE sync_dispatch_outbox.available_at
	END,
	status = CASE
		WHEN sync_dispatch_outbox.status = 'dispatched'
			AND sync_dispatch_outbox.last_error = 'feature_disabled'
			THEN 'dispatched'
		ELSE 'pending'
	END,
	dispatched_at = CASE
		WHEN sync_dispatch_outbox.status = 'dispatched'
			AND sync_dispatch_outbox.last_error = 'feature_disabled'
			THEN sync_dispatch_outbox.dispatched_at
		ELSE NULL
	END,
	dispatched_transport = CASE
		WHEN sync_dispatch_outbox.status = 'dispatched'
			AND sync_dispatch_outbox.last_error = 'feature_disabled'
			THEN sync_dispatch_outbox.dispatched_transport
		ELSE NULL
	END,
	dispatched_route_generation = CASE
		WHEN sync_dispatch_outbox.status = 'dispatched'
			AND sync_dispatch_outbox.last_error = 'feature_disabled'
			THEN sync_dispatch_outbox.dispatched_route_generation
		ELSE NULL
	END,
	transport_job_id = CASE
		WHEN sync_dispatch_outbox.status = 'dispatched'
			AND sync_dispatch_outbox.last_error = 'feature_disabled'
			THEN sync_dispatch_outbox.transport_job_id
		ELSE NULL
	END,
	claim_token = CASE
		WHEN NOT (
			sync_dispatch_outbox.status = 'dispatched'
			AND sync_dispatch_outbox.last_error = 'feature_disabled'
		)
			AND sync_dispatch_outbox.claim_expires_at IS NOT NULL
			AND sync_dispatch_outbox.claim_expires_at > $1
			THEN sync_dispatch_outbox.claim_token
		ELSE NULL
	END,
	claim_expires_at = CASE
		WHEN NOT (
			sync_dispatch_outbox.status = 'dispatched'
			AND sync_dispatch_outbox.last_error = 'feature_disabled'
		)
			AND sync_dispatch_outbox.claim_expires_at IS NOT NULL
			AND sync_dispatch_outbox.claim_expires_at > $1
			THEN sync_dispatch_outbox.claim_expires_at
		ELSE NULL
	END,
	claim_transport = CASE
		WHEN NOT (
			sync_dispatch_outbox.status = 'dispatched'
			AND sync_dispatch_outbox.last_error = 'feature_disabled'
		)
			AND sync_dispatch_outbox.claim_expires_at IS NOT NULL
			AND sync_dispatch_outbox.claim_expires_at > $1
			THEN sync_dispatch_outbox.claim_transport
		ELSE NULL
	END,
	claim_route_generation = CASE
		WHEN NOT (
			sync_dispatch_outbox.status = 'dispatched'
			AND sync_dispatch_outbox.last_error = 'feature_disabled'
		)
			AND sync_dispatch_outbox.claim_expires_at IS NOT NULL
			AND sync_dispatch_outbox.claim_expires_at > $1
			THEN sync_dispatch_outbox.claim_route_generation
		ELSE NULL
	END,
	updated_at = $1
WHERE sync_dispatch_outbox.status <> 'pending'
	AND NOT (
		sync_dispatch_outbox.status = 'dispatched'
		AND sync_dispatch_outbox.dispatched_transport = 'river'
	)
`

const materializeDiscoverySQL = `
WITH candidates AS (
	SELECT discovery.sync_run_id AS id, run.org_id
	FROM public.sync_run_reference_discoveries AS discovery
	JOIN public.sync_runs AS run ON run.id = discovery.sync_run_id
	WHERE run.status NOT IN ('success', 'partial_failed', 'failed')
		AND (
			(
				discovery.status IN ('planned', 'retrying')
				AND discovery.available_at <= $1
			)
			OR (
				discovery.status = 'running'
				AND discovery.lease_expires_at IS NOT NULL
				AND discovery.lease_expires_at <= $1
			)
		)
	ORDER BY discovery.available_at, discovery.sync_run_id
	LIMIT $2
)
INSERT INTO public.sync_dispatch_outbox (
	id, org_id, sync_run_id, kind, status, available_at, attempts,
	created_at, updated_at
)
SELECT gen_random_uuid(), candidates.org_id, candidates.id,
	'reference_discovery', 'pending', $1, 0, $1, $1
FROM candidates
ON CONFLICT (sync_run_id, kind) DO UPDATE
SET available_at = CASE
		WHEN sync_dispatch_outbox.status = 'dispatched'
			AND sync_dispatch_outbox.last_error = 'feature_disabled'
			THEN sync_dispatch_outbox.available_at
		WHEN EXCLUDED.available_at < sync_dispatch_outbox.available_at
			THEN EXCLUDED.available_at
		ELSE sync_dispatch_outbox.available_at
	END,
	status = CASE
		WHEN sync_dispatch_outbox.status = 'dispatched'
			AND sync_dispatch_outbox.last_error = 'feature_disabled'
			THEN 'dispatched'
		ELSE 'pending'
	END,
	dispatched_at = CASE
		WHEN sync_dispatch_outbox.status = 'dispatched'
			AND sync_dispatch_outbox.last_error = 'feature_disabled'
			THEN sync_dispatch_outbox.dispatched_at
		ELSE NULL
	END,
	dispatched_transport = CASE
		WHEN sync_dispatch_outbox.status = 'dispatched'
			AND sync_dispatch_outbox.last_error = 'feature_disabled'
			THEN sync_dispatch_outbox.dispatched_transport
		ELSE NULL
	END,
	dispatched_route_generation = CASE
		WHEN sync_dispatch_outbox.status = 'dispatched'
			AND sync_dispatch_outbox.last_error = 'feature_disabled'
			THEN sync_dispatch_outbox.dispatched_route_generation
		ELSE NULL
	END,
	transport_job_id = CASE
		WHEN sync_dispatch_outbox.status = 'dispatched'
			AND sync_dispatch_outbox.last_error = 'feature_disabled'
			THEN sync_dispatch_outbox.transport_job_id
		ELSE NULL
	END,
	claim_token = CASE
		WHEN NOT (
			sync_dispatch_outbox.status = 'dispatched'
			AND sync_dispatch_outbox.last_error = 'feature_disabled'
		)
			AND sync_dispatch_outbox.claim_expires_at IS NOT NULL
			AND sync_dispatch_outbox.claim_expires_at > $1
			THEN sync_dispatch_outbox.claim_token
		ELSE NULL
	END,
	claim_expires_at = CASE
		WHEN NOT (
			sync_dispatch_outbox.status = 'dispatched'
			AND sync_dispatch_outbox.last_error = 'feature_disabled'
		)
			AND sync_dispatch_outbox.claim_expires_at IS NOT NULL
			AND sync_dispatch_outbox.claim_expires_at > $1
			THEN sync_dispatch_outbox.claim_expires_at
		ELSE NULL
	END,
	claim_transport = CASE
		WHEN NOT (
			sync_dispatch_outbox.status = 'dispatched'
			AND sync_dispatch_outbox.last_error = 'feature_disabled'
		)
			AND sync_dispatch_outbox.claim_expires_at IS NOT NULL
			AND sync_dispatch_outbox.claim_expires_at > $1
			THEN sync_dispatch_outbox.claim_transport
		ELSE NULL
	END,
	claim_route_generation = CASE
		WHEN NOT (
			sync_dispatch_outbox.status = 'dispatched'
			AND sync_dispatch_outbox.last_error = 'feature_disabled'
		)
			AND sync_dispatch_outbox.claim_expires_at IS NOT NULL
			AND sync_dispatch_outbox.claim_expires_at > $1
			THEN sync_dispatch_outbox.claim_route_generation
		ELSE NULL
	END,
	updated_at = $1
WHERE sync_dispatch_outbox.status <> 'pending'
	AND NOT (
		sync_dispatch_outbox.status = 'dispatched'
		AND sync_dispatch_outbox.dispatched_transport = 'river'
	)
`

// post_sync is reconstructed only when the once-only finalizer ledger exists and the
// outbox row does not. An existing row in any state is immutable here.
const materializePostSyncSQL = `
WITH candidates AS (
	SELECT ledger.sync_run_id AS id, run.org_id
	FROM public.sync_run_post_dispatches AS ledger
	JOIN public.sync_runs AS run ON run.id = ledger.sync_run_id
	LEFT JOIN public.sync_dispatch_outbox AS outbox
		ON outbox.sync_run_id = ledger.sync_run_id
		AND outbox.kind = 'post_sync'
	WHERE ledger.kind = 'post_sync'
		AND outbox.id IS NULL
	ORDER BY ledger.dispatched_at, ledger.sync_run_id
	LIMIT $2
)
INSERT INTO public.sync_dispatch_outbox (
	id, org_id, sync_run_id, kind, status, available_at, attempts,
	created_at, updated_at
)
SELECT gen_random_uuid(), candidates.org_id, candidates.id,
	'post_sync', 'pending', $1, 0, $1, $1
FROM candidates
ON CONFLICT (sync_run_id, kind) DO NOTHING
`
