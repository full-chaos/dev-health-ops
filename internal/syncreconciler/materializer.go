package syncreconciler

import (
	"context"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// MaterializerResult reports rows inserted or re-armed by one bounded
// transaction. Existing pending rows are deliberately not counted because the
// Python reconciler's materialization wrapper leaves them untouched.
type MaterializerResult struct {
	Dispatch  int64
	Finalize  int64
	Discovery int64
	PostSync  int64
}

type materializerBeginFunc func(context.Context) (pgx.Tx, error)

// Materializer reconstructs missing sync-dispatch wakeups from authoritative
// domain state. It is transport-neutral: it neither reads nor mutates transport
// routes, and it never claims or publishes an outbox row.
//
// The component is intentionally command-unwired. Its transaction can coexist
// with the Python reconciler because the outbox has a unique (sync_run_id, kind)
// constraint. The first three conflict transitions match the Python wrapper;
// post_sync is deliberately stricter and never re-arms an existing row.
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
// transaction. staleDispatchCutoff is supplied by the future command owner so
// this dormant domain component does not duplicate environment policy.
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
	if err := tx.Commit(ctx); err != nil {
		return MaterializerResult{}, ErrUnavailable
	}
	return result, nil
}

// materializeDispatchSQL mirrors _dispatchable_run_ids followed by
// _materialize_outbox_wakeups and its canonical upsert transition. The
// DISTINCT candidate set is bounded before insertion, and the unique outbox
// key arbitrates concurrent Go/Python writers.
const materializeDispatchSQL = `
WITH candidates AS (
	SELECT DISTINCT run.id, run.org_id
	FROM public.sync_runs AS run
	JOIN public.sync_run_units AS unit ON unit.sync_run_id = run.id
	WHERE run.status NOT IN ('success', 'partial_failed', 'failed')
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
`

const materializeFinalizeSQL = `
WITH candidates AS (
	SELECT run.id, run.org_id
	FROM public.sync_runs AS run
	WHERE run.status NOT IN ('success', 'partial_failed', 'failed')
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
`

// post_sync is reconstructed only when the at-most-once ledger exists and the
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
