package jobroute

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type PostgresRiverQuiescer struct {
	pool  *pgxpool.Pool
	table string
}

// PostgresCelerySyncProviderQuiescer proves that the legacy Celery-owned
// sync.provider_unit workload has drained before its checked-in route can move
// to River. SyncRunUnit is the durable source of truth for those unit tasks,
// but a dispatching or running row is only trusted as evidence of live work
// when it also looks live: see Quiesce for the liveness test.
//
// This is deliberately narrower than a generic Celery probe. No other job
// kind has the same durable unit ledger, so accepting it here would turn an
// absence of evidence into activation authority.
type PostgresCelerySyncProviderQuiescer struct {
	pool *pgxpool.Pool
}

const syncProviderUnitKind = "sync.provider_unit"

const celerySyncProviderProbeTimeout = 5 * time.Second

// celerySyncProviderDispatchStaleThreshold mirrors the dispatch-layer guard's
// SYNC_UNIT_DISPATCH_STALE_SECONDS default (see sync/guard.py and
// sync/budget_guard.py._stale_dispatch_cutoff): a DISPATCHING row younger than
// this is still a fresh Celery claim; older than this and no worker ever
// picked it up, so it cannot be evidence Celery is still working the kind.
// CHAOS-3929: a Celery-replicas-0 window produces exactly this shape --
// dispatching rows with zero lease owner, zero attempts, null heartbeat --
// and treating any such row as live deadlocks the route move that would
// drain it.
const celerySyncProviderDispatchStaleThreshold = 15 * time.Minute

func NewPostgresCelerySyncProviderQuiescer(pool *pgxpool.Pool) (*PostgresCelerySyncProviderQuiescer, error) {
	if pool == nil {
		return nil, ErrInvalidConfiguration
	}
	return &PostgresCelerySyncProviderQuiescer{pool: pool}, nil
}

func NewPostgresRiverQuiescer(pool *pgxpool.Pool, schema string) (*PostgresRiverQuiescer, error) {
	if pool == nil || len(schema) == 0 || len(schema) > 63 {
		return nil, ErrInvalidConfiguration
	}
	table := pgx.Identifier{schema, "river_job"}.Sanitize()
	if table == "" {
		return nil, ErrInvalidConfiguration
	}
	return &PostgresRiverQuiescer{pool: pool, table: table}, nil
}

func (quiescer *PostgresRiverQuiescer) Quiesce(ctx context.Context, kind string) error {
	if quiescer == nil || quiescer.pool == nil || kind == "" {
		return ErrInvalidConfiguration
	}
	var count int64
	err := quiescer.pool.QueryRow(ctx, `
		SELECT count(*) FROM `+quiescer.table+`
		WHERE kind = $1 AND state IN ('available', 'pending', 'retryable', 'running', 'scheduled')`,
		kind,
	).Scan(&count)
	if err != nil {
		return fmt.Errorf("%w: %w", ErrUnavailable, err)
	}
	if count != 0 {
		return ErrLiveClaims
	}
	return nil
}

var _ Quiescer = (*PostgresRiverQuiescer)(nil)

// Quiesce rejects a canary cutover while legacy Celery can still own a unit in
// the checked-in LaunchDarkly feature-flags canary scope. DISPATCHING and
// RUNNING are the only states that can represent a Celery message or active
// task; planned and retrying work remains eligible for the post-cutover
// producer route decision. A bounded child context prevents an operator route
// lock from being held indefinitely when the semantic database is unhealthy.
//
// A row's status alone is not proof of life (CHAOS-3929): a producer can run
// ahead of a drained or never-started Celery consumer (e.g. workers scaled to
// zero mid-cutover) and leave DISPATCHING rows no one will ever claim. So each
// status is checked against the same liveness signal the dispatch-layer guard
// already uses for capacity accounting:
//   - DISPATCHING is live only while fresh: updated_at within
//     celerySyncProviderDispatchStaleThreshold of now. A row this old with no
//     claim has definitionally been orphaned, not just slow.
//   - RUNNING is live unless its lease has explicitly expired. A NULL lease is
//     unknown/pre-migration and stays live; only an explicit expiry proves the
//     worker is gone (mirrors sync/guard.py's capacity-consumer set).
func (quiescer *PostgresCelerySyncProviderQuiescer) Quiesce(ctx context.Context, kind string) error {
	if quiescer == nil || quiescer.pool == nil || kind != syncProviderUnitKind {
		return ErrInvalidConfiguration
	}
	probeCtx, cancel := context.WithTimeout(ctx, celerySyncProviderProbeTimeout)
	defer cancel()
	dispatchStaleCutoff := time.Now().UTC().Add(-celerySyncProviderDispatchStaleThreshold)
	var active bool
	err := quiescer.pool.QueryRow(probeCtx, `
		SELECT EXISTS (
			SELECT 1 FROM public.sync_run_units
			WHERE provider = 'launchdarkly'
			  AND dataset_key = 'feature-flags'
			  AND status IN ('dispatching', 'running')
			  AND (
				(status = 'dispatching' AND updated_at > $1)
				OR (status = 'running' AND (lease_expires_at IS NULL OR lease_expires_at > now()))
			  )
		)`, dispatchStaleCutoff).Scan(&active)
	if err != nil {
		return fmt.Errorf("%w: %w", ErrUnavailable, err)
	}
	if active {
		return ErrLiveClaims
	}
	return nil
}

var _ Quiescer = (*PostgresCelerySyncProviderQuiescer)(nil)

func IsPrecondition(err error) bool {
	return errors.Is(err, ErrDrift) || errors.Is(err, ErrPaused) ||
		errors.Is(err, ErrLiveClaims) || errors.Is(err, ErrPendingOutbox) ||
		errors.Is(err, ErrCeleryQuiescenceMissing) || errors.Is(err, ErrUnknownRoute)
}
