package jobroute

import (
	"context"
	"errors"
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
// to River. SyncRunUnit is the durable source of truth for those unit tasks:
// a dispatching or running row in the approved canary scope still represents
// an active or already-published Celery task.
//
// This is deliberately narrower than a generic Celery probe. No other job
// kind has the same durable unit ledger, so accepting it here would turn an
// absence of evidence into activation authority.
type PostgresCelerySyncProviderQuiescer struct {
	pool *pgxpool.Pool
}

const syncProviderUnitKind = "sync.provider_unit"

const celerySyncProviderProbeTimeout = 5 * time.Second

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
		return ErrUnavailable
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
func (quiescer *PostgresCelerySyncProviderQuiescer) Quiesce(ctx context.Context, kind string) error {
	if quiescer == nil || quiescer.pool == nil || kind != syncProviderUnitKind {
		return ErrInvalidConfiguration
	}
	probeCtx, cancel := context.WithTimeout(ctx, celerySyncProviderProbeTimeout)
	defer cancel()
	var active bool
	err := quiescer.pool.QueryRow(probeCtx, `
		SELECT EXISTS (
			SELECT 1 FROM public.sync_run_units
			WHERE provider = 'launchdarkly'
			  AND dataset_key = 'feature-flags'
			  AND status IN ('dispatching', 'running')
		)`).Scan(&active)
	if err != nil {
		return ErrUnavailable
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
