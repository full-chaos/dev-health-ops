package synccoverage

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

var (
	ErrPoolUnavailable = errors.New("sync coverage PostgreSQL pool is unavailable")
	ErrConfigNotFound  = errors.New("sync coverage configuration was not found")
)

type Projector struct {
	pool  *pgxpool.Pool
	clock func() time.Time
}

func NewProjector(pool *pgxpool.Pool, options ...Option) (*Projector, error) {
	if pool == nil {
		return nil, ErrPoolUnavailable
	}
	projector := &Projector{pool: pool, clock: func() time.Time { return time.Now().UTC() }}
	for _, option := range options {
		if option != nil {
			option(projector)
		}
	}
	return projector, nil
}

// Rebuild replaces one tenant-scoped projection in the same transaction that
// reads its source facts. An existing projection remains unchanged on any
// query, encoding, or upsert failure.
func (projector *Projector) Rebuild(
	ctx context.Context,
	orgID string,
	configID uuid.UUID,
) (json.RawMessage, error) {
	if ctx == nil {
		return nil, fmt.Errorf("sync coverage rebuild context is required")
	}
	if orgID == "" || configID == uuid.Nil {
		return nil, fmt.Errorf("sync coverage rebuild requires org and config identifiers")
	}
	tx, err := projector.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return nil, fmt.Errorf("begin sync coverage rebuild: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	lockName := fmt.Sprintf("sync-coverage:%s:%s", orgID, configID)
	if _, err := tx.Exec(ctx, "SELECT pg_advisory_xact_lock(hashtextextended($1, 0))", lockName); err != nil {
		return nil, fmt.Errorf("lock sync coverage projection: %w", err)
	}
	config, err := loadConfig(ctx, tx, orgID, configID)
	if err != nil {
		return nil, err
	}
	now := projector.clock().UTC()
	payload, sourceUpdatedAt, backfillUpdatedAt, err := buildProjection(ctx, tx, config, now)
	if err != nil {
		return nil, err
	}
	encoded, err := marshalPayload(payload)
	if err != nil {
		return nil, fmt.Errorf("encode sync coverage projection: %w", err)
	}
	if _, err := tx.Exec(ctx, `
INSERT INTO public.sync_coverage_projections (
    id, org_id, sync_config_id, history_lookback_days, projection_version,
    generated_at, source_updated_at, backfill_updated_at, invalidated_at,
    payload, created_at, updated_at
) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, NULL, $9::json, now(), now())
ON CONFLICT (org_id, sync_config_id, history_lookback_days) DO UPDATE SET
    projection_version = EXCLUDED.projection_version,
    generated_at = EXCLUDED.generated_at,
    source_updated_at = EXCLUDED.source_updated_at,
    backfill_updated_at = EXCLUDED.backfill_updated_at,
    invalidated_at = NULL,
    payload = EXCLUDED.payload,
    updated_at = now()`, uuid.New(), orgID, configID, HistoryLookbackDays,
		projectionVersion, now, sourceUpdatedAt, backfillUpdatedAt, encoded); err != nil {
		return nil, fmt.Errorf("upsert sync coverage projection: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("commit sync coverage projection: %w", err)
	}
	return encoded, nil
}

// RefreshDue follows the former Celery task's bounded priority order:
// invalidated rows, missing rows, then the oldest warm rows. One broken config
// does not stop the remaining bounded sweep.
func (projector *Projector) RefreshDue(ctx context.Context, limit int) (RefreshResult, error) {
	if ctx == nil {
		return RefreshResult{}, fmt.Errorf("sync coverage refresh context is required")
	}
	if limit < 0 {
		return RefreshResult{}, fmt.Errorf("sync coverage refresh limit must not be negative")
	}
	if limit == 0 {
		return RefreshResult{}, nil
	}
	rows, err := projector.pool.Query(ctx, `
SELECT config.org_id, config.id
FROM public.sync_configurations AS config
LEFT JOIN public.sync_coverage_projections AS projection
  ON projection.org_id = config.org_id
 AND projection.sync_config_id = config.id
 AND projection.history_lookback_days = $1
ORDER BY CASE
           WHEN projection.invalidated_at IS NOT NULL THEN 0
           WHEN projection.id IS NULL THEN 1
           ELSE 2
         END,
         projection.updated_at ASC NULLS FIRST,
         config.id
LIMIT $2`, HistoryLookbackDays, limit)
	if err != nil {
		return RefreshResult{}, fmt.Errorf("select due sync coverage projections: %w", err)
	}
	type configKey struct {
		OrgID string
		ID    uuid.UUID
	}
	keys := make([]configKey, 0, limit)
	for rows.Next() {
		var key configKey
		if err := rows.Scan(&key.OrgID, &key.ID); err != nil {
			rows.Close()
			return RefreshResult{}, fmt.Errorf("scan due sync coverage projection: %w", err)
		}
		keys = append(keys, key)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return RefreshResult{}, fmt.Errorf("iterate due sync coverage projections: %w", err)
	}
	rows.Close()

	result := RefreshResult{}
	for _, key := range keys {
		if _, err := projector.Rebuild(ctx, key.OrgID, key.ID); err != nil {
			result.Failed++
			result.Failures = append(result.Failures, RefreshFailure{OrgID: key.OrgID, ConfigID: key.ID, Err: err})
			continue
		}
		result.Refreshed++
	}
	return result, nil
}
