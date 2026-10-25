package syncadmin

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	schedsync "github.com/full-chaos/dev-health-ops/internal/scheduler/sync"
)

// runUnit is one sync_run_units row with its source row (the
// selectinload(SyncRunUnit.source) relationship), as list_units reads it.
type runUnit struct {
	ID, SyncRunID, IntegrationID, SourceID uuid.UUID
	OrgID, Provider, DatasetKey            string
	CostClass, Mode, Status                string
	SinceAt, BeforeAt, AvailableAt         *time.Time
	Attempts, RateLimitDeferrals           int64
	BudgetDeferrals                        int64
	DurationSeconds                        *int64
	Error                                  *string
	LastHeartbeatAt                        *time.Time
	Result, ProcessorFlags                 *string
	CreatedAt, UpdatedAt                   time.Time
	// HasSource is false when no integration_sources row has the unit's
	// source_id (unit.source is None).
	HasSource                                    bool
	SourceName, SourceFullName, SourceExternalID *string
}

// unitFilter narrows the units of one run. The zero value keeps every unit.
type unitFilter struct {
	Statuses   []string
	DatasetKey *string
	SourceID   *uuid.UUID
	// ErrorCategory keeps units whose stored result carries that error_category.
	ErrorCategory *string
	// Since keeps units that do not end before it; Before keeps units that
	// do not start after it (a unit with no bound on that side is kept).
	Since, Before *time.Time
}

// runUnitScope is the org scope of every units read: the run and the org,
// the same two predicates the whole-run read has always carried. The filter
// only adds AND terms after them.
func (f unitFilter) where(orgID string, runID uuid.UUID) (string, []any) {
	args := []any{runID, orgID}
	clause := "u.sync_run_id = $1 AND u.org_id = $2"
	add := func(sql string, value any) {
		args = append(args, value)
		clause += " AND " + fmt.Sprintf(sql, len(args))
	}
	if len(f.Statuses) > 0 {
		add("u.status = ANY($%d)", f.Statuses)
	}
	if f.DatasetKey != nil {
		add("u.dataset_key = $%d", *f.DatasetKey)
	}
	if f.SourceID != nil {
		add("u.source_id = $%d", *f.SourceID)
	}
	if f.ErrorCategory != nil {
		add("u.result::jsonb ->> 'error_category' = $%d", *f.ErrorCategory)
	}
	if f.Since != nil {
		add("(u.before_at IS NULL OR u.before_at >= $%d)", *f.Since)
	}
	if f.Before != nil {
		add("(u.since_at IS NULL OR u.since_at <= $%d)", *f.Before)
	}
	return clause, args
}

const runUnitFromJoin = `
FROM sync_run_units AS u
LEFT JOIN integration_sources AS s ON s.id = u.source_id`

// fullUnitColumns is every column of a unit the response renders.
const fullUnitColumns = `u.id, u.sync_run_id, u.integration_id, u.source_id, u.org_id, u.provider, u.dataset_key, u.cost_class,
       u.mode, u.status, u.since_at, u.before_at, u.available_at, u.attempts, u.rate_limit_deferrals,
       u.budget_deferrals, u.duration_seconds, u.error, u.last_heartbeat_at, u.result::text,
       u.processor_flags::text, u.created_at, u.updated_at,
       s.id IS NOT NULL, s.name, s.full_name, s.external_id`

// rollupUnitColumns is the same row without the large columns: error is
// NULL and result is only the two keys the rollups read (error_category,
// retry_exhausted), present only when the stored result is an object that
// has them, so an explicit null stays distinct from a missing key.
const rollupUnitColumns = `u.id, u.sync_run_id, u.integration_id, u.source_id, u.org_id, u.provider, u.dataset_key, u.cost_class,
       u.mode, u.status, u.since_at, u.before_at, u.available_at, u.attempts, u.rate_limit_deferrals,
       u.budget_deferrals, u.duration_seconds, NULL::text, u.last_heartbeat_at,
       CASE WHEN jsonb_typeof(u.result::jsonb) = 'object' THEN
         (CASE WHEN u.result::jsonb ? 'error_category' THEN jsonb_build_object('error_category', u.result::jsonb -> 'error_category') ELSE '{}'::jsonb END
          || CASE WHEN u.result::jsonb ? 'retry_exhausted' THEN jsonb_build_object('retry_exhausted', u.result::jsonb -> 'retry_exhausted') ELSE '{}'::jsonb END)::text
       END,
       u.processor_flags::text, u.created_at, u.updated_at,
       s.id IS NOT NULL, s.name, s.full_name, s.external_id`

func scanRunUnits(rows pgx.Rows) ([]runUnit, error) {
	defer rows.Close()
	var units []runUnit
	for rows.Next() {
		var unit runUnit
		if err := rows.Scan(&unit.ID, &unit.SyncRunID, &unit.IntegrationID, &unit.SourceID, &unit.OrgID,
			&unit.Provider, &unit.DatasetKey, &unit.CostClass, &unit.Mode, &unit.Status, &unit.SinceAt,
			&unit.BeforeAt, &unit.AvailableAt, &unit.Attempts, &unit.RateLimitDeferrals, &unit.BudgetDeferrals,
			&unit.DurationSeconds, &unit.Error, &unit.LastHeartbeatAt, &unit.Result, &unit.ProcessorFlags,
			&unit.CreatedAt, &unit.UpdatedAt, &unit.HasSource, &unit.SourceName, &unit.SourceFullName,
			&unit.SourceExternalID); err != nil {
			return nil, err
		}
		units = append(units, unit)
	}
	return units, rows.Err()
}

// querier is the read half of a pgxpool.Pool and of a pgx.Tx.
type querier interface {
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

// unitsSnapshotReads are the three reads of one units response: the rollup
// rows, the filtered count and the page.
type unitsSnapshotReads interface {
	runUnitRollupRows(ctx context.Context, orgID string, runID uuid.UUID) ([]runUnit, error)
	runUnitsPage(ctx context.Context, orgID string, runID uuid.UUID, filter unitFilter, offset int64, limit *int64) ([]runUnit, error)
	countRunUnits(ctx context.Context, orgID string, runID uuid.UUID, filter unitFilter) (int64, error)
}

// unitReads runs the units reads on one querier: the pool (each read its own
// statement) or a transaction (all reads one snapshot). Every query carries the
// org scope in its own WHERE (unitFilter.where); the connection sets no other
// per-session or per-transaction context.
type unitReads struct{ q querier }

// unitsSnapshot runs fn with the three reads on ONE read-only REPEATABLE READ
// transaction, so the rollups, the filtered count and the page describe the same
// rows however the run's units change meanwhile. A read-only transaction takes no
// row locks and cannot fail on serialisation; it is rolled back at the end.
func (s store) unitsSnapshot(ctx context.Context, fn func(unitsSnapshotReads) error) error {
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	return fn(unitReads{q: tx})
}

func (s store) runUnitRollupRows(ctx context.Context, orgID string, runID uuid.UUID) ([]runUnit, error) {
	return unitReads{q: s.pool}.runUnitRollupRows(ctx, orgID, runID)
}

func (s store) runUnitsPage(ctx context.Context, orgID string, runID uuid.UUID, filter unitFilter, offset int64, limit *int64) ([]runUnit, error) {
	return unitReads{q: s.pool}.runUnitsPage(ctx, orgID, runID, filter, offset, limit)
}

func (s store) countRunUnits(ctx context.Context, orgID string, runID uuid.UUID, filter unitFilter) (int64, error) {
	return unitReads{q: s.pool}.countRunUnits(ctx, orgID, runID, filter)
}

// runUnitRollupRows is SyncRunService.list_units for the rollups: every unit
// of the run in id order, without the large columns.
func (s unitReads) runUnitRollupRows(ctx context.Context, orgID string, runID uuid.UUID) ([]runUnit, error) {
	where, args := unitFilter{}.where(orgID, runID)
	rows, err := s.q.Query(ctx, "SELECT "+rollupUnitColumns+runUnitFromJoin+"\nWHERE "+where+"\nORDER BY u.id", args...)
	if err != nil {
		return nil, err
	}
	return scanRunUnits(rows)
}

// runUnitsPage reads the full rows of the filtered units in id order,
// skipping offset and stopping after limit (nil = no stop).
func (s unitReads) runUnitsPage(ctx context.Context, orgID string, runID uuid.UUID, filter unitFilter, offset int64, limit *int64) ([]runUnit, error) {
	where, args := filter.where(orgID, runID)
	query := "SELECT " + fullUnitColumns + runUnitFromJoin + "\nWHERE " + where + "\nORDER BY u.id"
	if limit != nil {
		args = append(args, *limit)
		query += fmt.Sprintf(" LIMIT $%d", len(args))
	}
	if offset > 0 {
		args = append(args, offset)
		query += fmt.Sprintf(" OFFSET $%d", len(args))
	}
	rows, err := s.q.Query(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	return scanRunUnits(rows)
}

// countRunUnits is the number of units the filter keeps.
func (s unitReads) countRunUnits(ctx context.Context, orgID string, runID uuid.UUID, filter unitFilter) (int64, error) {
	where, args := filter.where(orgID, runID)
	var count int64
	err := s.q.QueryRow(ctx, "SELECT count(*) FROM sync_run_units AS u WHERE "+where, args...).Scan(&count)
	return count, err
}

// watermarkRows is build_dataset_freshness's select: the org's watermark
// rows whose source_id or repo_id is one of the source keys and whose
// dataset_key or target is one of the lookup values.
func (s store) watermarkRows(ctx context.Context, orgID string, sourceKeys, lookupValues []string) ([]schedsync.WatermarkRow, error) {
	rows, err := s.pool.Query(ctx, `
SELECT source_id, dataset_key, repo_id, target, last_synced_at
FROM sync_watermarks
WHERE org_id = $1
  AND (source_id = ANY($2) OR repo_id = ANY($2))
  AND (dataset_key = ANY($3) OR target = ANY($3))`, orgID, sourceKeys, lookupValues)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []schedsync.WatermarkRow
	for rows.Next() {
		var row schedsync.WatermarkRow
		if err := rows.Scan(&row.SourceID, &row.DatasetKey, &row.RepoID, &row.Target, &row.LastSyncedAt); err != nil {
			return nil, err
		}
		out = append(out, row)
	}
	return out, rows.Err()
}
