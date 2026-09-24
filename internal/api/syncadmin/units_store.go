package syncadmin

import (
	"context"
	"time"

	"github.com/google/uuid"

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

// runUnits is SyncRunService.list_units after the id parsed: the org's
// units of the run, ordered by id.
func (s store) runUnits(ctx context.Context, orgID string, runID uuid.UUID) ([]runUnit, error) {
	rows, err := s.pool.Query(ctx, `
SELECT u.id, u.sync_run_id, u.integration_id, u.source_id, u.org_id, u.provider, u.dataset_key, u.cost_class,
       u.mode, u.status, u.since_at, u.before_at, u.available_at, u.attempts, u.rate_limit_deferrals,
       u.budget_deferrals, u.duration_seconds, u.error, u.last_heartbeat_at, u.result::text,
       u.processor_flags::text, u.created_at, u.updated_at,
       s.id IS NOT NULL, s.name, s.full_name, s.external_id
FROM sync_run_units AS u
LEFT JOIN integration_sources AS s ON s.id = u.source_id
WHERE u.sync_run_id = $1 AND u.org_id = $2
ORDER BY u.id`, runID, orgID)
	if err != nil {
		return nil, err
	}
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
