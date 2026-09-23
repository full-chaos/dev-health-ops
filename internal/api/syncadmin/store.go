package syncadmin

import (
	"context"
	"errors"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// store reads the sync admin tables over the api role's pool. No query
// carries an ORDER BY the Python service does not carry: an unordered
// Python read is an unordered read here, so both planes see one database's
// scan order.
type store struct {
	pool *pgxpool.Pool
}

// syncConfig is one sync_configurations row as the response reads it. The
// JSON columns are their stored text.
type syncConfig struct {
	ID              uuid.UUID
	Name            string
	Provider        string
	SyncTargets     *string
	SyncOptions     *string
	IsActive        bool
	ParentID        *uuid.UUID
	IntegrationID   *uuid.UUID
	SourceID        *uuid.UUID
	LastSyncAt      *time.Time
	LastSyncSuccess *bool
	LastSyncError   *string
	CreatedAt       time.Time
	UpdatedAt       time.Time
}

const syncConfigColumns = `id, name, provider, sync_targets::text, sync_options::text, is_active,
parent_id, integration_id, source_id, last_sync_at, last_sync_success, last_sync_error,
created_at, updated_at`

func scanSyncConfig(row pgx.Row) (*syncConfig, error) {
	var config syncConfig
	err := row.Scan(&config.ID, &config.Name, &config.Provider, &config.SyncTargets, &config.SyncOptions,
		&config.IsActive, &config.ParentID, &config.IntegrationID, &config.SourceID, &config.LastSyncAt,
		&config.LastSyncSuccess, &config.LastSyncError, &config.CreatedAt, &config.UpdatedAt)
	if err != nil {
		return nil, err
	}
	return &config, nil
}

// listConfigs is SyncConfigurationService.list_all.
func (s store) listConfigs(ctx context.Context, orgID string, activeOnly bool) ([]*syncConfig, error) {
	query := `SELECT ` + syncConfigColumns + ` FROM sync_configurations WHERE org_id = $1`
	if activeOnly {
		query += ` AND is_active = true`
	}
	rows, err := s.pool.Query(ctx, query, orgID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var configs []*syncConfig
	for rows.Next() {
		config, err := scanSyncConfig(rows)
		if err != nil {
			return nil, err
		}
		configs = append(configs, config)
	}
	return configs, rows.Err()
}

// configByID is SyncConfigurationService.get_by_id after the id parsed.
func (s store) configByID(ctx context.Context, orgID string, id uuid.UUID) (*syncConfig, error) {
	config, err := scanSyncConfig(s.pool.QueryRow(ctx,
		`SELECT `+syncConfigColumns+` FROM sync_configurations WHERE org_id = $1 AND id = $2`, orgID, id))
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	return config, err
}

// childrenCounts is list_sync_configs's children count map. Like the
// Python query it is keyed by parent id only, never by org.
func (s store) childrenCounts(ctx context.Context, parentIDs []uuid.UUID) (map[uuid.UUID]int64, error) {
	counts := map[uuid.UUID]int64{}
	if len(parentIDs) == 0 {
		return counts, nil
	}
	rows, err := s.pool.Query(ctx,
		`SELECT parent_id, count(id) FROM sync_configurations WHERE parent_id = ANY($1) GROUP BY parent_id`, parentIDs)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var parentID uuid.UUID
		var count int64
		if err := rows.Scan(&parentID, &count); err != nil {
			return nil, err
		}
		counts[parentID] = count
	}
	return counts, rows.Err()
}

// credentialIDs is _integration_credential_ids_for_configs: integration id
// -> its credential_id (nil when NULL), for the org's own integrations only.
func (s store) credentialIDs(ctx context.Context, orgID string, integrationIDs []uuid.UUID) (map[uuid.UUID]*uuid.UUID, error) {
	found := map[uuid.UUID]*uuid.UUID{}
	if len(integrationIDs) == 0 {
		return found, nil
	}
	rows, err := s.pool.Query(ctx,
		`SELECT id, credential_id FROM integrations WHERE org_id = $1 AND id = ANY($2)`, orgID, integrationIDs)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var id uuid.UUID
		var credentialID *uuid.UUID
		if err := rows.Scan(&id, &credentialID); err != nil {
			return nil, err
		}
		found[id] = credentialID
	}
	return found, rows.Err()
}

// plannerSource is one integration_sources row read for the repository
// selection.
type plannerSource struct {
	FullName  string
	IsEnabled bool
	Metadata  *string
}

// sourcesForIntegration is _planner_sources_for_config's SELECT, before its
// planner_managed_sync_config_id filter.
func (s store) sourcesForIntegration(ctx context.Context, orgID string, integrationID uuid.UUID, provider string) ([]plannerSource, error) {
	rows, err := s.pool.Query(ctx,
		`SELECT full_name, is_enabled, metadata::text FROM integration_sources
WHERE org_id = $1 AND integration_id = $2 AND provider = $3`, orgID, integrationID, provider)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var sources []plannerSource
	for rows.Next() {
		var source plannerSource
		if err := rows.Scan(&source.FullName, &source.IsEnabled, &source.Metadata); err != nil {
			return nil, err
		}
		sources = append(sources, source)
	}
	return sources, rows.Err()
}

// childOptions is _legacy_child_repositories_for_config's SELECT: each
// child's stored sync_options text.
func (s store) childOptions(ctx context.Context, orgID string, parentID uuid.UUID) ([]*string, error) {
	rows, err := s.pool.Query(ctx,
		`SELECT sync_options::text FROM sync_configurations WHERE org_id = $1 AND parent_id = $2`, orgID, parentID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var options []*string
	for rows.Next() {
		var text *string
		if err := rows.Scan(&text); err != nil {
			return nil, err
		}
		options = append(options, text)
	}
	return options, rows.Err()
}

// scheduledSyncJobIDs is list_sync_config_jobs's scheduled_jobs SELECT.
func (s store) scheduledSyncJobIDs(ctx context.Context, orgID string, configID uuid.UUID) ([]uuid.UUID, error) {
	rows, err := s.pool.Query(ctx,
		`SELECT id FROM scheduled_jobs WHERE org_id = $1 AND sync_config_id = $2 AND job_type = 'sync'`, orgID, configID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var ids []uuid.UUID
	for rows.Next() {
		var id uuid.UUID
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}
	return ids, rows.Err()
}

// jobRun is one job_runs row.
type jobRun struct {
	ID              uuid.UUID
	JobID           uuid.UUID
	Status          int64
	StartedAt       *time.Time
	CompletedAt     *time.Time
	DurationSeconds *int64
	Result          *string
	Error           *string
	TriggeredBy     string
	CreatedAt       time.Time
}

// jobRuns is list_sync_config_jobs's job_runs page, newest first.
func (s store) jobRuns(ctx context.Context, jobIDs []uuid.UUID, limit, offset int64) ([]jobRun, error) {
	rows, err := s.pool.Query(ctx,
		`SELECT id, job_id, status, started_at, completed_at, duration_seconds, result::text, error, triggered_by, created_at
FROM job_runs WHERE job_id = ANY($1) ORDER BY created_at DESC LIMIT $2 OFFSET $3`, jobIDs, limit, offset)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var runs []jobRun
	for rows.Next() {
		var run jobRun
		if err := rows.Scan(&run.ID, &run.JobID, &run.Status, &run.StartedAt, &run.CompletedAt, &run.DurationSeconds,
			&run.Result, &run.Error, &run.TriggeredBy, &run.CreatedAt); err != nil {
			return nil, err
		}
		runs = append(runs, run)
	}
	return runs, rows.Err()
}

// syncRun is one sync_runs row.
type syncRun struct {
	ID             uuid.UUID
	OrgID          string
	IntegrationID  *uuid.UUID
	TriggeredBy    string
	Mode           string
	Status         string
	TotalUnits     int64
	CompletedUnits int64
	FailedUnits    int64
	StartedAt      *time.Time
	CompletedAt    *time.Time
	Result         *string
	Error          *string
	CreatedAt      time.Time
}

const syncRunColumns = `id, org_id, integration_id, triggered_by, mode, status, total_units, completed_units,
failed_units, started_at, completed_at, result::text, error, created_at`

func scanSyncRun(row pgx.Row) (*syncRun, error) {
	var run syncRun
	if err := row.Scan(&run.ID, &run.OrgID, &run.IntegrationID, &run.TriggeredBy, &run.Mode, &run.Status,
		&run.TotalUnits, &run.CompletedUnits, &run.FailedUnits, &run.StartedAt, &run.CompletedAt, &run.Result,
		&run.Error, &run.CreatedAt); err != nil {
		return nil, err
	}
	return &run, nil
}

// syncRunByID is SyncRunService.get_run after the id parsed.
func (s store) syncRunByID(ctx context.Context, orgID string, id uuid.UUID) (*syncRun, error) {
	run, err := scanSyncRun(s.pool.QueryRow(ctx,
		`SELECT `+syncRunColumns+` FROM sync_runs WHERE id = $1 AND org_id = $2`, id, orgID))
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	return run, err
}

// syncRunsByID is _planner_sync_runs_for_job_runs.
func (s store) syncRunsByID(ctx context.Context, orgID string, ids []uuid.UUID) (map[uuid.UUID]*syncRun, error) {
	found := map[uuid.UUID]*syncRun{}
	if len(ids) == 0 {
		return found, nil
	}
	rows, err := s.pool.Query(ctx,
		`SELECT `+syncRunColumns+` FROM sync_runs WHERE id = ANY($1) AND org_id = $2`, ids, orgID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		run, err := scanSyncRun(rows)
		if err != nil {
			return nil, err
		}
		found[run.ID] = run
	}
	return found, rows.Err()
}

// unitStatusCounts is the per-run status count of
// _planner_sync_run_unit_rollups_for_job_runs.
func (s store) unitStatusCounts(ctx context.Context, orgID string, runIDs []uuid.UUID) (map[uuid.UUID]map[string]int64, error) {
	counts := map[uuid.UUID]map[string]int64{}
	rows, err := s.pool.Query(ctx,
		`SELECT sync_run_id, status, count(id) FROM sync_run_units
WHERE sync_run_id = ANY($1) AND org_id = $2 GROUP BY sync_run_id, status`, runIDs, orgID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var runID uuid.UUID
		var status string
		var count int64
		if err := rows.Scan(&runID, &status, &count); err != nil {
			return nil, err
		}
		if counts[runID] == nil {
			counts[runID] = map[string]int64{}
		}
		counts[runID][status] = count
	}
	return counts, rows.Err()
}

// unitRange is one (run, source) row of _planner_sync_run_unit_ranges.
type unitRange struct {
	RunID    uuid.UUID
	SourceID uuid.UUID
	Since    time.Time
	Before   time.Time
}

// unitRanges is _planner_sync_run_unit_ranges's SELECT.
func (s store) unitRanges(ctx context.Context, orgID string, runIDs []uuid.UUID, successOnly bool) ([]unitRange, error) {
	query := `SELECT sync_run_id, source_id, min(since_at), max(before_at) FROM sync_run_units
WHERE sync_run_id = ANY($1) AND org_id = $2 AND since_at IS NOT NULL AND before_at IS NOT NULL`
	if successOnly {
		query += ` AND status = 'success'`
	}
	query += ` GROUP BY sync_run_id, source_id`
	rows, err := s.pool.Query(ctx, query, runIDs, orgID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var ranges []unitRange
	for rows.Next() {
		var item unitRange
		if err := rows.Scan(&item.RunID, &item.SourceID, &item.Since, &item.Before); err != nil {
			return nil, err
		}
		ranges = append(ranges, item)
	}
	return ranges, rows.Err()
}

// backfillJob is one backfill_jobs row.
type backfillJob struct {
	ID              uuid.UUID
	OrgID           string
	SyncConfigID    uuid.UUID
	CeleryTaskID    *string
	Status          string
	SinceDate       time.Time
	BeforeDate      time.Time
	TotalChunks     int64
	CompletedChunks int64
	FailedChunks    int64
	ErrorMessage    *string
	StartedAt       *time.Time
	CompletedAt     *time.Time
	CreatedAt       time.Time
	UpdatedAt       time.Time
}

// countBackfillJobs and backfillJobs are BackfillJobService.list_jobs.
func (s store) countBackfillJobs(ctx context.Context, orgID string) (int64, error) {
	var total int64
	err := s.pool.QueryRow(ctx, `SELECT count(*) FROM backfill_jobs WHERE org_id = $1`, orgID).Scan(&total)
	return total, err
}

func (s store) backfillJobs(ctx context.Context, orgID string, limit, offset int64) ([]backfillJob, error) {
	rows, err := s.pool.Query(ctx,
		`SELECT id, org_id, sync_config_id, celery_task_id, status, since_date, before_date, total_chunks,
completed_chunks, failed_chunks, error_message, started_at, completed_at, created_at, updated_at
FROM backfill_jobs WHERE org_id = $1 ORDER BY created_at DESC LIMIT $2 OFFSET $3`, orgID, limit, offset)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var jobs []backfillJob
	for rows.Next() {
		var job backfillJob
		if err := rows.Scan(&job.ID, &job.OrgID, &job.SyncConfigID, &job.CeleryTaskID, &job.Status, &job.SinceDate,
			&job.BeforeDate, &job.TotalChunks, &job.CompletedChunks, &job.FailedChunks, &job.ErrorMessage,
			&job.StartedAt, &job.CompletedAt, &job.CreatedAt, &job.UpdatedAt); err != nil {
			return nil, err
		}
		jobs = append(jobs, job)
	}
	return jobs, rows.Err()
}

// unitActivity is _backfill_job_run_counts's latest unit updated_at and
// heartbeat for one run.
func (s store) unitActivity(ctx context.Context, orgID string, runID uuid.UUID) (*time.Time, *time.Time, error) {
	var updated, heartbeat *time.Time
	err := s.pool.QueryRow(ctx,
		`SELECT max(updated_at), max(last_heartbeat_at) FROM sync_run_units WHERE sync_run_id = $1 AND org_id = $2`,
		runID, orgID).Scan(&updated, &heartbeat)
	return updated, heartbeat, err
}

// runStatusCounts is _backfill_job_run_counts's status count for one run.
func (s store) runStatusCounts(ctx context.Context, orgID string, runID uuid.UUID) (map[string]int64, error) {
	rows, err := s.pool.Query(ctx,
		`SELECT status, count(*) FROM sync_run_units WHERE sync_run_id = $1 AND org_id = $2 GROUP BY status`, runID, orgID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	counts := map[string]int64{}
	for rows.Next() {
		var status string
		var count int64
		if err := rows.Scan(&status, &count); err != nil {
			return nil, err
		}
		counts[status] = count
	}
	return counts, rows.Err()
}
