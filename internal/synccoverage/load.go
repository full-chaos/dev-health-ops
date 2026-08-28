package synccoverage

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

var requestedStatuses = []string{"success", "failed", "planned", "dispatching", "running", "retrying"}

func buildProjection(
	ctx context.Context,
	tx pgx.Tx,
	config syncConfig,
	now time.Time,
) (projectionPayload, *time.Time, *time.Time, error) {
	scope, err := resolveScope(ctx, tx, config)
	if err != nil {
		return nil, nil, nil, err
	}
	truncatedBefore := now.AddDate(0, 0, -HistoryLookbackDays)
	windows, latestSuccessful, _, err := streamCompactWindows(ctx, tx, config.OrgID, scope, truncatedBefore, now)
	if err != nil {
		return nil, nil, nil, err
	}
	activePairs, err := loadActivePairs(ctx, tx, config.OrgID, scope)
	if err != nil {
		return nil, nil, nil, err
	}
	backfills, err := loadBackfillRanges(ctx, tx, config, scope, truncatedBefore)
	if err != nil {
		return nil, nil, nil, err
	}
	activeSchedule, hasSchedule, err := loadSchedule(ctx, tx, config)
	if err != nil {
		return nil, nil, nil, err
	}
	sourceUpdatedAt, err := sourceUpdatedAt(ctx, tx, config.OrgID, scope, truncatedBefore)
	if err != nil {
		return nil, nil, nil, err
	}
	backfillUpdatedAt, err := backfillUpdatedAt(ctx, tx, config, truncatedBefore)
	if err != nil {
		return nil, nil, nil, err
	}
	isTruncated, err := hasCoverageBefore(ctx, tx, config.OrgID, scope, truncatedBefore)
	if err != nil {
		return nil, nil, nil, err
	}
	payload, err := buildPayload(payloadInput{
		Config: config, Scope: scope, Windows: windows, Backfills: backfills,
		ActivePairs: activePairs, Schedule: activeSchedule, HasSchedule: hasSchedule,
		Now: now, LatestSuccessful: latestSuccessful, IsTruncated: isTruncated,
	})
	if err != nil {
		return nil, nil, nil, err
	}
	return payload, sourceUpdatedAt, backfillUpdatedAt, nil
}

func streamCompactWindows(
	ctx context.Context,
	tx pgx.Tx,
	orgID string,
	scope effectiveScope,
	truncatedBefore time.Time,
	now time.Time,
) ([]unitWindow, *time.Time, int, error) {
	if scope.IntegrationID == nil || len(scope.Sources) == 0 || len(scope.DatasetKeys) == 0 {
		return nil, nil, 0, nil
	}
	sourceIDs := make([]uuid.UUID, 0, len(scope.Sources))
	for _, item := range scope.Sources {
		sourceIDs = append(sourceIDs, item.ID)
	}
	rows, err := tx.Query(ctx, `
SELECT unit.source_id, unit.dataset_key, unit.processor_flags,
       unit.since_at, unit.before_at, unit.status,
       COALESCE(run.completed_at, run.started_at, run.created_at), unit.id
FROM public.sync_run_units AS unit
JOIN public.sync_runs AS run ON run.id = unit.sync_run_id
WHERE unit.org_id = $1
  AND unit.integration_id = $2
  AND unit.source_id = ANY($3::uuid[])
  AND unit.dataset_key = ANY($4::text[])
  AND unit.status = ANY($5::text[])
  AND unit.since_at IS NOT NULL
  AND unit.before_at IS NOT NULL
  AND unit.before_at >= $6
  AND run.org_id = $1
ORDER BY COALESCE(run.completed_at, run.started_at, run.created_at), unit.id`,
		orgID, *scope.IntegrationID, sourceIDs, queryDatasetKeys(scope.DatasetKeys), requestedStatuses, truncatedBefore)
	if err != nil {
		return nil, nil, 0, fmt.Errorf("query sync coverage unit windows: %w", err)
	}
	defer rows.Close()
	states := make(map[string]*pairState)
	var latestSuccessful *time.Time
	rowCount := 0
	for rows.Next() {
		rowCount++
		if rowCount > maxProjectionRows {
			return nil, nil, rowCount, fmt.Errorf("sync coverage projection row limit exceeded: %d", maxProjectionRows)
		}
		var sourceID uuid.UUID
		var dataset, status string
		var flags json.RawMessage
		var since, before, runTime time.Time
		var unitID uuid.UUID
		if err := rows.Scan(&sourceID, &dataset, &flags, &since, &before, &status, &runTime, &unitID); err != nil {
			return nil, nil, rowCount, fmt.Errorf("scan sync coverage unit window: %w", err)
		}
		since = maxTime(since.UTC(), truncatedBefore)
		before = before.UTC()
		if !since.Before(before) {
			continue
		}
		for _, effectiveKey := range effectiveDatasetKeys(dataset, flags) {
			if !containsString(scope.DatasetKeys, effectiveKey) {
				continue
			}
			recordFoldedKeyResolution(dataset, effectiveKey)
			key := sourceID.String() + "\x00" + effectiveKey
			state := states[key]
			if state == nil {
				state = &pairState{}
				states[key] = state
			}
			interval := coverageInterval{Since: since, Before: before}
			state.Requested = append(state.Requested, interval)
			switch status {
			case "success":
				state.Covered = append(state.Covered, interval)
				state.Failed = subtractIntervals(state.Failed, []coverageInterval{interval})
				value := runTime.UTC()
				if latestSuccessful == nil || value.After(*latestSuccessful) {
					latestSuccessful = &value
				}
			case "failed":
				state.Failed = mergeIntervals(append(state.Failed, interval))
			}
		}
	}
	if err := rows.Err(); err != nil {
		return nil, nil, rowCount, fmt.Errorf("iterate sync coverage unit windows: %w", err)
	}
	windows := make([]unitWindow, 0)
	for key, state := range states {
		parts := strings.SplitN(key, "\x00", 2)
		for _, item := range mergeIntervals(state.Requested) {
			windows = append(windows, unitWindow{Since: item.Since, Before: item.Before, SourceID: parts[0], DatasetKey: parts[1], Status: "planned", RunTime: time.Time{}})
		}
		for _, item := range mergeIntervals(state.Covered) {
			windows = append(windows, unitWindow{Since: item.Since, Before: item.Before, SourceID: parts[0], DatasetKey: parts[1], Status: "success", RunTime: time.Time{}})
		}
		for _, item := range state.Failed {
			windows = append(windows, unitWindow{Since: item.Since, Before: item.Before, SourceID: parts[0], DatasetKey: parts[1], Status: "failed", RunTime: now.Add(time.Microsecond)})
		}
		if len(windows) > maxCompactWindows {
			return nil, nil, rowCount, fmt.Errorf("sync coverage compact window limit exceeded: %d", maxCompactWindows)
		}
	}
	return windows, latestSuccessful, rowCount, nil
}

func loadActivePairs(ctx context.Context, tx pgx.Tx, orgID string, scope effectiveScope) (map[string]struct{}, error) {
	pairs := make(map[string]struct{})
	if scope.IntegrationID == nil || len(scope.Sources) == 0 || len(scope.DatasetKeys) == 0 {
		return pairs, nil
	}
	sourceIDs := make([]uuid.UUID, 0, len(scope.Sources))
	for _, item := range scope.Sources {
		sourceIDs = append(sourceIDs, item.ID)
	}
	rows, err := tx.Query(ctx, `
SELECT unit.source_id, unit.dataset_key, unit.processor_flags
FROM public.sync_runs AS run
JOIN public.sync_run_units AS unit ON unit.sync_run_id = run.id
WHERE run.org_id = $1 AND run.integration_id = $2
  AND run.status = ANY($3::text[])
  AND unit.org_id = $1
  AND unit.source_id = ANY($4::uuid[])
  AND unit.dataset_key = ANY($5::text[])`, orgID, *scope.IntegrationID,
		[]string{"planned", "dispatching", "running"}, sourceIDs, queryDatasetKeys(scope.DatasetKeys))
	if err != nil {
		return nil, fmt.Errorf("query active sync coverage pairs: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var sourceID uuid.UUID
		var dataset string
		var flags json.RawMessage
		if err := rows.Scan(&sourceID, &dataset, &flags); err != nil {
			return nil, fmt.Errorf("scan active sync coverage pair: %w", err)
		}
		for _, effectiveKey := range effectiveDatasetKeys(dataset, flags) {
			if containsString(scope.DatasetKeys, effectiveKey) {
				recordFoldedKeyResolution(dataset, effectiveKey)
				pairs[sourceID.String()+"\x00"+effectiveKey] = struct{}{}
			}
		}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate active sync coverage pairs: %w", err)
	}
	return pairs, nil
}

func loadBackfillRanges(ctx context.Context, tx pgx.Tx, config syncConfig, scope effectiveScope, truncatedBefore time.Time) ([]coverageInterval, error) {
	if len(scope.Sources) == 0 {
		return nil, nil
	}
	rows, err := tx.Query(ctx, `
SELECT id, celery_task_id, since_date, before_date
FROM public.backfill_jobs
WHERE org_id = $1 AND sync_config_id = $2 AND before_date >= $3::date`, config.OrgID, config.ID, truncatedBefore)
	if err != nil {
		return nil, fmt.Errorf("query sync coverage backfills: %w", err)
	}
	jobs := make([]backfillJob, 0)
	for rows.Next() {
		var job backfillJob
		if err := rows.Scan(&job.ID, &job.TaskID, &job.SinceDate, &job.BeforeDate); err != nil {
			rows.Close()
			return nil, fmt.Errorf("scan sync coverage backfill: %w", err)
		}
		jobs = append(jobs, job)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return nil, fmt.Errorf("iterate sync coverage backfills: %w", err)
	}
	rows.Close()

	scopeSources := make([]string, 0, len(scope.Sources))
	for _, item := range scope.Sources {
		scopeSources = append(scopeSources, item.ID.String())
	}
	result := make([]coverageInterval, 0)
	for _, job := range jobs {
		since := maxTime(job.SinceDate.UTC(), truncatedBefore)
		beforeDate := job.BeforeDate.UTC()
		before := time.Date(beforeDate.Year(), beforeDate.Month(), beforeDate.Day()+1, 0, 0, 0, 0, time.UTC).Add(-time.Microsecond)
		base := coverageInterval{Since: since, Before: before, RunIDs: []string{job.ID.String()}}
		runID, resolved := backfillRunID(job.TaskID)
		if !resolved {
			base.SourceIDs = uniqueSorted(scopeSources)
			result = append(result, base)
			continue
		}
		pairWindows, err := loadBackfillPairWindows(ctx, tx, config.OrgID, runID, scope.DatasetKeys)
		if err != nil {
			return nil, err
		}
		for pair, windows := range pairWindows {
			parts := strings.SplitN(pair, "\x00", 2)
			if !containsString(scopeSources, parts[0]) {
				continue
			}
			for _, window := range clipIntervals(windows, since, before) {
				result = append(result, coverageInterval{Since: window.Since, Before: window.Before,
					SourceIDs: []string{parts[0]}, DatasetKeys: []string{parts[1]}, RunIDs: base.RunIDs})
			}
		}
		if len(result) > maxBackfillPairs {
			return nil, fmt.Errorf("sync coverage backfill pair limit exceeded: %d", maxBackfillPairs)
		}
	}
	return result, nil
}

func backfillRunID(taskID *string) (uuid.UUID, bool) {
	if taskID == nil {
		return uuid.Nil, false
	}
	index := strings.LastIndex(*taskID, "sync_run:")
	if index < 0 {
		return uuid.Nil, false
	}
	parsed, err := uuid.Parse((*taskID)[index+len("sync_run:"):])
	return parsed, err == nil
}

// scopeDatasetKeys narrows which effective keys are accepted into the
// returned map -- and, on that narrowed set, which folded-key resolutions
// get recorded (CHAOS-4393 round 3). The caller (buildPayload) drops any
// unscoped dataset before it reaches a projected window regardless, so
// filtering here changes no observable output; it just moves the filter to
// where a genuine, PROJECTED alias resolution can be told apart from one a
// now-disabled or out-of-scope alias flag would otherwise inflate the
// counter with.
func loadBackfillPairWindows(ctx context.Context, tx pgx.Tx, orgID string, runID uuid.UUID, scopeDatasetKeys []string) (map[string][]coverageInterval, error) {
	rows, err := tx.Query(ctx, `
SELECT source_id, dataset_key, processor_flags, since_at, before_at
FROM public.sync_run_units
WHERE org_id = $1 AND sync_run_id = $2
  AND since_at IS NOT NULL AND before_at IS NOT NULL`, orgID, runID)
	if err != nil {
		return nil, fmt.Errorf("query linked backfill units: %w", err)
	}
	defer rows.Close()
	result := make(map[string][]coverageInterval)
	for rows.Next() {
		var sourceID uuid.UUID
		var dataset string
		var flags json.RawMessage
		var since, before time.Time
		if err := rows.Scan(&sourceID, &dataset, &flags, &since, &before); err != nil {
			return nil, fmt.Errorf("scan linked backfill unit: %w", err)
		}
		for _, effectiveKey := range effectiveDatasetKeys(dataset, flags) {
			if !containsString(scopeDatasetKeys, effectiveKey) {
				continue
			}
			recordFoldedKeyResolution(dataset, effectiveKey)
			key := sourceID.String() + "\x00" + effectiveKey
			result[key] = append(result[key], coverageInterval{Since: since.UTC(), Before: before.UTC()})
		}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate linked backfill units: %w", err)
	}
	for key := range result {
		result[key] = mergeIntervals(result[key])
	}
	return result, nil
}

func clipIntervals(input []coverageInterval, since, before time.Time) []coverageInterval {
	result := make([]coverageInterval, 0, len(input))
	for _, interval := range input {
		start := maxTime(interval.Since, since)
		end := minTime(interval.Before, before)
		if start.Before(end) {
			result = append(result, coverageInterval{Since: start, Before: end})
		}
	}
	return result
}

func loadSchedule(ctx context.Context, tx pgx.Tx, config syncConfig) (*schedule, bool, error) {
	var hasSchedule bool
	if err := tx.QueryRow(ctx, `
SELECT EXISTS (
  SELECT 1 FROM public.scheduled_jobs
  WHERE org_id = $1 AND sync_config_id = $2 AND job_type = 'sync'
)`, config.OrgID, config.ID).Scan(&hasSchedule); err != nil {
		return nil, false, fmt.Errorf("query sync coverage schedule presence: %w", err)
	}
	var active schedule
	err := tx.QueryRow(ctx, `
SELECT schedule_cron, next_run_at
FROM public.scheduled_jobs
WHERE org_id = $1 AND sync_config_id = $2 AND job_type = 'sync' AND status = 0
ORDER BY next_run_at ASC NULLS LAST, created_at DESC
LIMIT 1`, config.OrgID, config.ID).Scan(&active.Cron, &active.NextRunAt)
	if err == pgx.ErrNoRows {
		return nil, hasSchedule, nil
	}
	if err != nil {
		return nil, false, fmt.Errorf("query active sync coverage schedule: %w", err)
	}
	return &active, hasSchedule, nil
}

func sourceUpdatedAt(ctx context.Context, tx pgx.Tx, orgID string, scope effectiveScope, truncated time.Time) (*time.Time, error) {
	if scope.IntegrationID == nil || len(scope.Sources) == 0 || len(scope.DatasetKeys) == 0 {
		return nil, nil
	}
	sourceIDs := make([]uuid.UUID, 0, len(scope.Sources))
	for _, item := range scope.Sources {
		sourceIDs = append(sourceIDs, item.ID)
	}
	var value *time.Time
	if err := tx.QueryRow(ctx, `
SELECT max(updated_at) FROM public.sync_run_units
WHERE org_id = $1 AND integration_id = $2
  AND source_id = ANY($3::uuid[]) AND dataset_key = ANY($4::text[])
  AND before_at >= $5 AND status = ANY($6::text[])`, orgID, *scope.IntegrationID,
		sourceIDs, queryDatasetKeys(scope.DatasetKeys), truncated, requestedStatuses).Scan(&value); err != nil {
		return nil, fmt.Errorf("query sync coverage source timestamp: %w", err)
	}
	if value != nil {
		utc := value.UTC()
		value = &utc
	}
	return value, nil
}

func backfillUpdatedAt(ctx context.Context, tx pgx.Tx, config syncConfig, truncated time.Time) (*time.Time, error) {
	var value *time.Time
	if err := tx.QueryRow(ctx, `
SELECT max(updated_at) FROM public.backfill_jobs
WHERE org_id = $1 AND sync_config_id = $2 AND before_date >= $3::date`,
		config.OrgID, config.ID, truncated).Scan(&value); err != nil {
		return nil, fmt.Errorf("query sync coverage backfill timestamp: %w", err)
	}
	if value != nil {
		utc := value.UTC()
		value = &utc
	}
	return value, nil
}

func hasCoverageBefore(ctx context.Context, tx pgx.Tx, orgID string, scope effectiveScope, truncated time.Time) (bool, error) {
	if scope.IntegrationID == nil || len(scope.Sources) == 0 || len(scope.DatasetKeys) == 0 {
		return false, nil
	}
	sourceIDs := make([]uuid.UUID, 0, len(scope.Sources))
	for _, item := range scope.Sources {
		sourceIDs = append(sourceIDs, item.ID)
	}
	var exists bool
	if err := tx.QueryRow(ctx, `
SELECT EXISTS (
 SELECT 1 FROM public.sync_run_units
 WHERE org_id = $1 AND integration_id = $2
   AND source_id = ANY($3::uuid[]) AND dataset_key = ANY($4::text[])
   AND since_at < $5 AND status = 'success'
)`, orgID, *scope.IntegrationID, sourceIDs, queryDatasetKeys(scope.DatasetKeys), truncated).Scan(&exists); err != nil {
		return false, fmt.Errorf("query old sync coverage: %w", err)
	}
	return exists, nil
}
