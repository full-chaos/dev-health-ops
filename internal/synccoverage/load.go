package synccoverage

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	schedulersync "github.com/full-chaos/dev-health-ops/internal/scheduler/sync"
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
       COALESCE(run.completed_at, run.started_at, run.created_at), unit.id,
       COALESCE(unit.result->>'reports_complete' = 'false', false),
       (unit.result->'incomplete')::text
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
			return nil, nil, rowCount, newLimitError("projection_row", rowCount, maxProjectionRows)
		}
		var sourceID uuid.UUID
		var dataset, status string
		var flags json.RawMessage
		var since, before, runTime time.Time
		var unitID uuid.UUID
		var reportsIncomplete bool
		var incomplete *string
		if err := rows.Scan(&sourceID, &dataset, &flags, &since, &before, &status, &runTime, &unitID, &reportsIncomplete, &incomplete); err != nil {
			return nil, nil, rowCount, fmt.Errorf("scan sync coverage unit window: %w", err)
		}
		since = maxTime(since.UTC(), truncatedBefore)
		before = before.UTC()
		if !since.Before(before) {
			continue
		}
		effectiveKeys, err := effectiveDatasetKeys(dataset, flags)
		if err != nil {
			return nil, nil, rowCount, fmt.Errorf("expand sync coverage unit window: %w", err)
		}
		var closedReasons []string
		if status == "success" && reportsIncomplete {
			closedReasons = closedWithReason(incomplete)
		}
		for _, effectiveKey := range effectiveKeys {
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
			// A tests unit that finished with dropped or skipped reports
			// advanced its watermark but did not cover the window: it stays a
			// gap and is not the last complete sync. It did run, so it
			// supersedes an earlier failure of the window: the window reads as
			// a gap, not as failed.
			//
			// CHAOS-13988 "close with reason": when every lost report is
			// closed with a reason (closedWithReasonCauses) the window is
			// covered like a complete unit's, and is also recorded with its
			// reasons. A window once closed stays covered: a later gap-class
			// unit over it does not reopen it, and only a complete unit
			// (whatever its order) removes the reason.
			if status == "success" && reportsIncomplete && effectiveKey == "tests" {
				if len(closedReasons) == 0 {
					state.subtractFailed(interval)
					continue
				}
				state.Closed = append(state.Closed, closedRange{Interval: interval, Reasons: closedReasons})
			} else if status == "success" {
				state.Complete = append(state.Complete, interval)
			}
			switch status {
			case "success":
				state.Covered = append(state.Covered, interval)
				state.subtractFailed(interval)
				value := runTime.UTC()
				if latestSuccessful == nil || value.After(*latestSuccessful) {
					latestSuccessful = &value
				}
			case "failed":
				state.addFailed(interval)
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
		for _, item := range subtractClosedRanges(state.Closed, state.Complete) {
			windows = append(windows, unitWindow{Since: item.Interval.Since, Before: item.Interval.Before, SourceID: parts[0], DatasetKey: parts[1], Status: statusClosedWithReason, Reasons: item.Reasons})
		}
	}
	if len(windows) > maxCompactWindows {
		return nil, nil, rowCount, newLimitError("compact_window", len(windows), maxCompactWindows)
	}
	return windows, latestSuccessful, rowCount, nil
}

// closedWithReasonCauses are the report_member causes that close a tests
// window with a reason (chris 2026-11-17, CHAOS-13988): a report GitHub
// deleted after its retention period (artifact_expired), a broken report
// member (malformed) or a member refused by design (unsafe_doctype) can never
// be read by a retry or a backfill, so the window is covered and the reason
// is shown. The strings mirror the providersync GitHub tests vocabulary; this
// package does not import providersync.
var closedWithReasonCauses = map[string]map[string]struct{}{
	"report_member": {"artifact_expired": {}, "malformed": {}, "unsafe_doctype": {}},
}

// closedWithReason returns the distinct sorted causes of a unit result's
// `incomplete` list when EVERY observation in it is a closedWithReasonCauses
// pair. It returns nil -- the window stays a gap -- for a missing key, JSON
// null, an undecodable list, an empty list, or any other observation.
func closedWithReason(incomplete *string) []string {
	if incomplete == nil {
		return nil
	}
	var observations []struct {
		Component string `json:"component"`
		Cause     string `json:"cause"`
	}
	if err := json.Unmarshal([]byte(*incomplete), &observations); err != nil || len(observations) == 0 {
		return nil
	}
	reasons := make([]string, 0, len(observations))
	for _, observation := range observations {
		if _, ok := closedWithReasonCauses[observation.Component][observation.Cause]; !ok {
			return nil
		}
		reasons = append(reasons, observation.Cause)
	}
	return uniqueSorted(reasons)
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
		effectiveKeys, err := effectiveDatasetKeys(dataset, flags)
		if err != nil {
			return nil, fmt.Errorf("expand active sync coverage pair: %w", err)
		}
		for _, effectiveKey := range effectiveKeys {
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
	runIDs := make([]uuid.UUID, len(jobs))
	markers := make([]string, len(jobs))
	derived := make([]uuid.UUID, 0)
	for index, job := range jobs {
		runIDs[index], markers[index] = backfillRunID(job.TaskID)
		if markers[index] == backfillMarkerOccurrence {
			derived = append(derived, runIDs[index])
		}
	}
	planned, err := plannedRunIDs(ctx, tx, config.OrgID, derived)
	if err != nil {
		return nil, err
	}
	outcomes := make(map[string]int, len(backfillResolutionOutcomes))
	result := make([]coverageInterval, 0)
	for index, job := range jobs {
		since := maxTime(job.SinceDate.UTC(), truncatedBefore)
		beforeDate := job.BeforeDate.UTC()
		before := time.Date(beforeDate.Year(), beforeDate.Month(), beforeDate.Day()+1, 0, 0, 0, 0, time.UTC).Add(-time.Microsecond)
		base := coverageInterval{Since: since, Before: before, RunIDs: []string{job.ID.String()}}
		runID := runIDs[index]
		switch markers[index] {
		case backfillMarkerSyncRun:
			outcomes[backfillOutcomeSyncRun]++
		case backfillMarkerOccurrence:
			if _, ok := planned[runID]; !ok {
				// No run planned for the occurrence (yet, or ever): neither covered nor requested by this job.
				outcomes[backfillOutcomeOccurrenceUnplanned]++
				continue
			}
			outcomes[backfillOutcomeOccurrence]++
		default:
			outcomes[backfillOutcomeUnresolved]++
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
			return nil, newLimitError("backfill_pair", len(result), maxBackfillPairs)
		}
	}
	backfillResolutionMetrics.observe(outcomes)
	if waiting := outcomes[backfillOutcomeOccurrenceUnplanned] + outcomes[backfillOutcomeUnresolved]; waiting > 0 {
		slog.WarnContext(ctx, "sync coverage: backfill jobs without a planned run",
			slog.Any("sync_config_id", config.ID),
			slog.Int(backfillOutcomeOccurrenceUnplanned, outcomes[backfillOutcomeOccurrenceUnplanned]),
			slog.Int(backfillOutcomeUnresolved, outcomes[backfillOutcomeUnresolved]))
	}
	return result, nil
}

const (
	backfillMarkerSyncRun    = "sync_run"
	backfillMarkerOccurrence = "occurrence"
	syncRunMarker            = "sync_run:"
	occurrenceMarker         = "occurrence:"
)

// backfillRunID is the run a backfill_jobs row names. "sync_run:<uuid>" names it. An admin backfill keeps
// "occurrence:<id>" (or "idem:<hash>;occurrence:<id>") for ever, so its run is the materializer's deterministic
// run of that occurrence, the same derivation finalize uses to close the job. The marker is "" when neither applies.
func backfillRunID(taskID *string) (uuid.UUID, string) {
	if taskID == nil {
		return uuid.Nil, ""
	}
	if index := strings.LastIndex(*taskID, syncRunMarker); index >= 0 {
		parsed, err := uuid.Parse((*taskID)[index+len(syncRunMarker):])
		if err != nil {
			return uuid.Nil, ""
		}
		return parsed, backfillMarkerSyncRun
	}
	index := strings.LastIndex(*taskID, occurrenceMarker)
	if index < 0 {
		return uuid.Nil, ""
	}
	occurrence := (*taskID)[index+len(occurrenceMarker):]
	if end := strings.IndexByte(occurrence, ';'); end >= 0 {
		occurrence = occurrence[:end]
	}
	if occurrence == "" {
		return uuid.Nil, ""
	}
	derived, err := schedulersync.DeterministicSyncRunID(occurrence)
	if err != nil {
		return uuid.Nil, ""
	}
	parsed, err := uuid.Parse(derived)
	if err != nil {
		return uuid.Nil, ""
	}
	return parsed, backfillMarkerOccurrence
}

// plannedRunIDs is the subset of runIDs that exist as the org's sync runs.
func plannedRunIDs(ctx context.Context, tx pgx.Tx, orgID string, runIDs []uuid.UUID) (map[uuid.UUID]struct{}, error) {
	planned := make(map[uuid.UUID]struct{}, len(runIDs))
	if len(runIDs) == 0 {
		return planned, nil
	}
	rows, err := tx.Query(ctx, `SELECT id FROM public.sync_runs WHERE org_id = $1 AND id = ANY($2::uuid[])`, orgID, runIDs)
	if err != nil {
		return nil, fmt.Errorf("query sync coverage backfill runs: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var id uuid.UUID
		if err := rows.Scan(&id); err != nil {
			return nil, fmt.Errorf("scan sync coverage backfill run: %w", err)
		}
		planned[id] = struct{}{}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate sync coverage backfill runs: %w", err)
	}
	return planned, nil
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
		effectiveKeys, err := effectiveDatasetKeys(dataset, flags)
		if err != nil {
			return nil, fmt.Errorf("expand linked backfill unit: %w", err)
		}
		for _, effectiveKey := range effectiveKeys {
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
