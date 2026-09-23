package syncadmin

import (
	"errors"
	"math/big"
	"net/http"
	"sort"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/api/pybody"
	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"
	"github.com/full-chaos/dev-health-ops/internal/pythonparity"
)

// jobRunStatusLabels is JOB_RUN_STATUS_LABELS; any other status is
// "failed".
var jobRunStatusLabels = map[int64]string{0: "pending", 1: "running", 2: "success", 3: "failed", 4: "cancelled"}

// Sync run and unit statuses (models/integrations.py).
const (
	statusPlanned       = "planned"
	statusDispatching   = "dispatching"
	statusRunning       = "running"
	statusRetrying      = "retrying"
	statusSuccess       = "success"
	statusFailed        = "failed"
	statusPartialFailed = "partial_failed"
)

var (
	pageLimitMin = int64(1)
	pageLimitMax = int64(200)
	pageOffsetGe = int64(0)
)

// pageQuery validates `limit: int = Query(default=50, ge=1, le=200)` and
// `offset: int = Query(default=0, ge=0)`, both errors in one 422.
func pageQuery(w http.ResponseWriter, r *http.Request) (limit, offset *big.Int, ok bool) {
	query := r.URL.Query()
	var errs pybody.Errors
	limit, _ = errs.QueryInt("limit", pybody.QueryValue(query, "limit"), 50, &pageLimitMin, &pageLimitMax)
	offset, _ = errs.QueryInt("offset", pybody.QueryValue(query, "offset"), 0, &pageOffsetGe, nil)
	if len(errs) > 0 {
		writeQueryErrors(w, errs)
		return nil, nil, false
	}
	return limit, offset, true
}

// offsetValue is an offset past int64 as Postgres receives it: the Python
// plane binds it as an int8 parameter too, and asyncpg refuses a value out
// of that range before any query runs, which FastAPI answers with a 500.
func offsetValue(offset *big.Int) (int64, bool) {
	if !offset.IsInt64() {
		return 0, false
	}
	return offset.Int64(), true
}

// listJobs is list_sync_config_jobs.
func (h *handlers) listJobs(w http.ResponseWriter, r *http.Request) {
	limit, offset, ok := pageQuery(w, r)
	if !ok {
		return
	}
	config, ok := h.configFromPath(w, r)
	if !ok {
		return
	}
	ctx := r.Context()
	org := orgID(r)
	jobIDs, err := h.store.scheduledSyncJobIDs(ctx, org, config.ID)
	if err != nil {
		h.fail(w, r, "scheduled_jobs", err)
		return
	}
	if len(jobIDs) == 0 {
		policy.WriteModel(w, http.StatusOK, []pyjson.Value{}, nil)
		return
	}
	offsetInt, ok := offsetValue(offset)
	if !ok {
		h.fail(w, r, "offset_range", errOffsetRange)
		return
	}
	runs, err := h.store.jobRuns(ctx, jobIDs, limit.Int64(), offsetInt)
	if err != nil {
		h.fail(w, r, "job_runs", err)
		return
	}
	decoded := make([]pyjson.Value, len(runs))
	syncRunIDs := make([]*uuid.UUID, len(runs))
	var idSet []uuid.UUID
	for index := range runs {
		value, err := decodeStored(runs[index].Result)
		if err != nil {
			h.fail(w, r, "decode_job_result", err)
			return
		}
		decoded[index] = value
		if id := plannerSyncRunID(value); id != nil {
			syncRunIDs[index] = id
			idSet = append(idSet, *id)
		}
	}
	plannerRuns, rollups, err := h.plannerRollups(r, org, idSet)
	if err != nil {
		h.fail(w, r, "planner_rollups", err)
		return
	}
	list := make([]pyjson.Value, len(runs))
	for index := range runs {
		var plannerRun *syncRun
		var rollup *unitRollup
		if id := syncRunIDs[index]; id != nil {
			plannerRun = plannerRuns[*id]
			rollup = rollups[*id]
		}
		body, err := jobRunResponse(&runs[index], decoded[index], plannerRun, rollup)
		if err != nil {
			h.fail(w, r, "render_job_run", err)
			return
		}
		list[index] = body
	}
	policy.WriteModel(w, http.StatusOK, list, nil)
}

var errOffsetRange = errors.New("offset is outside the int8 range")

// plannerSyncRunID is _planner_job_run_sync_run_id: result["sync_run_id"]
// when result is a dict, read as uuid.UUID(str(value)); nil when absent,
// None, or not a UUID.
func plannerSyncRunID(result pyjson.Value) *uuid.UUID {
	object, ok := result.(*pyjson.Object)
	if !ok {
		return nil
	}
	// An absent or None value reads as str(None), which is no UUID.
	value, _ := object.Get("sync_run_id")
	parsed, err := pythonparity.ParseUUID(pyjson.Str(value))
	if err != nil {
		return nil
	}
	return &parsed
}

// coverageRange is SyncCoverageRange for one run.
type coverageRange struct {
	Since, Before time.Time
	SourceIDs     []string
	RunID         uuid.UUID
}

func (c *coverageRange) value() pyjson.Value {
	if c == nil {
		return nil
	}
	sources := make([]pyjson.Value, len(c.SourceIDs))
	for index, id := range c.SourceIDs {
		sources[index] = id
	}
	out := pyjson.NewObject()
	out.Set("since", pyTime(c.Since))
	out.Set("before", pyTime(c.Before))
	out.Set("source_ids", sources)
	out.Set("run_ids", []pyjson.Value{c.RunID.String()})
	return out
}

// unitRollup is _SyncRunUnitRollup.
type unitRollup struct {
	StatusCounts map[string]int64
	Requested    *coverageRange
	Covered      *coverageRange
}

// plannerRollups is _planner_sync_runs_for_job_runs plus
// _planner_sync_run_unit_rollups_for_job_runs over the page's distinct
// sync run ids. A rollup exists for every id, found run or not.
func (h *handlers) plannerRollups(r *http.Request, org string, ids []uuid.UUID) (map[uuid.UUID]*syncRun, map[uuid.UUID]*unitRollup, error) {
	rollups := map[uuid.UUID]*unitRollup{}
	if len(ids) == 0 {
		return map[uuid.UUID]*syncRun{}, rollups, nil
	}
	ctx := r.Context()
	runs, err := h.store.syncRunsByID(ctx, org, ids)
	if err != nil {
		return nil, nil, err
	}
	counts, err := h.store.unitStatusCounts(ctx, org, ids)
	if err != nil {
		return nil, nil, err
	}
	requested, err := h.rangesByRun(r, org, ids, false)
	if err != nil {
		return nil, nil, err
	}
	covered, err := h.rangesByRun(r, org, ids, true)
	if err != nil {
		return nil, nil, err
	}
	for _, id := range ids {
		rollups[id] = &unitRollup{StatusCounts: counts[id], Requested: requested[id], Covered: covered[id]}
	}
	return runs, rollups, nil
}

// rangesByRun is _planner_sync_run_unit_ranges: per run, the earliest
// since and latest before over its sources, and the sorted source ids.
func (h *handlers) rangesByRun(r *http.Request, org string, ids []uuid.UUID, successOnly bool) (map[uuid.UUID]*coverageRange, error) {
	rows, err := h.store.unitRanges(r.Context(), org, ids, successOnly)
	if err != nil {
		return nil, err
	}
	out := map[uuid.UUID]*coverageRange{}
	sources := map[uuid.UUID]map[string]bool{}
	for _, row := range rows {
		since, before := row.Since.UTC(), row.Before.UTC()
		current := out[row.RunID]
		if current == nil {
			current = &coverageRange{Since: since, Before: before, RunID: row.RunID}
			out[row.RunID] = current
			sources[row.RunID] = map[string]bool{}
		}
		if since.Before(current.Since) {
			current.Since = since
		}
		if before.After(current.Before) {
			current.Before = before
		}
		sources[row.RunID][row.SourceID.String()] = true
	}
	for runID, current := range out {
		for id := range sources[runID] {
			current.SourceIDs = append(current.SourceIDs, id)
		}
		sort.Strings(current.SourceIDs)
	}
	return out, nil
}

// effectiveRunStatus is _effective_backfill_run_status.
func effectiveRunStatus(runStatus string, counts map[string]int64, totalUnits int64) string {
	success, failed := counts[statusSuccess], counts[statusFailed]
	settled := success + failed
	if totalUnits > 0 && settled >= totalUnits {
		switch {
		case failed == 0:
			return statusSuccess
		case success == 0:
			return statusFailed
		default:
			return statusPartialFailed
		}
	}
	if settled > 0 || counts[statusRunning] > 0 || counts[statusRetrying] > 0 {
		return statusRunning
	}
	if counts[statusDispatching] > 0 {
		return statusDispatching
	}
	if counts[statusPlanned] > 0 {
		return statusPlanned
	}
	return runStatus
}

// runUnitRollup is _sync_run_unit_rollup: total, completed, failed and
// the effective status.
func runUnitRollup(run *syncRun, counts map[string]int64) (int64, int64, int64, string) {
	var sum int64
	for _, count := range counts {
		sum += count
	}
	total := max(sum, run.TotalUnits)
	return total, counts[statusSuccess], counts[statusFailed], effectiveRunStatus(run.Status, counts, total)
}

// plannerJobRunStatus is _planner_job_run_status.
func plannerJobRunStatus(status string) int64 {
	switch status {
	case statusPlanned:
		return 0
	case statusSuccess:
		return 2
	case statusPartialFailed, statusFailed:
		return 3
	}
	return 1
}

// elapsedSeconds is _elapsed_seconds: whole seconds, truncated, never
// negative; None unless both ends are known.
func elapsedSeconds(started, completed *time.Time) pyjson.Value {
	if started == nil || completed == nil {
		return nil
	}
	micros := completed.Sub(*started).Microseconds()
	seconds := int64(float64(micros) / 1e6)
	return max(0, seconds)
}

// itemsSyncedKeys is _items_synced_from_result's key order.
var itemsSyncedKeys = []string{"items_synced", "rows_ingested", "rows", "items", "count"}

// itemsSynced is _items_synced_from_result.
func itemsSynced(result pyjson.Value) (pyjson.Int, error) {
	object, ok := result.(*pyjson.Object)
	if !ok {
		return pyjson.IntOf(0), nil
	}
	for _, key := range itemsSyncedKeys {
		if value, present := object.Get(key); present {
			return pyIntOrZero(value)
		}
	}
	return pyjson.IntOf(0), nil
}

// jobRunResponse is _job_run_response. A planner-managed run takes its
// lifecycle and counts from the linked sync run.
func jobRunResponse(run *jobRun, result pyjson.Value, plannerRun *syncRun, rollup *unitRollup) (*pyjson.Object, error) {
	statusValue := run.Status
	startedAt, completedAt := pyTimeOrNone(run.StartedAt), pyTimeOrNone(run.CompletedAt)
	var duration pyjson.Value
	if run.DurationSeconds != nil {
		duration = *run.DurationSeconds
	}
	errorValue := stringOrNone(run.Error)
	var items pyjson.Value
	count, err := itemsSynced(result)
	if err != nil {
		return nil, err
	}
	items = count
	resultValue := result
	var enrichment pyjson.Value

	if plannerRun != nil {
		counts := map[string]int64{}
		if rollup != nil {
			counts = rollup.StatusCounts
		}
		total, completed, failed, effective := runUnitRollup(plannerRun, counts)
		syncResult, err := decodeStored(plannerRun.Result)
		if err != nil {
			return nil, err
		}
		merged := pyjson.NewObject()
		for _, source := range []pyjson.Value{result, syncResult} {
			if object, ok := source.(*pyjson.Object); ok {
				for _, key := range object.Keys() {
					value, _ := object.Get(key)
					merged.Set(key, value)
				}
			}
		}
		merged.Set("sync_run_status", effective)
		merged.Set("total_units", total)
		merged.Set("completed_units", completed)
		merged.Set("failed_units", failed)
		resultValue = merged
		statusValue = plannerJobRunStatus(effective)
		startedAt, completedAt = pyTimeOrNone(plannerRun.StartedAt), pyTimeOrNone(plannerRun.CompletedAt)
		duration = elapsedSeconds(plannerRun.StartedAt, plannerRun.CompletedAt)
		if plannerRun.Error != nil && *plannerRun.Error != "" {
			errorValue = *plannerRun.Error
		}
		items = completed
		enrichment = jobEnrichment(plannerRun, rollup, total, completed, failed)
	}
	resultValue, err = pyDictOrNone(resultValue)
	if err != nil {
		return nil, err
	}
	label, known := jobRunStatusLabels[statusValue]
	if !known {
		label = statusFailed
	}
	out := pyjson.NewObject()
	out.Set("id", run.ID.String())
	out.Set("job_id", run.JobID.String())
	out.Set("status", label)
	out.Set("started_at", startedAt)
	out.Set("completed_at", completedAt)
	out.Set("duration_seconds", duration)
	out.Set("items_synced", items)
	out.Set("result", resultValue)
	out.Set("error", errorValue)
	out.Set("triggered_by", run.TriggeredBy)
	out.Set("sync_run", enrichment)
	out.Set("created_at", pyTime(run.CreatedAt))
	return out, nil
}

// jobEnrichment is _sync_run_job_enrichment (SyncRunJobEnrichment).
func jobEnrichment(run *syncRun, rollup *unitRollup, total, completed, failed int64) *pyjson.Object {
	var requested, covered pyjson.Value
	if rollup != nil {
		requested, covered = rollup.Requested.value(), rollup.Covered.value()
	}
	out := pyjson.NewObject()
	out.Set("mode", run.Mode)
	out.Set("triggered_by", run.TriggeredBy)
	out.Set("requested_range", requested)
	out.Set("covered_range", covered)
	out.Set("total_units", total)
	out.Set("completed_units", completed)
	out.Set("failed_units", failed)
	out.Set("sync_run_id", run.ID.String())
	return out
}

// listBackfillJobs is list_backfill_jobs.
func (h *handlers) listBackfillJobs(w http.ResponseWriter, r *http.Request) {
	limit, offset, ok := pageQuery(w, r)
	if !ok {
		return
	}
	ctx := r.Context()
	org := orgID(r)
	total, err := h.store.countBackfillJobs(ctx, org)
	if err != nil {
		h.fail(w, r, "count_backfill_jobs", err)
		return
	}
	offsetInt, ok := offsetValue(offset)
	if !ok {
		h.fail(w, r, "offset_range", errOffsetRange)
		return
	}
	jobs, err := h.store.backfillJobs(ctx, org, limit.Int64(), offsetInt)
	if err != nil {
		h.fail(w, r, "backfill_jobs", err)
		return
	}
	items := make([]pyjson.Value, len(jobs))
	for index := range jobs {
		counts, err := h.backfillRunCounts(r, &jobs[index])
		if err != nil {
			h.fail(w, r, "backfill_run_counts", err)
			return
		}
		items[index] = backfillJobResponse(&jobs[index], counts)
	}
	out := pyjson.NewObject()
	out.Set("items", items)
	out.Set("total", total)
	out.Set("limit", pyjson.Int{Int: limit})
	out.Set("offset", pyjson.Int{Int: offset})
	policy.WriteJSON(w, http.StatusOK, out, nil)
}

// backfillCounts is _backfill_job_run_counts's result dict.
type backfillCounts struct {
	Status                   string
	Total, Completed, Failed int64
	CompletedAt              *time.Time
	ErrorMessage             *string
	UpdatedAt                *time.Time
}

// backfillSyncRunID is _backfill_job_sync_run_id: the text after the last
// "sync_run:" marker in celery_task_id, "" when none.
func backfillSyncRunID(job *backfillJob) string {
	if job.CeleryTaskID == nil {
		return ""
	}
	const marker = "sync_run:"
	index := strings.LastIndex(*job.CeleryTaskID, marker)
	if index < 0 {
		return ""
	}
	return (*job.CeleryTaskID)[index+len(marker):]
}

// backfillRunCounts is _backfill_job_run_counts: nil when the job names no
// sync run, a non-UUID one, or one not in the job's org.
func (h *handlers) backfillRunCounts(r *http.Request, job *backfillJob) (*backfillCounts, error) {
	runID, err := pythonparity.ParseUUID(backfillSyncRunID(job))
	if err != nil {
		return nil, nil
	}
	ctx := r.Context()
	run, err := h.store.syncRunByID(ctx, job.OrgID, runID)
	if err != nil || run == nil {
		return nil, err
	}
	updated, heartbeat, err := h.store.unitActivity(ctx, job.OrgID, runID)
	if err != nil {
		return nil, err
	}
	counts, err := h.store.runStatusCounts(ctx, job.OrgID, runID)
	if err != nil {
		return nil, err
	}
	var sum int64
	for _, count := range counts {
		sum += count
	}
	total := max(sum, run.TotalUnits)
	return &backfillCounts{
		Status:       effectiveRunStatus(run.Status, counts, total),
		Total:        total,
		Completed:    counts[statusSuccess],
		Failed:       counts[statusFailed],
		CompletedAt:  run.CompletedAt,
		ErrorMessage: run.Error,
		UpdatedAt:    latest(updated, heartbeat),
	}, nil
}

// latest is _latest_datetime over two nullable instants.
func latest(first, second *time.Time) *time.Time {
	switch {
	case first == nil:
		return second
	case second == nil:
		return first
	case second.After(*first):
		return second
	}
	return first
}

// backfillJobResponse is _backfill_job_response (BackfillJobResponse) for
// the list: metrics_diagnostics stays None.
func backfillJobResponse(job *backfillJob, counts *backfillCounts) *pyjson.Object {
	status := job.Status
	total, completed, failed := job.TotalChunks, job.CompletedChunks, job.FailedChunks
	errorMessage := stringOrNone(job.ErrorMessage)
	completedAt := pyTimeOrNone(job.CompletedAt)
	updatedAt := job.UpdatedAt
	if counts != nil {
		status, total, completed, failed = counts.Status, counts.Total, counts.Completed, counts.Failed
		errorMessage = stringOrNone(counts.ErrorMessage)
		completedAt = pyTimeOrNone(counts.CompletedAt)
		updatedAt = *latest(&job.UpdatedAt, counts.UpdatedAt)
	}
	progress := 0.0
	if total > 0 {
		progress = float64(completed) / float64(total) * 100
	}
	out := pyjson.NewObject()
	out.Set("id", job.ID.String())
	out.Set("sync_config_id", job.SyncConfigID.String())
	out.Set("status", status)
	out.Set("since_date", job.SinceDate.Format(time.DateOnly))
	out.Set("before_date", job.BeforeDate.Format(time.DateOnly))
	out.Set("total_chunks", total)
	out.Set("completed_chunks", completed)
	out.Set("failed_chunks", failed)
	out.Set("progress_pct", pyjson.Float(progress))
	out.Set("error_message", errorMessage)
	out.Set("started_at", pyTimeOrNone(job.StartedAt))
	out.Set("completed_at", completedAt)
	out.Set("created_at", pyTime(job.CreatedAt))
	out.Set("updated_at", pyTime(updatedAt))
	out.Set("metrics_diagnostics", nil)
	return out
}

// getSyncRun is get_sync_run.
func (h *handlers) getSyncRun(w http.ResponseWriter, r *http.Request) {
	id, err := pythonparity.ParseUUID(r.PathValue("run_id"))
	if err != nil {
		policy.WriteDetail(w, http.StatusNotFound, "Sync run not found", nil)
		return
	}
	run, err := h.store.syncRunByID(r.Context(), orgID(r), id)
	if err != nil {
		h.fail(w, r, "get_sync_run", err)
		return
	}
	if run == nil {
		policy.WriteDetail(w, http.StatusNotFound, "Sync run not found", nil)
		return
	}
	body, err := syncRunResponse(run)
	if err != nil {
		h.fail(w, r, "render_sync_run", err)
		return
	}
	policy.WriteModel(w, http.StatusOK, body, nil)
}

// syncRunResponse is _sync_run_to_response (SyncRunResponse).
func syncRunResponse(run *syncRun) (*pyjson.Object, error) {
	decoded, err := decodeStored(run.Result)
	if err != nil {
		return nil, err
	}
	result, err := pyDictOrNone(decoded)
	if err != nil {
		return nil, err
	}
	integrationID := "None"
	if run.IntegrationID != nil {
		integrationID = run.IntegrationID.String()
	}
	out := pyjson.NewObject()
	out.Set("id", run.ID.String())
	out.Set("org_id", run.OrgID)
	out.Set("integration_id", integrationID)
	out.Set("triggered_by", run.TriggeredBy)
	out.Set("mode", run.Mode)
	out.Set("status", run.Status)
	out.Set("total_units", run.TotalUnits)
	out.Set("completed_units", run.CompletedUnits)
	out.Set("failed_units", run.FailedUnits)
	out.Set("started_at", pyTimeOrNone(run.StartedAt))
	out.Set("completed_at", pyTimeOrNone(run.CompletedAt))
	out.Set("result", result)
	out.Set("error", stringOrNone(run.Error))
	out.Set("created_at", pyTime(run.CreatedAt))
	return out, nil
}
