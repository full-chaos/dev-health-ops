package syncdispatchruntime

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"sort"
	"strings"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/jobruntime"
	"github.com/full-chaos/dev-health-ops/internal/providersync"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

var ErrFinalizeSyncRunUnavailable = errors.New("native finalize_sync_run is unavailable")

// Native port of src/dev_health_ops/workers/sync_units.py::finalize_sync_run
// (CHAOS-4175). Faithfulness notes, gathered reading that function and its
// full call graph before writing any of this:
//
//   - finalize_sync_run's own status/result computation is NOT gated by the
//     once-only SyncRunPostDispatch ledger insert below: Python recomputes and
//     writes sync_runs.status/completed_units/failed_units/result/error, stamps
//     the canonical SyncConfiguration, updates BackfillJob/JobRun observers,
//     and writes SyncComputeCheckpoint rows on EVERY call -- including a
//     RE-finalization that will go on to hit "already_dispatched". Only the
//     ledger row itself, the coverage-projection invalidation, the post_sync
//     outbox wakeup, and the zero-unit counter are once-only. This native port
//     preserves that split exactly (see the two `return` points below).
//   - The CHAOS-4159 zero-unit branch reads run.result/run.error as they stood
//     BEFORE this call (the planner's recorded cause), not the not-yet-written
//     new payload -- loadFinalizeRun's return value is that pre-image and is
//     never mutated in place.
//   - job_runs.status is an INTEGER enum (PENDING=0/RUNNING=1/SUCCESS=2/
//     FAILED=3), not a string -- see jobRunStatusSuccess/jobRunStatusFailed.
//   - build_post_sync_dispatch_payload's only use inside finalize_sync_run is
//     to populate the Celery return dict's "post_sync_targets" key. Nothing in
//     the Go runtime reads a River job's return value (River jobs return only
//     error), and no other Python caller of finalize_sync_run inspects that
//     key either (grepped). So it is deliberately NOT reproduced here -- there
//     is no return-value channel for it to occupy.
type NativeFinalizeSyncRunService struct {
	pool     *pgxpool.Pool
	logger   *slog.Logger
	observer jobruntime.ZeroUnitFinalizationObserver
	now      func() time.Time
}

// NewNativeFinalizeSyncRunService constructs the native finalize_sync_run
// executor. observers follows workgraph.NewPostgresStore's variadic-observer
// convention: at most the first is used, and it is optional -- a caller that
// doesn't care about zero-unit telemetry (a unit test, say) can omit it
// entirely rather than threading a stub through. This is the SAME
// jobruntime.MetricsCollector every other worker-runtime counter lives on
// (registered once as the "worker_runtime" health.Registry source), not a
// standalone collector: CHAOS-4175's review found this codebase already has
// four bounded-cardinality multi-dimension counters on that one collector
// (sync lease results, idempotency renewal, daily-metrics lease, DORA/
// capacity refusals) and ruled the zero-unit counter belongs there too,
// rather than inventing a second, unregistered collector alongside it.
func NewNativeFinalizeSyncRunService(
	pool *pgxpool.Pool,
	logger *slog.Logger,
	observers ...jobruntime.ZeroUnitFinalizationObserver,
) (*NativeFinalizeSyncRunService, error) {
	if pool == nil {
		return nil, ErrFinalizeSyncRunUnavailable
	}
	if logger == nil {
		logger = slog.Default()
	}
	var observer jobruntime.ZeroUnitFinalizationObserver
	if len(observers) > 0 {
		observer = observers[0]
	}
	return &NativeFinalizeSyncRunService{pool: pool, logger: logger, observer: observer, now: time.Now}, nil
}

const (
	syncRunUnitStatusSuccess = "success"
	syncRunUnitStatusFailed  = "failed"

	syncRunStatusSuccess       = "success"
	syncRunStatusFailed        = "failed"
	syncRunStatusPartialFailed = "partial_failed"

	// jobRunStatusPending/Running/Success/Failed mirror
	// models.settings.JobRunStatus (an int Enum): PENDING=0 RUNNING=1
	// SUCCESS=2 FAILED=3 CANCELLED=4. Only the four this function reads or
	// writes are named here.
	jobRunStatusPending = 0
	jobRunStatusRunning = 1
	jobRunStatusSuccess = 2
	jobRunStatusFailed  = 3

	outboxKindFinalizeSyncRun = "finalize_sync_run"
	outboxKindPostSync        = "post_sync"

	featureDisabledErrorCategory = "feature_disabled"

	zeroUnitGenericError  = "No sync units planned"
	zeroUnitGenericReason = "no_sync_units_planned"

	syncComputeTypeWorkGraph      = "work_graph"
	syncComputeCheckpointStatusOK = "ready"
)

// Finalize is the native equivalent of bridge.Finalize / the Python
// finalize_sync_run task. It satisfies the same signature CoordinatorBridge
// declares so finalizeWorker can hold either a bridge or this service, the
// same way postSyncWorker already holds *NativePostSyncService instead of a
// bridge.
func (service *NativeFinalizeSyncRunService) Finalize(ctx context.Context, args FinalizeSyncRunArgs) error {
	if service == nil || service.pool == nil || ctx == nil || args.valid() != nil {
		return ErrFinalizeSyncRunUnavailable
	}
	tx, err := service.pool.Begin(ctx)
	if err != nil {
		return ErrFinalizeSyncRunUnavailable
	}
	defer func() { _ = tx.Rollback(ctx) }()

	current, err := currentTransportReference(ctx, tx, args, outboxKindFinalizeSyncRun)
	if err != nil {
		return err
	}
	if !current {
		return nil
	}

	run, err := loadFinalizeRun(ctx, tx, args)
	if err != nil {
		return err
	}
	if run == nil {
		// Python: `if run is None: ... return {"status": "missing", ...}`.
		// Nothing was staged; a committed no-op mirrors NativePostSyncService's
		// own "nothing to do" convention.
		return tx.Commit(ctx)
	}

	units, err := loadFinalizeUnits(ctx, tx, run.id)
	if err != nil {
		return err
	}
	for _, unit := range units {
		if unit.status != syncRunUnitStatusSuccess && unit.status != syncRunUnitStatusFailed {
			// Python: `return {"status": "pending", ...}`.
			return tx.Commit(ctx)
		}
	}

	aggregate := aggregateFinalizeUnits(units)

	// --- CHAOS-4159 zero-unit branch: read the PRE-image before it's
	// overwritten below. run.result/run.errorText are exactly what the
	// planner (or an earlier terminalization) last wrote. ---
	newError := run.errorText
	resultPayload := map[string]any{
		"completed_units": aggregate.successCount,
		"failed_units":    aggregate.failedCount,
	}
	if aggregate.errorCategory != "" {
		resultPayload["error_category"] = aggregate.errorCategory
	}
	// Python: `if error_category == FEATURE_DISABLED_ERROR_CATEGORY and
	// run.error is None:` -- a STRICT None check, not the whitespace-aware
	// blank check the zero-unit branch below uses. An explicitly-empty-string
	// run.error (however that could arise) must NOT be overwritten here.
	if aggregate.errorCategory == featureDisabledErrorCategory && newError == nil {
		newError = aggregate.firstFailedUnitError
	}
	var zeroUnitReason string
	isZeroUnit := len(units) == 0
	if isZeroUnit {
		plannerResult := run.result
		// Python: `if isinstance(planner_category, str) and planner_category:`
		// -- plain truthiness (a whitespace-only string IS truthy in
		// Python), not the .strip()-based blank check zeroUnitReasonFrom
		// applies to the SAME map one line below. Two different predicates
		// over the same key, ported as written rather than unified.
		if plannerCategory, ok := stringField(plannerResult, "error_category"); ok && plannerCategory != "" {
			if _, already := resultPayload["error_category"]; !already {
				resultPayload["error_category"] = plannerCategory
			}
		}
		zeroUnitReason = zeroUnitReasonFrom(plannerResult)
		// Python: `if run.error is None or not run.error.strip():` --
		// None OR whitespace-only counts as absent here (unlike the
		// feature_disabled check above, which is a strict None check).
		if newError == nil || strings.TrimSpace(*newError) == "" {
			generic := zeroUnitGenericError
			newError = &generic
		} else {
			sanitized := sanitizeErrorText(*newError)
			newError = &sanitized
		}
		resultPayload["reason"] = zeroUnitReason
	}

	newStatus := aggregateRunStatus(len(units), aggregate.successCount, aggregate.failedCount)
	completedAt := run.completedAt
	if completedAt == nil {
		now := service.nowUTC()
		completedAt = &now
	}

	if err := writeFinalizeRun(ctx, tx, run.id, aggregate.successCount, aggregate.failedCount, *completedAt, newStatus, resultPayload, newError); err != nil {
		return err
	}

	runSuccess := newStatus == syncRunStatusSuccess
	var stampError *string
	if !runSuccess {
		// Python: `run.error or "Sync run completed with failed units"` --
		// `or` treats None AND "" as falsy, but NOT a whitespace-only
		// string (any non-empty Python string is truthy). Deliberately not
		// the same predicate as the zero-unit branch's `.strip()` check
		// above.
		source := "Sync run completed with failed units"
		if newError != nil && *newError != "" {
			source = *newError
		}
		sanitized := sanitizeErrorText(source)
		stampError = &sanitized
	}

	if err := stampCanonicalSyncConfig(ctx, tx, run.orgID, run.integrationID, *completedAt, runSuccess, stampError, resultPayload); err != nil {
		return err
	}
	if err := observeTerminalSyncRun(ctx, tx, run, *completedAt, newStatus, aggregate.successCount, aggregate.failedCount, resultPayload, stampError); err != nil {
		return err
	}
	service.checkpointSuccessfulComputeInputs(ctx, tx, run, units, *completedAt)

	nested, err := tx.Begin(ctx)
	if err != nil {
		return ErrFinalizeSyncRunUnavailable
	}
	dispatchID := uuid.New().String()
	_, err = nested.Exec(ctx, `
INSERT INTO public.sync_run_post_dispatches (id, org_id, sync_run_id, kind, dispatched_at)
VALUES ($1::uuid, $2, $3::uuid, $4, $5)`,
		dispatchID, run.orgID, run.id, outboxKindPostSync, *completedAt)
	if err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == "23505" {
			if rollbackErr := nested.Rollback(ctx); rollbackErr != nil {
				return ErrFinalizeSyncRunUnavailable
			}
			// Already dispatched by an earlier finalize. The run-status
			// writes above still commit (matches Python: the `with session`
			// block commits on this early return too); only the once-only
			// ledger/coverage-invalidation/wakeup/counter are skipped.
			return tx.Commit(ctx)
		}
		return ErrFinalizeSyncRunUnavailable
	}
	if err := invalidateSyncCoverageForIntegration(ctx, nested, run.orgID, run.integrationID); err != nil {
		return err
	}
	if err := upsertPostSyncOutboxWakeup(ctx, nested, run.orgID, run.id, *completedAt); err != nil {
		return err
	}
	if err := nested.Commit(ctx); err != nil {
		return ErrFinalizeSyncRunUnavailable
	}

	if err := tx.Commit(ctx); err != nil {
		return ErrFinalizeSyncRunUnavailable
	}

	if isZeroUnit {
		// DECIDED above (zeroUnitReason), INCREMENTED here -- after the
		// transaction actually committed. See zero_unit_telemetry.py's
		// module docstring and sync_units.py:2244-2257: a counter bumped
		// before commit would overcount every retry of a finalization that
		// eventually succeeds once.
		provider := resolveRunProviderBestEffort(ctx, service.pool, run.integrationID)
		reason := zeroUnitReason
		if reason == "" {
			reason = zeroUnitGenericReason
		}
		if service.observer != nil {
			// Metric failures are dropped, matching every other Observe call
			// site in this tree (e.g. workgraph.PostgresStore.observeReleaseLost):
			// telemetry must never decide whether a durably-committed
			// finalization can be reported successful.
			_ = service.observer.ObserveZeroUnitFinalization(provider, reason)
		}
		service.logger.InfoContext(ctx, "finalize_sync_run.zero_unit_finalized",
			slog.String("sync_run_id", run.id),
			slog.String("provider", provider),
			slog.String("reason", reason),
		)
	}
	return nil
}

func (service *NativeFinalizeSyncRunService) nowUTC() time.Time {
	if service.now == nil {
		return time.Now().UTC()
	}
	return service.now().UTC()
}

// currentTransportReference is the shared "is this the exact durable River
// delivery that created this job" check every native coordinator entrypoint
// needs -- ported from worker_sync.py::_current_river_reference (the HTTP
// bridge's own guard) and native_post_sync.go's currentPostSyncReference,
// generalized over `kind` since finalize_sync_run needs the identical check
// for a different kind literal.
func currentTransportReference(ctx context.Context, tx pgx.Tx, args Args, kind string) (bool, error) {
	var current bool
	err := tx.QueryRow(ctx, `
SELECT EXISTS (
    SELECT 1
    FROM public.sync_dispatch_outbox AS outbox
    JOIN public.sync_dispatch_transport_routes AS route
      ON route.kind = outbox.kind
    WHERE outbox.id = $1::uuid
      AND outbox.sync_run_id = $2::uuid
      AND outbox.org_id = $3
      AND outbox.kind = $5
      AND outbox.status = 'dispatched'
      AND outbox.dispatched_transport = 'river'
      AND outbox.dispatched_route_generation = $4
      AND route.transport = 'river'
      AND route.generation = $4
      AND route.paused = false
)`, args.OutboxID(), args.SyncRunID(), args.OrganizationID(), args.RouteGeneration(), kind).Scan(&current)
	if err != nil {
		return false, ErrFinalizeSyncRunUnavailable
	}
	return current, nil
}

type finalizeSyncRun struct {
	id             string
	orgID          string
	integrationID  string
	status         string
	totalUnits     int
	completedUnits int
	failedUnits    int
	completedAt    *time.Time
	result         map[string]any
	errorText      *string
}

func loadFinalizeRun(ctx context.Context, tx pgx.Tx, args FinalizeSyncRunArgs) (*finalizeSyncRun, error) {
	var (
		run           finalizeSyncRun
		integrationID string
		completedAt   *time.Time
		resultRaw     []byte
		errorText     *string
	)
	err := tx.QueryRow(ctx, `
SELECT id::text, org_id, integration_id::text, status, total_units, completed_units,
       failed_units, completed_at, result, error
FROM public.sync_runs
WHERE id = $1::uuid AND org_id = $2`,
		args.SyncRunID(), args.OrganizationID(),
	).Scan(&run.id, &run.orgID, &integrationID, &run.status, &run.totalUnits, &run.completedUnits,
		&run.failedUnits, &completedAt, &resultRaw, &errorText)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, ErrFinalizeSyncRunUnavailable
	}
	run.integrationID = integrationID
	run.completedAt = completedAt
	run.errorText = errorText
	if len(resultRaw) > 0 {
		var decoded map[string]any
		if jsonErr := json.Unmarshal(resultRaw, &decoded); jsonErr == nil {
			run.result = decoded
		}
	}
	return &run, nil
}

type finalizeSyncRunUnit struct {
	id             string
	status         string
	provider       string
	datasetKey     string
	sourceID       string
	errorText      *string
	errorCategory  string
	processorFlags map[string]any
}

func loadFinalizeUnits(ctx context.Context, tx pgx.Tx, runID string) ([]finalizeSyncRunUnit, error) {
	rows, err := tx.Query(ctx, `
SELECT id::text, status, provider, dataset_key, source_id::text, error, result, processor_flags
FROM public.sync_run_units
WHERE sync_run_id = $1::uuid
ORDER BY id`, runID)
	if err != nil {
		return nil, ErrFinalizeSyncRunUnavailable
	}
	defer rows.Close()
	var units []finalizeSyncRunUnit
	for rows.Next() {
		var (
			unit         finalizeSyncRunUnit
			errorText    *string
			resultRaw    []byte
			processorRaw []byte
		)
		if err := rows.Scan(&unit.id, &unit.status, &unit.provider, &unit.datasetKey, &unit.sourceID,
			&errorText, &resultRaw, &processorRaw); err != nil {
			return nil, ErrFinalizeSyncRunUnavailable
		}
		unit.errorText = errorText
		if len(resultRaw) > 0 {
			var decoded map[string]any
			if json.Unmarshal(resultRaw, &decoded) == nil {
				if category, ok := stringField(decoded, "error_category"); ok {
					unit.errorCategory = category
				}
			}
		}
		if len(processorRaw) > 0 {
			var decoded map[string]any
			if json.Unmarshal(processorRaw, &decoded) == nil {
				unit.processorFlags = decoded
			}
		}
		units = append(units, unit)
	}
	if rows.Err() != nil {
		return nil, ErrFinalizeSyncRunUnavailable
	}
	return units, nil
}

type finalizeAggregate struct {
	successCount         int
	failedCount          int
	errorCategory        string
	firstFailedUnitError *string
}

// aggregateFinalizeUnits ports the aggregation block at sync_units.py:2080-
// 2116. error_category and firstFailedUnitError both resolve to the FIRST
// (by id, i.e. slice order) failed unit satisfying their own predicate --
// Python's `next(... for unit in units if ...)` walks units in the same
// ORDER BY id sequence loadFinalizeUnits already produced.
func aggregateFinalizeUnits(units []finalizeSyncRunUnit) finalizeAggregate {
	var aggregate finalizeAggregate
	for _, unit := range units {
		switch unit.status {
		case syncRunUnitStatusSuccess:
			aggregate.successCount++
		case syncRunUnitStatusFailed:
			aggregate.failedCount++
			if aggregate.errorCategory == "" && unit.errorCategory != "" {
				aggregate.errorCategory = unit.errorCategory
			}
			if aggregate.firstFailedUnitError == nil && unit.errorText != nil && *unit.errorText != "" {
				text := *unit.errorText
				aggregate.firstFailedUnitError = &text
			}
		}
	}
	return aggregate
}

// aggregateRunStatus ports sync_units.py:_aggregate_run_status verbatim,
// including the total_count==0 -> FAILED trade CHAOS-4159's docstring
// ratifies (a zero-unit run is a loud failure, never a silent success).
func aggregateRunStatus(totalCount, successCount, failedCount int) string {
	if totalCount == 0 {
		return syncRunStatusFailed
	}
	if failedCount == 0 {
		return syncRunStatusSuccess
	}
	if successCount == 0 {
		return syncRunStatusFailed
	}
	return syncRunStatusPartialFailed
}

// zeroUnitReasonFrom ports sync_units.py:_zero_unit_reason verbatim: prefers
// an explicit "reason", falls back to "error_category", else the generic
// residual. Python's predicate is `isinstance(value, str) and value.strip()`
// -- a non-string, blank, OR WHITESPACE-ONLY value counts as absent (unlike
// the plain-truthiness check the caller applies to the same map's
// "error_category" key one branch up in Finalize -- ported as two distinct
// predicates because that is what the Python source has).
func zeroUnitReasonFrom(plannerResult map[string]any) string {
	for _, key := range []string{"reason", "error_category"} {
		if value, ok := stringField(plannerResult, key); ok && strings.TrimSpace(value) != "" {
			return value
		}
	}
	return zeroUnitGenericReason
}

func stringField(source map[string]any, key string) (string, bool) {
	if source == nil {
		return "", false
	}
	raw, ok := source[key]
	if !ok {
		return "", false
	}
	text, ok := raw.(string)
	return text, ok
}

func writeFinalizeRun(
	ctx context.Context,
	tx pgx.Tx,
	runID string,
	completedUnits, failedUnits int,
	completedAt time.Time,
	status string,
	result map[string]any,
	errorText *string,
) error {
	encoded, err := json.Marshal(result)
	if err != nil {
		return ErrFinalizeSyncRunUnavailable
	}
	_, err = tx.Exec(ctx, `
UPDATE public.sync_runs
SET completed_units = $2, failed_units = $3, completed_at = $4, status = $5,
    result = $6::json, error = $7
WHERE id = $1::uuid`,
		runID, completedUnits, failedUnits, completedAt, status, encoded, errorText)
	if err != nil {
		return ErrFinalizeSyncRunUnavailable
	}
	return nil
}

// stampCanonicalSyncConfig ports trigger_routing.py::stamp_sync_run_canonical_config
// + canonical_sync_config_for_sync_run. "Canonical" is the oldest
// (created_at, id) top-level (parent_id IS NULL) SyncConfiguration for this
// integration; a run with no canonical config is a no-op, matching Python's
// `if config is None: return`.
func stampCanonicalSyncConfig(
	ctx context.Context,
	tx pgx.Tx,
	orgID, integrationID string,
	completedAt time.Time,
	success bool,
	errorText *string,
	stats map[string]any,
) error {
	var configID string
	err := tx.QueryRow(ctx, `
SELECT id::text
FROM public.sync_configurations
WHERE org_id = $1 AND integration_id = $2::uuid AND parent_id IS NULL
ORDER BY created_at ASC, id ASC
LIMIT 1`, orgID, integrationID).Scan(&configID)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil
	}
	if err != nil {
		return ErrFinalizeSyncRunUnavailable
	}
	encodedStats, err := json.Marshal(stats)
	if err != nil {
		return ErrFinalizeSyncRunUnavailable
	}
	_, err = tx.Exec(ctx, `
UPDATE public.sync_configurations
SET last_sync_at = $2, last_sync_success = $3, last_sync_error = $4, last_sync_stats = $5::json
WHERE id = $1::uuid`, configID, completedAt, success, errorText, encodedStats)
	if err != nil {
		return ErrFinalizeSyncRunUnavailable
	}
	return nil
}

// observeTerminalSyncRun ports sync_units.py::sync_observers_for_terminal_sync_run.
// Both satellite tables (backfill_jobs, job_runs) are updated by a filtered
// UPDATE rather than Python's select-then-mutate-in-Python-then-flush loop --
// same net writes, since neither loop branches per-row on anything the WHERE
// clause can't already express.
func observeTerminalSyncRun(
	ctx context.Context,
	tx pgx.Tx,
	run *finalizeSyncRun,
	completedAt time.Time,
	status string,
	completedUnits, failedUnits int,
	result map[string]any,
	errorText *string,
) error {
	success := status == syncRunStatusSuccess
	backfillStatus := "completed"
	if !success {
		backfillStatus = "failed"
	}
	marker := "sync_run:" + run.id
	if _, err := tx.Exec(ctx, `
UPDATE public.backfill_jobs
SET status = $1, total_chunks = $2, completed_chunks = $3, failed_chunks = $4,
    completed_at = $5, error_message = $6
WHERE org_id = $7 AND celery_task_id LIKE '%' || $8 || '%'`,
		backfillStatus, run.totalUnits, completedUnits, failedUnits, completedAt, errorText,
		run.orgID, marker); err != nil {
		return ErrFinalizeSyncRunUnavailable
	}

	jobRunStatus := jobRunStatusFailed
	if success {
		jobRunStatus = jobRunStatusSuccess
	}
	patch := map[string]any{
		"sync_run_status": status,
		"total_units":     run.totalUnits,
		"completed_units": completedUnits,
		"failed_units":    failedUnits,
	}
	if category, ok := stringField(result, "error_category"); ok && category != "" {
		patch["error_category"] = category
	}
	encodedPatch, err := json.Marshal(patch)
	if err != nil {
		return ErrFinalizeSyncRunUnavailable
	}
	// job_runs.result is a plain `json` column (sa.JSON(), not JSONB) --
	// the `||` shallow-merge operator only exists for jsonb, so cast
	// through jsonb and back. Patch fields win on key conflict, matching
	// Python's `{**result, **result_patch}`.
	if _, err := tx.Exec(ctx, `
UPDATE public.job_runs
SET status = $1, completed_at = $2, error = $3,
    result = (result::jsonb || $4::jsonb)::json
WHERE status IN ($5, $6) AND result->>'sync_run_id' = $7`,
		jobRunStatus, completedAt, errorText, encodedPatch,
		jobRunStatusPending, jobRunStatusRunning, run.id); err != nil {
		return ErrFinalizeSyncRunUnavailable
	}
	return nil
}

// checkpointSuccessfulComputeInputs ports
// sync_units.py::_checkpoint_successful_compute_inputs. Best-effort by
// design (Python wraps the whole loop in try/except SQLAlchemyError and only
// logs) -- a checkpoint-write failure must never fail finalize itself, so
// this reports via the service logger rather than returning an error.
func (service *NativeFinalizeSyncRunService) checkpointSuccessfulComputeInputs(
	ctx context.Context,
	tx pgx.Tx,
	run *finalizeSyncRun,
	units []finalizeSyncRunUnit,
	checkpointedAt time.Time,
) {
	for _, unit := range units {
		if unit.status != syncRunUnitStatusSuccess {
			continue
		}
		targets, ok := providersyncCapabilityLegacyTargets(unit.provider, unit.datasetKey)
		if !ok || !intersectsWorkGraphTargets(targets) {
			continue
		}
		metadata := map[string]any{
			"legacy_targets": sortedStrings(targets),
		}
		if familyDatasets := familyDatasetKeysFromFlags(unit.processorFlags); len(familyDatasets) > 0 {
			metadata["family_datasets"] = familyDatasets
		}
		encodedMetadata, err := json.Marshal(metadata)
		if err != nil {
			service.logger.WarnContext(ctx, "finalize_sync_run.compute_checkpoint_unit_failed",
				slog.String("sync_run_id", run.id), slog.String("unit_id", unit.id), slog.String("error", err.Error()))
			continue
		}
		checkpointID := uuid.New().String()
		_, err = tx.Exec(ctx, `
INSERT INTO public.sync_compute_checkpoints (
    id, org_id, sync_run_id, sync_run_unit_id, source_id, provider, dataset_key,
    compute_type, status, checkpointed_at, metadata, created_at, updated_at
) VALUES ($1::uuid, $2, $3::uuid, $4::uuid, $5::uuid, $6, $7, $8, $9, $10, $11::json, $10, $10)
ON CONFLICT ON CONSTRAINT uq_sync_compute_checkpoint_unit_type DO NOTHING`,
			checkpointID, run.orgID, run.id, unit.id, unit.sourceID, unit.provider, unit.datasetKey,
			syncComputeTypeWorkGraph, syncComputeCheckpointStatusOK, checkpointedAt, encodedMetadata)
		if err != nil {
			// A true constraint violation is handled by DO NOTHING above; any
			// other error here is the SQLAlchemyError branch Python logs and
			// continues past.
			service.logger.WarnContext(ctx, "finalize_sync_run.compute_checkpoint_unit_failed",
				slog.String("sync_run_id", run.id), slog.String("unit_id", unit.id), slog.String("error", err.Error()))
		}
	}
}

// invalidateSyncCoverageForIntegration ports
// sync_coverage.py::invalidate_sync_coverage_projection_sync (the
// integration_id selector variant -- the only one finalize_sync_run calls).
// Pure Postgres: an advisory transaction lock per resolved sync_config_id
// (serializes against a concurrent coverage rebuild) followed by a single
// invalidating UPDATE.
func invalidateSyncCoverageForIntegration(ctx context.Context, tx pgx.Tx, orgID, integrationID string) error {
	rows, err := tx.Query(ctx, `
SELECT id::text FROM public.sync_configurations
WHERE org_id = $1 AND integration_id = $2::uuid
ORDER BY id`, orgID, integrationID)
	if err != nil {
		return ErrFinalizeSyncRunUnavailable
	}
	var configIDs []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			rows.Close()
			return ErrFinalizeSyncRunUnavailable
		}
		configIDs = append(configIDs, id)
	}
	if rows.Err() != nil {
		rows.Close()
		return ErrFinalizeSyncRunUnavailable
	}
	rows.Close()
	for _, configID := range configIDs {
		lockName := "sync-coverage:" + orgID + ":" + configID
		if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1, 0))`, lockName); err != nil {
			return ErrFinalizeSyncRunUnavailable
		}
	}
	if len(configIDs) == 0 {
		return nil
	}
	if _, err := tx.Exec(ctx, `
UPDATE public.sync_coverage_projections
SET invalidated_at = now()
WHERE org_id = $1 AND sync_config_id = ANY($2::uuid[])`, orgID, configIDs); err != nil {
		return ErrFinalizeSyncRunUnavailable
	}
	return nil
}

// upsertPostSyncOutboxWakeup mirrors the exact CASE-based "feature_disabled
// wins, earliest eligible time otherwise wins" upsert shape already proven in
// production Go for kind='finalize_sync_run' (repository_postgres.go's
// upsertFinalizeSQL), and verified against the real Python
// dispatch_outbox.py::upsert_outbox_wakeup (the `terminal_denial`/
// `live_claim` case expressions there match one-for-one). Kept as its own
// literal here (kind='post_sync') rather than generalizing upsertFinalizeSQL
// across packages, to avoid touching that already-tested production query.
func upsertPostSyncOutboxWakeup(ctx context.Context, tx pgx.Tx, orgID, syncRunID string, availableAt time.Time) error {
	_, err := tx.Exec(ctx, `
INSERT INTO public.sync_dispatch_outbox (
    id, org_id, sync_run_id, kind, status, available_at, attempts,
    created_at, updated_at
) VALUES ($4::uuid, $1, $2::uuid, 'post_sync', 'pending', $3, 0, $3, $3)
ON CONFLICT (sync_run_id, kind) DO UPDATE
SET status = CASE
        WHEN public.sync_dispatch_outbox.status = 'dispatched'
         AND public.sync_dispatch_outbox.last_error = 'feature_disabled'
        THEN public.sync_dispatch_outbox.status
        ELSE 'pending'
    END,
    available_at = CASE
        WHEN public.sync_dispatch_outbox.status = 'dispatched'
         AND public.sync_dispatch_outbox.last_error = 'feature_disabled'
        THEN public.sync_dispatch_outbox.available_at
        ELSE LEAST(public.sync_dispatch_outbox.available_at, EXCLUDED.available_at)
    END,
    dispatched_at = CASE
        WHEN public.sync_dispatch_outbox.status = 'dispatched'
         AND public.sync_dispatch_outbox.last_error = 'feature_disabled'
        THEN public.sync_dispatch_outbox.dispatched_at
        ELSE NULL
    END,
    last_error = CASE
        WHEN public.sync_dispatch_outbox.status = 'dispatched'
         AND public.sync_dispatch_outbox.last_error = 'feature_disabled'
        THEN public.sync_dispatch_outbox.last_error
        ELSE NULL
    END,
    claim_token = CASE
        WHEN NOT (
            public.sync_dispatch_outbox.status = 'dispatched'
            AND public.sync_dispatch_outbox.last_error = 'feature_disabled'
        )
         AND public.sync_dispatch_outbox.claim_expires_at IS NOT NULL
         AND public.sync_dispatch_outbox.claim_expires_at > EXCLUDED.updated_at
        THEN public.sync_dispatch_outbox.claim_token
        ELSE NULL
    END,
    claim_expires_at = CASE
        WHEN NOT (
            public.sync_dispatch_outbox.status = 'dispatched'
            AND public.sync_dispatch_outbox.last_error = 'feature_disabled'
        )
         AND public.sync_dispatch_outbox.claim_expires_at IS NOT NULL
         AND public.sync_dispatch_outbox.claim_expires_at > EXCLUDED.updated_at
        THEN public.sync_dispatch_outbox.claim_expires_at
        ELSE NULL
    END,
    claim_transport = CASE
        WHEN NOT (
            public.sync_dispatch_outbox.status = 'dispatched'
            AND public.sync_dispatch_outbox.last_error = 'feature_disabled'
        )
         AND public.sync_dispatch_outbox.claim_expires_at IS NOT NULL
         AND public.sync_dispatch_outbox.claim_expires_at > EXCLUDED.updated_at
        THEN public.sync_dispatch_outbox.claim_transport
        ELSE NULL
    END,
    updated_at = EXCLUDED.updated_at`,
		orgID, syncRunID, availableAt, uuid.New().String())
	if err != nil {
		return ErrFinalizeSyncRunUnavailable
	}
	return nil
}

// resolveRunProviderBestEffort ports sync_units.py::_run_provider. Telemetry
// must never be the thing that fails a finalization (same rule Python
// states), so this runs in its own connection AFTER the finalizing
// transaction has already committed and swallows its own errors.
func resolveRunProviderBestEffort(ctx context.Context, pool *pgxpool.Pool, integrationID string) string {
	var provider string
	err := pool.QueryRow(ctx, `SELECT provider FROM public.integrations WHERE id = $1::uuid`, integrationID).Scan(&provider)
	if err != nil || provider == "" {
		return "unknown"
	}
	return provider
}

// providersyncCapabilityLegacyTargets is the same registry-owned mapping
// native_post_sync.go's loadPostSyncPlan already uses
// (providersync.Capability), standing in for
// planner.py::map_datasets_to_legacy_targets's single-pair case. "Do NOT
// hand-roll string mapping in finalize" (planner.py's own docstring) applies
// here exactly as it does in Python.
func providersyncCapabilityLegacyTargets(provider, dataset string) ([]string, bool) {
	capability, ok := providersync.Capability(provider, dataset)
	if !ok {
		return nil, false
	}
	return capability.LegacyTargets, true
}

var workGraphLegacyTargets = map[string]bool{"git": true, "prs": true, "work-items": true}

func intersectsWorkGraphTargets(targets []string) bool {
	for _, target := range targets {
		if workGraphLegacyTargets[target] {
			return true
		}
	}
	return false
}

func sortedStrings(values []string) []string {
	sorted := append([]string(nil), values...)
	sort.Strings(sorted)
	return sorted
}

// workItemFamilyDatasets/familyDatasetFlag/familyDatasetKeysFromFlags port
// src/dev_health_ops/sync/family_flags.py verbatim (WORK_ITEM_DATASETS,
// family_dataset_flag, family_dataset_keys_from_flags).
var workItemFamilyDatasets = []string{
	"work-items",
	"work-item-labels",
	"work-item-projects",
	"work-item-history",
	"work-item-comments",
}

func familyDatasetFlag(dataset string) string {
	flag := "family_dataset_"
	for _, character := range dataset {
		if character == '-' {
			flag += "_"
		} else {
			flag += string(character)
		}
	}
	return flag
}

func familyDatasetKeysFromFlags(flags map[string]any) []string {
	if flags == nil {
		return nil
	}
	var enabled []string
	for _, dataset := range workItemFamilyDatasets {
		if value, ok := flags[familyDatasetFlag(dataset)]; ok {
			if truthy, ok := value.(bool); ok && truthy {
				enabled = append(enabled, dataset)
			}
		}
	}
	return enabled
}
