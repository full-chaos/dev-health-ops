package datahealth

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/graph/model"
)

// SourceHealthStageOther is the stage of a failure whose stored stage or error
// category is not one of the named codes.
const SourceHealthStageOther = "other"

// ErrSourceHealthUnavailable marks a source-health read that could not run. An
// unreadable source is never an empty list: an empty list reads as "no sources".
var ErrSourceHealthUnavailable = errors.New("source health unavailable")

// sourceHealthStages is the closed set of stage codes the field may serve. A
// stored value outside it is served as SourceHealthStageOther, so no worker
// text can reach the field through the stage. The codes are the error
// categories the sync paths write into job_runs.result (providersync,
// syncdispatchruntime, syncreconciler, schedulerservice) and the job runtime's
// error categories.
var sourceHealthStages = map[string]bool{
	"provider_unit_retryable":          true,
	"provider_budget_contention":       true,
	"provider_rate_limited":            true,
	"provider_unit_chunk_continuation": true,
	"dependency_unavailable":           true,
	"reference_discovery_failed":       true,
	"invalid_provider_family_claim":    true,
	"feature_disabled":                 true,
	"route_unavailable":                true,
	"validation":                       true,
	"panic":                            true,
	"timeout":                          true,
	"cancelled":                        true,
	"retryable":                        true,
	"permanent":                        true,
	"terminal_domain":                  true,
	"tenant_scope":                     true,
	"budget":                           true,
	"rate_limit":                       true,
	"idempotency":                      true,
}

// sourceHealthSQL reads the same rows as connectorsSQL but selects no error
// text: whether an error exists is a boolean, and the stage and category are
// single values the Go side checks against sourceHealthStages.
const sourceHealthSQL = `
SELECT c.provider, c.name, c.sync_targets, c.last_sync_at, c.last_sync_success,
       COALESCE(c.last_sync_error, '') <> '' AS has_sync_error, c.updated_at,
       r.status, r.started_at, r.completed_at, r.stage, r.category,
       COALESCE(r.has_error, FALSE)
FROM sync_configurations c
LEFT JOIN LATERAL (
    SELECT jr.status, jr.started_at, jr.completed_at,
           jr.result->>'stage' AS stage, jr.result->>'error_category' AS category,
           COALESCE(jr.error, '') <> '' AS has_error
    FROM job_runs jr
    JOIN scheduled_jobs sj ON sj.id = jr.job_id AND sj.org_id = c.org_id
    WHERE sj.sync_config_id = c.id
    ORDER BY jr.created_at DESC
    LIMIT 1
) r ON TRUE
WHERE c.org_id = $1 AND c.is_active IS TRUE
ORDER BY c.provider, c.name`

// SourceHealth serves the member-level source health of one authorized org: one
// row per active sync configuration. lastSyncAt is the last SUCCESSFUL sync
// (null after a failed one), lastFailure is set when the latest sync failed.
// A source that never synced has neither; that is not the same as a healthy one.
func (r *Reader) SourceHealth(ctx context.Context, orgID string) ([]model.SourceHealth, error) {
	if r.Postgres == nil {
		slog.ErrorContext(ctx, "query-api: source health unavailable, no postgres reader",
			"operation", "sourceHealth")
		return nil, ErrSourceHealthUnavailable
	}
	rows, err := r.Postgres.Query(ctx, sourceHealthSQL, orgID)
	if err != nil {
		slog.ErrorContext(ctx, "query-api: source health read failed",
			"operation", "sourceHealth", "cause", pgErrorClass(err), "error", err)
		return nil, ErrSourceHealthUnavailable
	}
	defer rows.Close()

	out := []model.SourceHealth{}
	for rows.Next() {
		var (
			c                connectorRow
			hasSyncError     bool
			stage            *string
			category         *string
			hasRunError      bool
			runStatus        *int32
			runStart, runEnd *time.Time
		)
		if err := rows.Scan(&c.provider, &c.name, &c.syncTargets, &c.lastSyncAt, &c.lastSyncSuccess,
			&hasSyncError, &c.updatedAt, &runStatus, &runStart, &runEnd, &stage, &category, &hasRunError); err != nil {
			slog.ErrorContext(ctx, "query-api: source health scan failed",
				"operation", "sourceHealth", "cause", pgErrorClass(err), "error", err)
			return nil, ErrSourceHealthUnavailable
		}
		c.hasRun = runStatus != nil
		c.runStatus, c.runStarted, c.runComplete = runStatus, runStart, runEnd
		out = append(out, r.sourceHealthRow(c, hasSyncError, hasRunError, stage, category))
	}
	if err := rows.Err(); err != nil {
		slog.ErrorContext(ctx, "query-api: source health read failed",
			"operation", "sourceHealth", "cause", pgErrorClass(err), "error", err)
		return nil, ErrSourceHealthUnavailable
	}
	return out, nil
}

func (r *Reader) sourceHealthRow(c connectorRow, hasSyncError, hasRunError bool, stage, category *string) model.SourceHealth {
	row := model.SourceHealth{Provider: c.provider, Scope: connectorScope(c)}
	// last_sync_at is stamped on every finished sync, failed ones too; only a
	// sync that did not fail is a successful sync.
	if c.lastSyncAt != nil && (c.lastSyncSuccess == nil || *c.lastSyncSuccess) {
		row.LastSyncAt = utc(c.lastSyncAt)
	}

	failedRun := c.hasRun && c.runStatus != nil && (*c.runStatus == jobRunFailed || *c.runStatus == jobRunCancelled)
	lastSyncFailed := c.lastSyncSuccess != nil && !*c.lastSyncSuccess
	if !hasSyncError && !(c.hasRun && hasRunError) && !lastSyncFailed && !failedRun {
		return row
	}

	var occurred time.Time
	switch {
	case c.hasRun && c.runComplete != nil:
		occurred = *c.runComplete
	case c.hasRun && c.runStarted != nil:
		occurred = *c.runStarted
	case c.lastSyncAt != nil:
		occurred = *c.lastSyncAt
	case c.updatedAt != nil:
		occurred = *c.updatedAt
	default:
		occurred = r.now()
	}
	row.LastFailure = &model.SourceHealthFailure{OccurredAt: occurred.UTC(), Stage: sourceHealthStage(stage, category)}
	return row
}

func sourceHealthStage(stage, category *string) string {
	for _, candidate := range []*string{stage, category} {
		if candidate != nil && sourceHealthStages[*candidate] {
			return *candidate
		}
	}
	return SourceHealthStageOther
}
