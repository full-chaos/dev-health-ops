package datahealth

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"strings"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/providersync"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/graph/model"
)

// SourceHealthStageOther is the stage of a failure whose stored stage or error
// category is not one of the named codes.
const SourceHealthStageOther = "other"

// ErrSourceHealthUnavailable marks a source-health read that could not run. An
// unreadable source is never an empty list: an empty list reads as "no sources".
var ErrSourceHealthUnavailable = errors.New("source health unavailable")

// sourceHealthSQL reads the same rows as connectorsSQL but selects no free text:
// whether an error exists is a boolean, and the stage and category are single
// values the Go side checks against sourceHealthStages.
//
// Which configurations are listed:
//   - an active one, or an inactive one that carries a failure (a source that a
//     failure deactivated must not vanish from the answer);
//   - and only one that can speak for itself: the canonical configuration of
//     its integration (the one the sync finish stamps: oldest top-level
//     configuration), or one that carries its own stamp or its own run. A
//     non-canonical configuration with neither would read "never synced" when
//     its integration synced.
const sourceHealthSQL = `
SELECT c.provider, c.sync_targets, c.last_sync_at, c.last_sync_success,
       COALESCE(c.last_sync_error, '') <> '' AS has_sync_error, c.updated_at,
       c.last_sync_stats->>'error_category' AS stats_category,
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
WHERE c.org_id = $1
  AND (c.is_active IS TRUE OR c.last_sync_success IS FALSE OR COALESCE(c.last_sync_error, '') <> '')
  AND (
        c.integration_id IS NULL
        OR c.id = (
            SELECT c2.id FROM sync_configurations c2
            WHERE c2.org_id = c.org_id AND c2.integration_id = c.integration_id AND c2.parent_id IS NULL
            ORDER BY c2.created_at ASC, c2.id ASC LIMIT 1)
        OR c.last_sync_at IS NOT NULL
        OR c.last_sync_success IS NOT NULL
        OR COALESCE(c.last_sync_error, '') <> ''
        OR r.status IS NOT NULL
  )
ORDER BY c.provider, c.id`

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
			statsCategory    *string
			stage            *string
			category         *string
			hasRunError      bool
			runStatus        *int32
			runStart, runEnd *time.Time
		)
		if err := rows.Scan(&c.provider, &c.syncTargets, &c.lastSyncAt, &c.lastSyncSuccess,
			&hasSyncError, &c.updatedAt, &statsCategory, &runStatus, &runStart, &runEnd, &stage, &category, &hasRunError); err != nil {
			slog.ErrorContext(ctx, "query-api: source health scan failed",
				"operation", "sourceHealth", "cause", pgErrorClass(err), "error", err)
			return nil, ErrSourceHealthUnavailable
		}
		c.hasRun = runStatus != nil
		c.runStatus, c.runStarted, c.runComplete = runStatus, runStart, runEnd
		out = append(out, r.sourceHealthRow(c, hasSyncError, hasRunError, stage, category, statsCategory))
	}
	if err := rows.Err(); err != nil {
		slog.ErrorContext(ctx, "query-api: source health read failed",
			"operation", "sourceHealth", "cause", pgErrorClass(err), "error", err)
		return nil, ErrSourceHealthUnavailable
	}
	return out, nil
}

func (r *Reader) sourceHealthRow(c connectorRow, hasSyncError, hasRunError bool, stage, category, statsCategory *string) model.SourceHealth {
	row := model.SourceHealth{Provider: c.provider, Scope: sourceHealthScope(c.provider, c.syncTargets)}
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
	row.LastFailure = &model.SourceHealthFailure{OccurredAt: occurred.UTC(), Stage: sourceHealthStage(stage, category, statsCategory)}
	return row
}

func sourceHealthStage(candidates ...*string) string {
	for _, candidate := range candidates {
		if candidate != nil && sourceHealthStages[*candidate] {
			return *candidate
		}
	}
	return SourceHealthStageOther
}

// Scopes that name no dataset: a configuration with no sync targets syncs every
// enabled dataset; a list of targets the provider has no dataset for says nothing.
const (
	SourceHealthScopeAll   = "all"
	SourceHealthScopeOther = "other"
)

// sourceHealthScope is a closed scope: the provider's own dataset targets (git,
// prs, work-items, ...) the configuration selects, in the platform's fixed
// order, never the configuration's name nor any stored target text.
func sourceHealthScope(provider string, syncTargets []byte) string {
	var stored []any
	if len(syncTargets) > 0 {
		_ = json.Unmarshal(syncTargets, &stored)
	}
	if len(stored) == 0 {
		return SourceHealthScopeAll
	}
	selected := map[string]bool{}
	for _, target := range stored {
		if name, ok := target.(string); ok {
			selected[name] = true
		}
	}
	kept := []string{}
	for _, target := range providersync.SupportedLegacyTargets(strings.ToLower(provider)) {
		if selected[target] {
			kept = append(kept, target)
		}
	}
	if len(kept) == 0 {
		return SourceHealthScopeOther
	}
	return strings.Join(kept, ", ")
}
