package datahealth

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sort"
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

// sourceHealthSQL fetches every signal stored about each sync configuration
// of the org and decides nothing: deriveSourceHealth alone turns the signals
// into the listed rows and their values. It selects no free text: whether an
// error exists is a boolean, and the stage and categories are single values
// the Go side checks against sourceHealthStages. The run signals are the
// newest successful run and the newest failed run of the configuration's own
// jobs in the same org, each picked and served by the same time,
// sourceHealthRunTimeSQL.
var sourceHealthSQL = fmt.Sprintf(`
SELECT c.id::text, c.integration_id::text, c.parent_id IS NOT NULL, c.created_at, c.is_active IS TRUE,
       c.provider, c.sync_targets, c.last_sync_at, c.last_sync_success,
       COALESCE(c.last_sync_error, '') <> '', c.updated_at, c.last_sync_stats->>'error_category',
       EXISTS (
           SELECT 1 FROM job_runs jr
           JOIN scheduled_jobs sj ON sj.id = jr.job_id AND sj.org_id = c.org_id
           WHERE sj.sync_config_id = c.id
       ),
       ok.at, bad.at, bad.stage, bad.category
FROM sync_configurations c
LEFT JOIN LATERAL (
    SELECT %[4]s
    FROM job_runs jr
    JOIN scheduled_jobs sj ON sj.id = jr.job_id AND sj.org_id = c.org_id
    WHERE sj.sync_config_id = c.id AND jr.status = %[1]d AND COALESCE(jr.error, '') = ''
    ORDER BY %[4]s DESC, jr.id DESC
    LIMIT 1
) ok(at) ON TRUE
LEFT JOIN LATERAL (
    SELECT %[4]s, jr.result->>'stage', jr.result->>'error_category'
    FROM job_runs jr
    JOIN scheduled_jobs sj ON sj.id = jr.job_id AND sj.org_id = c.org_id
    WHERE sj.sync_config_id = c.id AND (jr.status IN (%[2]d, %[3]d) OR COALESCE(jr.error, '') <> '')
    ORDER BY %[4]s DESC, jr.id DESC
    LIMIT 1
) bad(at, stage, category) ON TRUE
WHERE c.org_id = $1
ORDER BY c.provider, c.id`, jobRunSuccess, jobRunFailed, jobRunCancelled, sourceHealthRunTimeSQL)

// sourceHealthRunTimeSQL is the time of a run: when it completed, else when it
// started, else when it was created. The newest run is the one with the newest
// such time, so overlapping runs cannot hide a newer success or failure.
const sourceHealthRunTimeSQL = `COALESCE(jr.completed_at, jr.started_at, jr.created_at)`

// sourceSignals is everything stored about one sync configuration that bears
// on its health.
type sourceSignals struct {
	id            string
	integrationID string // "" = no integration
	isChild       bool
	createdAt     *time.Time
	active        bool
	provider      string
	syncTargets   []byte

	// The config stamp, written by the sync finish on the canonical config.
	lastSyncAt      *time.Time
	lastSyncSuccess *bool
	hasSyncError    bool
	updatedAt       *time.Time
	statsCategory   *string

	hasRun    bool       // any run of the configuration's own jobs
	okRun     *runSignal // newest run that succeeded without an error
	failedRun *runSignal // newest run that failed, was cancelled or carries an error
}

type runSignal struct {
	at              time.Time // sourceHealthRunTimeSQL
	stage, category *string
}

// stampFailed: the writer sets the error text exactly when the sync failed.
func (c sourceSignals) stampFailed() bool {
	return (c.lastSyncSuccess != nil && !*c.lastSyncSuccess) || c.hasSyncError
}

func (c sourceSignals) hasOwnSignal() bool {
	return c.lastSyncAt != nil || c.lastSyncSuccess != nil || c.hasSyncError || c.hasRun
}

type listedSource struct {
	id  string
	row model.SourceHealth
}

// deriveSourceHealth is the one place that decides which configurations are
// listed and what each row says, from the signals of every source:
//
//   - lastSyncAt is the newest success of any source: a stamp that did not fail
//     (a missing success flag counts as success), or a successful run.
//   - lastFailure is the newest failure of any source (a failed stamp, a failed
//     run), only when it is newer than lastSyncAt.
//   - A configuration is listed when it is active or has a lastFailure, and it
//     can speak for itself: no integration, the canonical configuration of its
//     integration (oldest top-level one, the one the sync finish stamps), or a
//     signal of its own. A silent non-canonical configuration would read
//     "never synced" when its integration synced.
//   - An integration with an active configuration is never absent: when none of
//     its configurations is listed, its oldest active one (top-level before
//     child) is listed.
//
// The order of configs is kept.
func deriveSourceHealth(configs []sourceSignals, now time.Time) []listedSource {
	canonical := map[string]int{}
	for i, c := range configs {
		if c.integrationID == "" || c.isChild {
			continue
		}
		if j, seen := canonical[c.integrationID]; !seen || createdBefore(c, configs[j]) {
			canonical[c.integrationID] = i
		}
	}

	rows := make([]model.SourceHealth, len(configs))
	listed := make([]bool, len(configs))
	integrationListed := map[string]bool{}
	for i, c := range configs {
		rows[i] = deriveSourceRow(c, now)
		j, hasCanonical := canonical[c.integrationID]
		speaks := c.integrationID == "" || (hasCanonical && j == i) || c.hasOwnSignal()
		listed[i] = (c.active || rows[i].LastFailure != nil) && speaks
		if listed[i] && c.integrationID != "" {
			integrationListed[c.integrationID] = true
		}
	}

	fallback := map[string]int{}
	for i, c := range configs {
		if !c.active || c.integrationID == "" || integrationListed[c.integrationID] {
			continue
		}
		if j, seen := fallback[c.integrationID]; !seen || fallbackBefore(c, configs[j]) {
			fallback[c.integrationID] = i
		}
	}
	for _, i := range fallback {
		listed[i] = true
	}

	out := []listedSource{}
	for i, c := range configs {
		if listed[i] {
			out = append(out, listedSource{id: c.id, row: rows[i]})
		}
	}
	return out
}

// createdBefore orders as the sync finish picks the canonical configuration:
// created_at, then id; a missing created_at sorts last.
func createdBefore(a, b sourceSignals) bool {
	switch {
	case a.createdAt != nil && b.createdAt != nil && !a.createdAt.Equal(*b.createdAt):
		return a.createdAt.Before(*b.createdAt)
	case (a.createdAt == nil) != (b.createdAt == nil):
		return a.createdAt != nil
	default:
		return a.id < b.id
	}
}

func fallbackBefore(a, b sourceSignals) bool {
	if a.isChild != b.isChild {
		return !a.isChild
	}
	return createdBefore(a, b)
}

type failureSignal struct {
	at    time.Time
	codes []*string
}

func deriveSourceRow(c sourceSignals, now time.Time) model.SourceHealth {
	provider := sourceHealthProvider(c.provider)
	row := model.SourceHealth{Provider: provider, Scope: sourceHealthScope(provider, c.syncTargets)}

	var success *time.Time
	if c.lastSyncAt != nil && !c.stampFailed() {
		success = c.lastSyncAt
	}
	if c.okRun != nil {
		if at := c.okRun.at; success == nil || at.After(*success) {
			success = &at
		}
	}
	row.LastSyncAt = utc(success)

	// A run carries its own stage, so on equal times it is read first.
	var failures []failureSignal
	if c.failedRun != nil {
		failures = append(failures, failureSignal{at: c.failedRun.at, codes: []*string{c.failedRun.stage, c.failedRun.category}})
	}
	if c.stampFailed() {
		at := now
		switch {
		case c.lastSyncAt != nil:
			at = *c.lastSyncAt
		case c.updatedAt != nil:
			at = *c.updatedAt
		}
		failures = append(failures, failureSignal{at: at, codes: []*string{c.statsCategory}})
	}
	var current []failureSignal
	for _, f := range failures {
		if success == nil || f.at.After(*success) {
			current = append(current, f)
		}
	}
	if len(current) == 0 {
		return row
	}
	sort.SliceStable(current, func(i, j int) bool { return current[i].at.After(current[j].at) })
	var codes []*string
	for _, f := range current {
		codes = append(codes, f.codes...)
	}
	row.LastFailure = &model.SourceHealthFailure{OccurredAt: current[0].at.UTC(), Stage: sourceHealthStage(codes...)}
	return row
}

// SourceHealth serves the member-level source health of one authorized org:
// the rows deriveSourceHealth lists. A source that never synced has neither a
// time nor a failure; that is not the same as a healthy one.
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

	configs := []sourceSignals{}
	for rows.Next() {
		var (
			c                     sourceSignals
			integrationID         *string
			okAt, badAt           *time.Time
			badStage, badCategory *string
		)
		if err := rows.Scan(&c.id, &integrationID, &c.isChild, &c.createdAt, &c.active,
			&c.provider, &c.syncTargets, &c.lastSyncAt, &c.lastSyncSuccess,
			&c.hasSyncError, &c.updatedAt, &c.statsCategory, &c.hasRun,
			&okAt, &badAt, &badStage, &badCategory); err != nil {
			slog.ErrorContext(ctx, "query-api: source health scan failed",
				"operation", "sourceHealth", "cause", pgErrorClass(err), "error", err)
			return nil, ErrSourceHealthUnavailable
		}
		if integrationID != nil {
			c.integrationID = *integrationID
		}
		if okAt != nil {
			c.okRun = &runSignal{at: *okAt}
		}
		if badAt != nil {
			c.failedRun = &runSignal{at: *badAt, stage: badStage, category: badCategory}
		}
		configs = append(configs, c)
	}
	if err := rows.Err(); err != nil {
		slog.ErrorContext(ctx, "query-api: source health read failed",
			"operation", "sourceHealth", "cause", pgErrorClass(err), "error", err)
		return nil, ErrSourceHealthUnavailable
	}

	out := []model.SourceHealth{}
	for _, source := range deriveSourceHealth(configs, r.now()) {
		out = append(out, source.row)
	}
	return out, nil
}

func sourceHealthStage(candidates ...*string) string {
	for _, candidate := range candidates {
		if candidate != nil && sourceHealthStages[*candidate] {
			return *candidate
		}
	}
	return SourceHealthStageOther
}

// SourceHealthProviderOther is the provider of a configuration whose stored
// provider is not one the platform syncs.
const SourceHealthProviderOther = "other"

// sourceHealthProvider is a closed provider: a provider of the platform's
// provider registry (providersync.Capabilities), never the stored text.
func sourceHealthProvider(stored string) string {
	provider := strings.ToLower(strings.TrimSpace(stored))
	if len(providersync.Capabilities(provider)) == 0 {
		return SourceHealthProviderOther
	}
	return provider
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
	// No value, or a JSON null, names no target. Anything that is not a list
	// is unknown, never "every dataset".
	var decoded any
	if len(syncTargets) > 0 {
		if err := json.Unmarshal(syncTargets, &decoded); err != nil {
			return SourceHealthScopeOther
		}
	}
	stored, isList := decoded.([]any)
	if decoded != nil && !isList {
		return SourceHealthScopeOther
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
