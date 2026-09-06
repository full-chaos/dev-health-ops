package operational

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// ErrWebhookSyncUnroutable is wrapped into the Permanent error Work returns
// when a recognised github/gitlab webhook event cannot be resolved to a
// native, tenant-scoped SyncConfiguration -- see SyncDispatchResult.Reason
// for the specific cause (a webhook_sync_unroutable:<cause> value, also
// used as-is for the telemetry counter this path increments).
var ErrWebhookSyncUnroutable = errors.New("webhook event has no routable native sync configuration")

// githubSyncEventTypes/gitlabSyncEventTypes are the EXACT event-type sets
// Python's _process_github_event/_process_gitlab_event (system_webhooks.py)
// already route to a repo- or project-scoped sync -- ported verbatim so this
// PR neither starts syncing event types Python silently no-ops today (e.g.
// github "release", gitlab "tag_push") nor drops one Python does handle.
var githubSyncEventTypes = map[string]bool{
	"push":          true,
	"pull_request":  true,
	"issue_created": true,
	"issue_updated": true,
	"issue_closed":  true,
	"deployment":    true,
	"workflow_run":  true,
}

var gitlabSyncEventTypes = map[string]bool{
	"push":          true,
	"merge_request": true,
	"issue_created": true,
	"issue_updated": true,
	"issue_closed":  true,
	"pipeline":      true,
}

// isRepoScopedSyncEvent reports whether (provider, eventType) is routed by
// this PR into a native, tenant-scoped incremental sync via the
// scheduled_sync_occurrences/sync_manual_triggers "Sync Now" mechanism
// (CHAOS-5319) instead of the unchanged HTTP dispatch to the Python bridge.
// jira is deliberately excluded: no native work-items manual-trigger path
// exists yet (CHAOS-5319 scoping report), so every jira event keeps
// dispatching over HTTP exactly as before -- ticketed as follow-up scope,
// not silently broken here.
func isRepoScopedSyncEvent(provider, eventType string) bool {
	switch provider {
	case "github":
		return githubSyncEventTypes[eventType]
	case "gitlab":
		return gitlabSyncEventTypes[eventType]
	default:
		return false
	}
}

// SyncDispatchWriter is an OPTIONAL capability interface, the same shape as
// InstallationWriter (github_app_events.go) and
// internal/scheduler/sync/occurrence_reconciler.go's prometheusWriter --
// WebhookHandler.Work type-asserts for it before ever calling the HTTP
// dispatcher.
type SyncDispatchWriter interface {
	TriggerScopedSync(
		ctx context.Context, provider, eventType string, payload []byte, deliveredAt time.Time,
	) (SyncDispatchResult, error)
}

type SyncDispatchResult struct {
	Processed    bool
	Reason       string // "webhook_sync_unroutable:<cause>", set iff Processed is false
	OrgID        string
	SyncConfigID string
	OccurrenceID string
}

var errSyncDispatchStoreUnavailable = errors.New("sync dispatch store is unavailable")

const syncOccurrenceIdentityVersion = "sync_scheduler_occurrence_v1"

// scheduledSyncOccurrenceIdentity ports
// scheduled_sync_occurrence_identity (src/dev_health_ops/sync/
// execution_trigger.py:127) byte-for-byte: the occurrence_id must be
// bit-identical to what Python's admin "Sync Now" button would mint for the
// same (config_id, scheduled_for), since both producers feed the SAME
// scheduled_sync_occurrences table and Go's scheduler/materializer key off
// this string as the row's primary key.
func scheduledSyncOccurrenceIdentity(configID string, scheduledFor time.Time) string {
	scheduledFor = scheduledFor.UTC()
	fields := [][2]string{
		{"identity_version", syncOccurrenceIdentityVersion},
		{"config_id", configID},
		// Python: scheduled_for.strftime("%Y-%m-%dT%H:%M:%S.%f") + "000Z" --
		// %f is always 6 zero-padded digits, matching Go's ".000000".
		{"scheduled_for", scheduledFor.Format("2006-01-02T15:04:05.000000") + "000Z"},
	}
	digest := sha256.New()
	for _, field := range fields {
		name, value := []byte(field[0]), []byte(field[1])
		fmt.Fprintf(digest, "%d:", len(name))
		digest.Write(name)
		fmt.Fprintf(digest, "%d:", len(value))
		digest.Write(value)
		digest.Write([]byte("\n"))
	}
	return "sha256:" + hex.EncodeToString(digest.Sum(nil))
}

// TriggerScopedSync resolves the tenant-scoped child SyncConfiguration this
// webhook event belongs to and mints its native "Sync Now" occurrence --
// see this file's package doc comment for the full design. It never calls
// into Python and never falls back to a parent-config broad sync or a
// global token (team-lead ruling, CHAOS-5319): a repo with no matching
// child config fails loud via SyncDispatchResult.Reason instead.
func (store *PostgresStore) TriggerScopedSync(
	ctx context.Context, provider, eventType string, rawPayload []byte, deliveredAt time.Time,
) (SyncDispatchResult, error) {
	if store == nil || store.pool == nil {
		return SyncDispatchResult{}, errSyncDispatchStoreUnavailable
	}
	var payload map[string]any
	_ = json.Unmarshal(rawPayload, &payload)

	orgID, sourceID, cause, err := resolveWebhookSyncSource(ctx, store.pool, provider, payload)
	if err != nil {
		return SyncDispatchResult{}, err
	}
	if cause != "" {
		return SyncDispatchResult{Reason: "webhook_sync_unroutable:" + cause}, nil
	}

	configID, syncOptions, isActive, cause, err := lookupSyncConfig(ctx, store.pool, orgID, provider, sourceID)
	if err != nil {
		return SyncDispatchResult{}, err
	}
	if cause != "" {
		return SyncDispatchResult{Reason: "webhook_sync_unroutable:" + cause}, nil
	}

	jobID, err := ensureScheduledJobForSyncConfig(ctx, store.pool, orgID, configID, provider, syncOptions, isActive)
	if err != nil {
		return SyncDispatchResult{}, err
	}

	scheduledFor := deliveredAt.UTC()
	if scheduledFor.IsZero() {
		// Defensive only: a real webhook_deliveries row always has created_at
		// NOT NULL. A zero time would otherwise mint a fixed, collidable
		// occurrence_id across every such delivery.
		scheduledFor = time.Now().UTC()
	}
	occurrenceID := scheduledSyncOccurrenceIdentity(configID, scheduledFor)

	if err := insertScheduledSyncOccurrence(ctx, store.pool, occurrenceID, orgID, configID, jobID, scheduledFor); err != nil {
		return SyncDispatchResult{}, err
	}
	if err := insertSyncManualTrigger(ctx, store.pool, occurrenceID, sourceID); err != nil {
		return SyncDispatchResult{}, err
	}
	slog.InfoContext(ctx, "webhook sync dispatch: routed to native sync",
		"org_id", orgID, "provider", provider, "source_id", sourceID,
		"sync_config_id", configID, "occurrence_id", occurrenceID)
	return SyncDispatchResult{
		Processed: true, OrgID: orgID, SyncConfigID: configID, OccurrenceID: occurrenceID,
	}, nil
}

// resolveWebhookSyncSource resolves the org_id + integration_sources.id this
// event's repo/project belongs to. github identity is resolved via the
// installation -- PR1's github_app_installations.org_id linkage -- rather
// than the raw org_ref string on webhook_deliveries (team-lead ruling: never
// trust org_ref for tenant scoping). gitlab has no installation concept, so
// its identity is resolved directly off the payload's project id, and MUST
// match exactly one integration_sources row org-wide or it is unroutable
// (an ambiguous match across two orgs owning the same external project id
// is a correctness hazard, not a "pick one" situation).
func resolveWebhookSyncSource(
	ctx context.Context, pool *pgxpool.Pool, provider string, payload map[string]any,
) (orgID, sourceID, cause string, err error) {
	switch provider {
	case "github":
		installation, _ := payload["installation"].(map[string]any)
		instIDFloat, ok := installation["id"].(float64)
		if !ok {
			return "", "", "missing_installation_id", nil
		}
		var found bool
		orgID, found, err = lookupInstallationOrgID(ctx, pool, int64(instIDFloat))
		if err != nil {
			return "", "", "", err
		}
		if !found {
			return "", "", "unknown_installation", nil
		}
		if orgID == "" {
			return "", "", "installation_not_linked_to_org", nil
		}
		repository, _ := payload["repository"].(map[string]any)
		externalID := ""
		if idFloat, ok := repository["id"].(float64); ok {
			externalID = strconv.FormatInt(int64(idFloat), 10)
		}
		fullName, _ := repository["full_name"].(string)
		if externalID == "" && fullName == "" {
			return "", "", "missing_repo_identity", nil
		}
		sourceID, matches, err := lookupIntegrationSource(ctx, pool, provider, &orgID, externalID, fullName)
		if err != nil {
			return "", "", "", err
		}
		if matches == 0 {
			return "", "", "no_integration_source", nil
		}
		if matches > 1 {
			return "", "", "ambiguous_integration_source", nil
		}
		return orgID, sourceID, "", nil
	case "gitlab":
		project, _ := payload["project"].(map[string]any)
		externalID := ""
		switch id := project["id"].(type) {
		case float64:
			externalID = strconv.FormatInt(int64(id), 10)
		case string:
			externalID = id
		}
		fullName, _ := project["path_with_namespace"].(string)
		if externalID == "" && fullName == "" {
			return "", "", "missing_repo_identity", nil
		}
		sourceID, orgID, matches, err := lookupIntegrationSourceUnscoped(ctx, pool, provider, externalID, fullName)
		if err != nil {
			return "", "", "", err
		}
		if matches == 0 {
			return "", "", "no_integration_source", nil
		}
		if matches > 1 {
			return "", "", "ambiguous_integration_source", nil
		}
		return orgID, sourceID, "", nil
	default:
		return "", "", "unsupported_provider", nil
	}
}

// lookupInstallationOrgID distinguishes "no installation row at all"
// (found=false, cause unknown_installation) from "the installation exists
// but has never been linked to an org" (found=true, orgID="", cause
// installation_not_linked_to_org) -- two different operator-facing
// diagnoses that a collapsed empty-string return would conflate.
func lookupInstallationOrgID(ctx context.Context, pool *pgxpool.Pool, installationID int64) (orgID string, found bool, err error) {
	err = pool.QueryRow(ctx, `
SELECT COALESCE(org_id, '') FROM public.github_app_installations WHERE installation_id = $1`,
		installationID,
	).Scan(&orgID)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", false, nil
	}
	if err != nil {
		return "", false, err
	}
	return orgID, true, nil
}

// lookupIntegrationSource matches within one already-known org (github).
// external_id is preferred; full_name is tried only when external_id is
// empty (a malformed/legacy payload) or matched nothing.
func lookupIntegrationSource(
	ctx context.Context, pool *pgxpool.Pool, provider string, orgID *string, externalID, fullName string,
) (sourceID string, matches int, err error) {
	if externalID != "" {
		sourceID, matches, err = scanIntegrationSourceIDs(ctx, pool, `
SELECT id::text FROM public.integration_sources
WHERE provider = $1 AND org_id = $2 AND external_id = $3 LIMIT 2`, provider, *orgID, externalID)
		if err != nil || matches > 0 {
			return sourceID, matches, err
		}
	}
	if fullName == "" {
		return "", 0, nil
	}
	return scanIntegrationSourceIDs(ctx, pool, `
SELECT id::text FROM public.integration_sources
WHERE provider = $1 AND org_id = $2 AND full_name = $3 LIMIT 2`, provider, *orgID, fullName)
}

// lookupIntegrationSourceUnscoped matches org-wide (gitlab has no
// installation-derived org hint) -- see resolveWebhookSyncSource's doc
// comment on why >1 match must fail loud rather than pick one.
func lookupIntegrationSourceUnscoped(
	ctx context.Context, pool *pgxpool.Pool, provider, externalID, fullName string,
) (sourceID, orgID string, matches int, err error) {
	scan := func(query, value string) (string, string, int, error) {
		rows, err := pool.Query(ctx, query, provider, value)
		if err != nil {
			return "", "", 0, err
		}
		defer rows.Close()
		var id, org string
		count := 0
		for rows.Next() {
			count++
			if count == 1 {
				if scanErr := rows.Scan(&id, &org); scanErr != nil {
					return "", "", 0, scanErr
				}
			}
		}
		return id, org, count, rows.Err()
	}
	if externalID != "" {
		id, org, count, err := scan(`
SELECT id::text, org_id FROM public.integration_sources
WHERE provider = $1 AND external_id = $2 LIMIT 2`, externalID)
		if err != nil || count > 0 {
			return id, org, count, err
		}
	}
	if fullName == "" {
		return "", "", 0, nil
	}
	return scan(`
SELECT id::text, org_id FROM public.integration_sources
WHERE provider = $1 AND full_name = $2 LIMIT 2`, fullName)
}

func scanIntegrationSourceIDs(ctx context.Context, pool *pgxpool.Pool, query string, args ...any) (string, int, error) {
	rows, err := pool.Query(ctx, query, args...)
	if err != nil {
		return "", 0, err
	}
	defer rows.Close()
	var id string
	count := 0
	for rows.Next() {
		count++
		if count == 1 {
			if scanErr := rows.Scan(&id); scanErr != nil {
				return "", 0, scanErr
			}
		}
	}
	return id, count, rows.Err()
}

// lookupSyncConfig resolves the (at most one) active SyncConfiguration this
// webhook event's source should trigger. Preference order, ported from
// plan_request_for_config's own precedence (src/dev_health_ops/sync/
// trigger_routing.py:101): (1) a CHILD config scoped directly to sourceID
// (sync_configurations.source_id = sourceID) when one exists; (2) otherwise
// the org's single planner-managed PARENT config for this provider
// (sync_configurations.planner_managed = true). More than one active match
// at EITHER step is a data-hygiene issue this PR treats the same as none:
// fail loud rather than guess which one the webhook meant.
//
// CHAOS-5319 r1 corrective (2026-09-06): the child branch is verified LIVE
// against the local fixture stack to be the RARE case today -- 0 of 8
// sampled real (org_id, provider) sources had one, all resolve only via
// their org's parent config. Deliberately does NOT read
// integration_sources.metadata->>'planner_managed_sync_config_id' as a
// shortcut to the parent: verified live that this pointer is STALE for at
// least one real org/provider (github, org 70d529e0 -- pointed at a
// sync_configurations id that does not exist), while the direct
// org_id+provider+planner_managed+is_active query below resolved correctly
// for every sampled source. The pointer is a display/legacy convenience
// column, not a reliable FK; querying sync_configurations directly is.
//
// The parent query also requires source_id IS NULL: a row can in principle
// be BOTH planner_managed and scoped to one source (a data shape this PR
// has not observed live but does not rule out), and such a row is a CHILD
// config, not the org-wide parent -- the child branch above is where it
// belongs, not silently matched here too.
func lookupSyncConfig(
	ctx context.Context, pool *pgxpool.Pool, orgID, provider, sourceID string,
) (configID string, syncOptions map[string]any, isActive bool, cause string, err error) {
	configID, syncOptions, isActive, matches, err := scanOneSyncConfig(ctx, pool, `
SELECT id::text, sync_options, is_active FROM public.sync_configurations
WHERE org_id = $1 AND provider = $2 AND source_id = $3 AND is_active = true
LIMIT 2`, orgID, provider, sourceID)
	if err != nil {
		return "", nil, false, "", err
	}
	if matches == 1 {
		return configID, syncOptions, isActive, "", nil
	}
	if matches > 1 {
		return "", nil, false, "ambiguous_sync_config", nil
	}
	// No child config scoped to this source -- the NORMAL case today, not an
	// error condition; logged at debug so an operator tracing a specific
	// delivery can still see which branch was taken.
	slog.DebugContext(ctx, "webhook sync dispatch: no child sync config for source, trying org parent config",
		"org_id", orgID, "provider", provider, "source_id", sourceID)

	configID, syncOptions, isActive, matches, err = scanOneSyncConfig(ctx, pool, `
SELECT id::text, sync_options, is_active FROM public.sync_configurations
WHERE org_id = $1 AND provider = $2 AND source_id IS NULL AND planner_managed = true AND is_active = true
LIMIT 2`, orgID, provider)
	if err != nil {
		return "", nil, false, "", err
	}
	if matches == 0 {
		return "", nil, false, "no_sync_configuration", nil
	}
	if matches > 1 {
		return "", nil, false, "ambiguous_sync_config", nil
	}
	return configID, syncOptions, isActive, "", nil
}

// scanOneSyncConfig runs query (expected to select id::text, sync_options,
// is_active, LIMIT 2) and reports how many rows matched -- 0/1/2+, never
// picking one arbitrarily out of a 2+ result, so the caller can fail loud on
// ambiguity instead of silently taking the first row Postgres happened to
// return.
func scanOneSyncConfig(
	ctx context.Context, pool *pgxpool.Pool, query string, args ...any,
) (configID string, syncOptions map[string]any, isActive bool, matches int, err error) {
	rows, err := pool.Query(ctx, query, args...)
	if err != nil {
		return "", nil, false, 0, err
	}
	defer rows.Close()
	var rawOptions []byte
	count := 0
	for rows.Next() {
		count++
		if count == 1 {
			if scanErr := rows.Scan(&configID, &rawOptions, &isActive); scanErr != nil {
				return "", nil, false, 0, scanErr
			}
		}
	}
	if err := rows.Err(); err != nil {
		return "", nil, false, 0, err
	}
	if count != 1 {
		return "", nil, false, count, nil
	}
	_ = json.Unmarshal(rawOptions, &syncOptions)
	if syncOptions == nil {
		syncOptions = map[string]any{}
	}
	return configID, syncOptions, isActive, count, nil
}

// ensureScheduledJobForSyncConfig ports _ensure_scheduled_job_for_config
// (src/dev_health_ops/sync/execution_trigger.py:234) verbatim, including its
// ACTIVE-only-with-an-explicit-cron status rule: a webhook-created child
// config almost never carries sync_options.schedule_cron, so the freshly
// minted job lands PAUSED -- deliberately, matching Python, so wiring a
// webhook never silently starts a new recurring hourly cron sync. This is
// safe for our own occurrence because a sync_manual_triggers-backed
// occurrence bypasses the job-status eligibility gate entirely
// (internal/scheduler/sync/materializer.go:638, `manualTrigger == nil &&
// occurrence.JobStatus != 0`).
func ensureScheduledJobForSyncConfig(
	ctx context.Context, pool *pgxpool.Pool, orgID, configID, provider string, syncOptions map[string]any, isActive bool,
) (string, error) {
	var jobID string
	err := pool.QueryRow(ctx, `
SELECT id::text FROM public.scheduled_jobs
WHERE org_id = $1 AND sync_config_id = $2 AND job_type = 'sync'`, orgID, configID,
	).Scan(&jobID)
	if err == nil {
		return jobID, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return "", err
	}

	explicitCron, _ := syncOptions["schedule_cron"].(string)
	cron := explicitCron
	if cron == "" {
		cron = "0 * * * *"
	}
	tz, _ := syncOptions["timezone"].(string)
	if tz == "" {
		tz = "UTC"
	}
	status := jobStatusPaused
	if isActive && explicitCron != "" {
		status = jobStatusActive
	}
	jobConfig, err := json.Marshal(map[string]string{"provider": provider, "sync_config_id": configID})
	if err != nil {
		return "", err
	}

	err = pool.QueryRow(ctx, `
INSERT INTO public.scheduled_jobs
	(id, org_id, name, job_type, provider, schedule_cron, timezone, job_config, sync_config_id, status)
VALUES (gen_random_uuid(), $1, $2, 'sync', $3, $4, $5, $6::jsonb, $7, $8)
ON CONFLICT (org_id, sync_config_id, job_type) DO NOTHING
RETURNING id::text`,
		orgID, fmt.Sprintf("sync-config-%s", configID), provider, cron, tz, jobConfig, configID, status,
	).Scan(&jobID)
	if err == nil {
		return jobID, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return "", err
	}
	// A concurrent webhook delivery for the same never-before-scheduled
	// config won the INSERT race -- read back the row it created.
	if err := pool.QueryRow(ctx, `
SELECT id::text FROM public.scheduled_jobs
WHERE org_id = $1 AND sync_config_id = $2 AND job_type = 'sync'`, orgID, configID,
	).Scan(&jobID); err != nil {
		return "", err
	}
	return jobID, nil
}

const (
	jobStatusActive = 0 // JobStatus.ACTIVE (src/dev_health_ops/models/settings.py:64)
	jobStatusPaused = 1 // JobStatus.PAUSED
)

// insertScheduledSyncOccurrence is idempotent on retry: occurrence_id is a
// pure function of (configID, scheduledFor), and scheduledFor is this
// delivery's own stable created_at, so ON CONFLICT DO NOTHING here means "a
// prior attempt at this exact delivery already scheduled it" rather than a
// collision between two different deliveries.
func insertScheduledSyncOccurrence(
	ctx context.Context, pool *pgxpool.Pool, occurrenceID, orgID, configID, jobID string, scheduledFor time.Time,
) error {
	_, err := pool.Exec(ctx, `
INSERT INTO public.scheduled_sync_occurrences
	(occurrence_id, identity_version, org_id, sync_config_id, scheduled_job_id, scheduled_for)
VALUES ($1, $2, $3, $4, $5, $6)
ON CONFLICT (occurrence_id) DO NOTHING`,
		occurrenceID, syncOccurrenceIdentityVersion, orgID, configID, jobID, scheduledFor,
	)
	return err
}

// insertSyncManualTrigger ports the SyncManualTrigger row
// _create_go_manual_sync_execution_trigger inserts (execution_trigger.py:
// 502): mode "incremental", since/before/dataset_keys all NULL (NULL
// dataset_keys means "all enabled datasets", the column's own documented
// fallback -- a deliberate simplification vs. Python's per-event-type
// sync_git/sync_prs/... flags; see this PR's RISK-NOTES), triggered_by
// "manual" (the CHECK constraint only allows 'manual'/'backfill' -- there is
// no third "webhook" value to carry this trigger's true origin, so "manual"
// is the closest fit, same as every other non-cron, non-backfill trigger
// path today), and source_ids scoped to exactly this repo's source.
func insertSyncManualTrigger(ctx context.Context, pool *pgxpool.Pool, occurrenceID, sourceID string) error {
	_, err := pool.Exec(ctx, `
INSERT INTO public.sync_manual_triggers (occurrence_id, mode, source_ids, triggered_by)
VALUES ($1, 'incremental', $2, 'manual')
ON CONFLICT (occurrence_id) DO NOTHING`,
		occurrenceID, []string{sourceID},
	)
	return err
}
