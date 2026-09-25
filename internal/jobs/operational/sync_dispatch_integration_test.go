//go:build integration

package operational

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/pgmigrate"
	schedsync "github.com/full-chaos/dev-health-ops/internal/scheduler/sync"
	postgresstore "github.com/full-chaos/dev-health-ops/internal/storage/postgres"
	riverstore "github.com/full-chaos/dev-health-ops/internal/storage/river"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

// applySyncDispatchSchema builds the real schema -- the pgmigrate baseline
// and chain, the same schema the Alembic heads produce -- so every NOT NULL
// column without a server default, and every foreign key, is enforced
// exactly as in a deployment. A hand-made subset hid that the scheduled
// job insert omitted created_at/updated_at (CHAOS-6652).
func applySyncDispatchSchema(ctx context.Context, t *testing.T, pool *pgxpool.Pool) {
	t.Helper()
	conn, err := pgx.Connect(ctx, pool.Config().ConnString())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close(context.Background())
	baseline, err := pgmigrate.LoadBaseline()
	if err != nil {
		t.Fatal(err)
	}
	chain, err := pgmigrate.LoadChain()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := pgmigrate.Upgrade(ctx, conn, baseline, chain); err != nil {
		t.Fatalf("apply the schema: %v", err)
	}
}

func insertIntegrationSource(
	ctx context.Context, t *testing.T, pool *pgxpool.Pool, orgID, provider, externalID, fullName string,
) string {
	t.Helper()
	var integrationID string
	now := time.Now().UTC()
	if err := pool.QueryRow(ctx, `
INSERT INTO public.integrations (id, org_id, provider, name, config, is_active, created_at, updated_at)
VALUES (gen_random_uuid(), $1, $2, $3, '{}', true, $4, $4) RETURNING id::text`,
		orgID, provider, "integration-"+externalID+"-"+fullName, now,
	).Scan(&integrationID); err != nil {
		t.Fatalf("insert integration: %v", err)
	}
	var id string
	err := pool.QueryRow(ctx, `
INSERT INTO public.integration_sources (id, org_id, integration_id, provider, source_type, external_id, name, full_name, metadata,
	is_enabled, discovered_at, last_seen_at)
VALUES (gen_random_uuid(), $1, $2, $3, 'repository', $4, $5, $5, '{}', true, $6, $6) RETURNING id::text`,
		orgID, integrationID, provider, externalID, fullName, now,
	).Scan(&id)
	if err != nil {
		t.Fatalf("insert integration source: %v", err)
	}
	return id
}

func insertSyncConfiguration(
	ctx context.Context, t *testing.T, pool *pgxpool.Pool, orgID, provider, sourceID string, syncOptions string,
) string {
	t.Helper()
	var id string
	err := pool.QueryRow(ctx, `
INSERT INTO public.sync_configurations (id, org_id, name, provider, source_id, sync_targets, sync_options, is_active,
	planner_managed, created_at, updated_at)
VALUES (gen_random_uuid(), $1, $2, $3, $4, '[]', $5::json, true, false, $6, $6) RETURNING id::text`,
		orgID, "webhook-child-"+sourceID, provider, sourceID, syncOptions, time.Now().UTC(),
	).Scan(&id)
	if err != nil {
		t.Fatalf("insert sync configuration: %v", err)
	}
	return id
}

func insertParentSyncConfig(
	ctx context.Context, t *testing.T, pool *pgxpool.Pool, orgID, provider string, isActive bool,
) string {
	t.Helper()
	var id string
	err := pool.QueryRow(ctx, `
INSERT INTO public.sync_configurations (id, org_id, name, provider, sync_targets, planner_managed, is_active, sync_options,
	created_at, updated_at)
VALUES (gen_random_uuid(), $1, $2 || '-' || gen_random_uuid()::text, $3, '[]', TRUE, $4, '{}', $5, $5) RETURNING id::text`,
		orgID, "planner-"+provider, provider, isActive, time.Now().UTC(),
	).Scan(&id)
	if err != nil {
		t.Fatalf("insert parent sync configuration: %v", err)
	}
	return id
}

func insertGithubInstallation(ctx context.Context, t *testing.T, pool *pgxpool.Pool, installationID int64, orgID string) {
	t.Helper()
	now := time.Now().UTC()
	if _, err := pool.Exec(ctx, `
INSERT INTO public.github_app_installations (id, installation_id, org_id, created_at, updated_at)
VALUES (gen_random_uuid(), $1, $2, $3, $3)`, installationID, orgID, now); err != nil {
		t.Fatalf("insert github app installation: %v", err)
	}
}

func countRows(ctx context.Context, t *testing.T, pool *pgxpool.Pool, table string) int {
	t.Helper()
	var count int
	if err := pool.QueryRow(ctx, "SELECT count(*) FROM "+table).Scan(&count); err != nil {
		t.Fatalf("count %s: %v", table, err)
	}
	return count
}

func TestTriggerScopedSyncGithubCreatesOccurrenceAndManualTrigger(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	pool, domain, coordinator := startSyncDispatchDatabase(ctx, t)
	store := &PostgresStore{pool: domain}

	insertGithubInstallation(ctx, t, pool, 555, "org-1")
	sourceID := insertIntegrationSource(ctx, t, pool, "org-1", "github", "42", "full-chaos/dev-health")
	configID := insertSyncConfiguration(ctx, t, pool, "org-1", "github", sourceID, `{}`)

	deliveredAt := time.Date(2026, 9, 6, 12, 0, 0, 0, time.UTC)
	payload := []byte(`{"installation":{"id":555},"repository":{"id":42,"full_name":"full-chaos/dev-health"}}`)
	result, err := triggerAndMint(ctx, t, store, coordinator, "github", "push", payload, deliveredAt)
	if err != nil {
		t.Fatal(err)
	}
	if !result.Processed || result.SyncConfigID != configID || result.OrgID != "org-1" {
		t.Fatalf("result = %+v", result)
	}

	var identityVersion, orgID, syncConfigID string
	var scheduledFor time.Time
	if err := pool.QueryRow(ctx, `
SELECT identity_version, org_id, sync_config_id::text, scheduled_for
FROM public.scheduled_sync_occurrences WHERE occurrence_id = $1`, result.OccurrenceID,
	).Scan(&identityVersion, &orgID, &syncConfigID, &scheduledFor); err != nil {
		t.Fatalf("read occurrence: %v", err)
	}
	if identityVersion != "sync_scheduler_occurrence_v1" || orgID != "org-1" || syncConfigID != configID {
		t.Fatalf("occurrence = version=%q org=%q config=%q", identityVersion, orgID, syncConfigID)
	}
	if !scheduledFor.Equal(deliveredAt) {
		t.Fatalf("scheduled_for = %s, want the delivery's own created_at %s", scheduledFor, deliveredAt)
	}

	var mode, triggeredBy string
	var sourceIDs []string
	if err := pool.QueryRow(ctx, `
SELECT mode, triggered_by, source_ids FROM public.sync_manual_triggers WHERE occurrence_id = $1`, result.OccurrenceID,
	).Scan(&mode, &triggeredBy, &sourceIDs); err != nil {
		t.Fatalf("read manual trigger: %v", err)
	}
	if mode != "incremental" || triggeredBy != "manual" || len(sourceIDs) != 1 || sourceIDs[0] != sourceID {
		t.Fatalf("manual trigger = mode=%q triggeredBy=%q sourceIDs=%v", mode, triggeredBy, sourceIDs)
	}

	var jobStatus int
	if err := pool.QueryRow(ctx, `SELECT status FROM public.scheduled_jobs WHERE sync_config_id = $1`, configID).Scan(&jobStatus); err != nil {
		t.Fatalf("read scheduled job: %v", err)
	}
	if jobStatus != 1 {
		t.Fatalf("a freshly created job with no explicit schedule_cron must be PAUSED(1), got %d", jobStatus)
	}
	// The row carries every column the ScheduledJob model fills, as
	// upsertScheduledJob writes them: job_config in json.dumps form, the
	// counters zero, both timestamps set.
	var jobConfig string
	var running bool
	var runs, failures int
	var createdSet, updatedEqual bool
	if err := pool.QueryRow(ctx, `SELECT job_config::text, is_running, run_count, failure_count,
created_at IS NOT NULL, created_at = updated_at FROM public.scheduled_jobs WHERE sync_config_id = $1`, configID).
		Scan(&jobConfig, &running, &runs, &failures, &createdSet, &updatedEqual); err != nil {
		t.Fatal(err)
	}
	wantConfig := `{"provider": "github", "sync_config_id": "` + configID + `"}`
	if jobConfig != wantConfig || running || runs != 0 || failures != 0 || !createdSet || !updatedEqual {
		t.Fatalf("scheduled job row = config %s running %v runs %d failures %d created %v updated=created %v; want config %s, idle, zero counters, timestamps set",
			jobConfig, running, runs, failures, createdSet, updatedEqual, wantConfig)
	}
}

func TestTriggerScopedSyncGitlabCreatesOccurrenceAndManualTrigger(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	pool, domain, coordinator := startSyncDispatchDatabase(ctx, t)
	store := &PostgresStore{pool: domain}

	sourceID := insertIntegrationSource(ctx, t, pool, "org-2", "gitlab", "99", "group/project")
	configID := insertSyncConfiguration(ctx, t, pool, "org-2", "gitlab", sourceID, `{}`)

	deliveredAt := time.Date(2026, 9, 6, 12, 0, 0, 0, time.UTC)
	payload := []byte(`{"project":{"id":99,"path_with_namespace":"group/project"}}`)
	result, err := triggerAndMint(ctx, t, store, coordinator, "gitlab", "merge_request", payload, deliveredAt)
	if err != nil {
		t.Fatal(err)
	}
	if !result.Processed || result.SyncConfigID != configID || result.OrgID != "org-2" {
		t.Fatalf("result = %+v", result)
	}
	if countRows(ctx, t, pool, "public.sync_manual_triggers") != 1 {
		t.Fatal("expected exactly one sync_manual_triggers row")
	}
}

// TestTriggerScopedSyncNoConfigAtAllIsUnroutable covers the source having
// neither a child config NOR an org parent config -- genuinely nothing to
// route to. See TestTriggerScopedSync{Github,Gitlab}RoutesViaParentConfig
// below for the child-absent-but-parent-present case, which is the NORMAL
// shape in production (verified live: 0/8 sampled real sources have a child
// config, all resolve via their org's parent -- CHAOS-5319 r1 corrective).
func TestTriggerScopedSyncNoConfigAtAllIsUnroutable(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	pool, domain, coordinator := startSyncDispatchDatabase(ctx, t)
	store := &PostgresStore{pool: domain}

	insertGithubInstallation(ctx, t, pool, 555, "org-1")
	// An IntegrationSource exists (the repo is discovered) but NO
	// SyncConfiguration at all -- neither a child scoped to it nor an org
	// parent config for this provider.
	insertIntegrationSource(ctx, t, pool, "org-1", "github", "42", "full-chaos/dev-health")

	payload := []byte(`{"installation":{"id":555},"repository":{"id":42,"full_name":"full-chaos/dev-health"}}`)
	result, err := triggerAndMint(ctx, t, store, coordinator, "github", "push", payload, time.Now().UTC())
	if err != nil {
		t.Fatal(err)
	}
	if result.Processed || result.Reason != "webhook_sync_unroutable:no_sync_configuration" {
		t.Fatalf("result = %+v", result)
	}
	if countRows(ctx, t, pool, "public.scheduled_sync_occurrences") != 0 {
		t.Fatal("an unroutable event must never write an occurrence row")
	}
}

// TestTriggerScopedSyncGithubRoutesViaParentConfig covers the PRODUCTION-
// SHAPE fixture: a source with no child sync_configurations row at all,
// routing via the org's planner-managed parent config instead. This is the
// CHAOS-5319 r1 corrective's own repro shape (0/8 real sampled sources had a
// child config).
func TestTriggerScopedSyncGithubRoutesViaParentConfig(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	pool, domain, coordinator := startSyncDispatchDatabase(ctx, t)
	store := &PostgresStore{pool: domain}

	insertGithubInstallation(ctx, t, pool, 555, "org-1")
	insertIntegrationSource(ctx, t, pool, "org-1", "github", "42", "full-chaos/dev-health")
	parentConfigID := insertParentSyncConfig(ctx, t, pool, "org-1", "github", true)

	payload := []byte(`{"installation":{"id":555},"repository":{"id":42,"full_name":"full-chaos/dev-health"}}`)
	result, err := triggerAndMint(ctx, t, store, coordinator, "github", "push", payload, time.Now().UTC())
	if err != nil {
		t.Fatal(err)
	}
	if !result.Processed || result.SyncConfigID != parentConfigID {
		t.Fatalf("result = %+v, want processed against the parent config %q", result, parentConfigID)
	}
}

// TestTriggerScopedSyncGitlabRoutesViaParentConfig is gitlab's twin of
// TestTriggerScopedSyncGithubRoutesViaParentConfig.
func TestTriggerScopedSyncGitlabRoutesViaParentConfig(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	pool, domain, coordinator := startSyncDispatchDatabase(ctx, t)
	store := &PostgresStore{pool: domain}

	insertIntegrationSource(ctx, t, pool, "org-2", "gitlab", "99", "group/project")
	parentConfigID := insertParentSyncConfig(ctx, t, pool, "org-2", "gitlab", true)

	payload := []byte(`{"project":{"id":99,"path_with_namespace":"group/project"}}`)
	result, err := triggerAndMint(ctx, t, store, coordinator, "gitlab", "merge_request", payload, time.Now().UTC())
	if err != nil {
		t.Fatal(err)
	}
	if !result.Processed || result.SyncConfigID != parentConfigID {
		t.Fatalf("result = %+v, want processed against the parent config %q", result, parentConfigID)
	}
}

// TestTriggerScopedSyncInactiveParentConfigIsUnroutable covers a source with
// no child config whose org parent config exists but is_active=false -- the
// exact shape found live for org 70d529e0's gitlab parent config during the
// CHAOS-5319 r1 corrective's repro.
func TestTriggerScopedSyncInactiveParentConfigIsUnroutable(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	pool, domain, coordinator := startSyncDispatchDatabase(ctx, t)
	store := &PostgresStore{pool: domain}

	insertIntegrationSource(ctx, t, pool, "org-2", "gitlab", "99", "group/project")
	insertParentSyncConfig(ctx, t, pool, "org-2", "gitlab", false)

	payload := []byte(`{"project":{"id":99,"path_with_namespace":"group/project"}}`)
	result, err := triggerAndMint(ctx, t, store, coordinator, "gitlab", "merge_request", payload, time.Now().UTC())
	if err != nil {
		t.Fatal(err)
	}
	if result.Processed || result.Reason != "webhook_sync_unroutable:no_sync_configuration" {
		t.Fatalf("result = %+v, want unroutable (an inactive parent does not count as a match)", result)
	}
}

// TestTriggerScopedSyncTwoActiveParentConfigsIsAmbiguous covers a data-
// hygiene edge case (two active planner-managed configs for the same
// org+provider) -- treated the same as the existing ambiguous-source cases:
// fail loud rather than pick one.
func TestTriggerScopedSyncTwoActiveParentConfigsIsAmbiguous(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	pool, domain, coordinator := startSyncDispatchDatabase(ctx, t)
	store := &PostgresStore{pool: domain}

	insertIntegrationSource(ctx, t, pool, "org-2", "gitlab", "99", "group/project")
	insertParentSyncConfig(ctx, t, pool, "org-2", "gitlab", true)
	insertParentSyncConfig(ctx, t, pool, "org-2", "gitlab", true)

	payload := []byte(`{"project":{"id":99,"path_with_namespace":"group/project"}}`)
	result, err := triggerAndMint(ctx, t, store, coordinator, "gitlab", "merge_request", payload, time.Now().UTC())
	if err != nil {
		t.Fatal(err)
	}
	if result.Processed || result.Reason != "webhook_sync_unroutable:ambiguous_sync_config" {
		t.Fatalf("result = %+v", result)
	}
}

// TestTriggerScopedSyncJiraRoutesViaParentConfig covers CHAOS-5320's native
// jira path -- resolved the same way as TestTriggerScopedSyncGithubRoutes
// ViaParentConfig/...Gitlab..., verified live against the local fixture
// stack that integration_sources.external_id for provider='jira' is exactly
// the project key a real webhook payload's issue.fields.project.key
// carries.
func TestTriggerScopedSyncJiraRoutesViaParentConfig(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	pool, domain, coordinator := startSyncDispatchDatabase(ctx, t)
	store := &PostgresStore{pool: domain}

	insertIntegrationSource(ctx, t, pool, "org-3", "jira", "CHAOS", "Full Chaos")
	parentConfigID := insertParentSyncConfig(ctx, t, pool, "org-3", "jira", true)

	payload := []byte(`{"issue":{"key":"CHAOS-1","fields":{"project":{"key":"CHAOS","name":"Full Chaos"}}}}`)
	result, err := triggerAndMint(ctx, t, store, coordinator, "jira", "issue_updated", payload, time.Now().UTC())
	if err != nil {
		t.Fatal(err)
	}
	if !result.Processed || result.SyncConfigID != parentConfigID || result.OrgID != "org-3" {
		t.Fatalf("result = %+v, want processed against the parent config %q", result, parentConfigID)
	}
}

// TestTriggerScopedSyncJiraAmbiguousIntegrationSourceIsUnroutable covers two
// different orgs both naming a jira project with the same key -- the exact
// collision resolveWebhookSyncSource's doc comment names as the reason jira
// identity must never be trusted at face value (the router's own
// `org_id = project_key` extraction is a raw, untrusted hint here too).
func TestTriggerScopedSyncJiraAmbiguousIntegrationSourceIsUnroutable(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	pool, domain, coordinator := startSyncDispatchDatabase(ctx, t)
	store := &PostgresStore{pool: domain}

	insertIntegrationSource(ctx, t, pool, "org-a", "jira", "OPS", "Ops Team A")
	insertIntegrationSource(ctx, t, pool, "org-b", "jira", "OPS", "Ops Team B")

	payload := []byte(`{"issue":{"key":"OPS-1","fields":{"project":{"key":"OPS","name":"Ops Team A"}}}}`)
	result, err := triggerAndMint(ctx, t, store, coordinator, "jira", "issue_created", payload, time.Now().UTC())
	if err != nil {
		t.Fatal(err)
	}
	if result.Processed || result.Reason != "webhook_sync_unroutable:ambiguous_integration_source" {
		t.Fatalf("result = %+v", result)
	}
}

// TestTriggerScopedSyncJiraMissingProjectKeyIsUnroutable covers a payload
// shape with no resolvable project identity at all -- resolveWebhookSyncSource
// must fail loud (missing_repo_identity), never guess or fall back to a
// broad/unscoped sync.
func TestTriggerScopedSyncJiraMissingProjectKeyIsUnroutable(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	_, domain, coordinator := startSyncDispatchDatabase(ctx, t)
	store := &PostgresStore{pool: domain}

	payload := []byte(`{"issue":{"key":"CHAOS-1","fields":{}}}`)
	result, err := triggerAndMint(ctx, t, store, coordinator, "jira", "issue_updated", payload, time.Now().UTC())
	if err != nil {
		t.Fatal(err)
	}
	if result.Processed || result.Reason != "webhook_sync_unroutable:missing_repo_identity" {
		t.Fatalf("result = %+v", result)
	}
}

// TestTriggerScopedSyncChildConfigWinsOverParent covers the preference order
// itself: when BOTH a child config and an org parent config exist, the child
// wins (existing TestTriggerScopedSyncGithubCreatesOccurrenceAndManualTrigger
// already exercises "child present, no parent"; this adds "both present").
func TestTriggerScopedSyncChildConfigWinsOverParent(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	pool, domain, coordinator := startSyncDispatchDatabase(ctx, t)
	store := &PostgresStore{pool: domain}

	insertGithubInstallation(ctx, t, pool, 555, "org-1")
	sourceID := insertIntegrationSource(ctx, t, pool, "org-1", "github", "42", "full-chaos/dev-health")
	childConfigID := insertSyncConfiguration(ctx, t, pool, "org-1", "github", sourceID, `{}`)
	insertParentSyncConfig(ctx, t, pool, "org-1", "github", true)

	payload := []byte(`{"installation":{"id":555},"repository":{"id":42,"full_name":"full-chaos/dev-health"}}`)
	result, err := triggerAndMint(ctx, t, store, coordinator, "github", "push", payload, time.Now().UTC())
	if err != nil {
		t.Fatal(err)
	}
	if !result.Processed || result.SyncConfigID != childConfigID {
		t.Fatalf("result = %+v, want the CHILD config %q to win over the parent", result, childConfigID)
	}
}

func TestTriggerScopedSyncUnknownInstallationIsUnroutable(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	_, domain, coordinator := startSyncDispatchDatabase(ctx, t)
	store := &PostgresStore{pool: domain}

	payload := []byte(`{"installation":{"id":999},"repository":{"id":1,"full_name":"a/b"}}`)
	result, err := triggerAndMint(ctx, t, store, coordinator, "github", "push", payload, time.Now().UTC())
	if err != nil {
		t.Fatal(err)
	}
	if result.Processed || result.Reason != "webhook_sync_unroutable:unknown_installation" {
		t.Fatalf("result = %+v", result)
	}
}

// TestTriggerScopedSyncGithubMatchesByFullNameWhenExternalIDMissing covers
// lookupIntegrationSource's fallback branch: a payload whose repository
// object carries no numeric id (a malformed/legacy shape) still resolves
// via full_name.
func TestTriggerScopedSyncGithubMatchesByFullNameWhenExternalIDMissing(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	pool, domain, coordinator := startSyncDispatchDatabase(ctx, t)
	store := &PostgresStore{pool: domain}

	insertGithubInstallation(ctx, t, pool, 555, "org-1")
	sourceID := insertIntegrationSource(ctx, t, pool, "org-1", "github", "42", "full-chaos/dev-health")
	insertSyncConfiguration(ctx, t, pool, "org-1", "github", sourceID, `{}`)

	payload := []byte(`{"installation":{"id":555},"repository":{"full_name":"full-chaos/dev-health"}}`)
	result, err := triggerAndMint(ctx, t, store, coordinator, "github", "push", payload, time.Now().UTC())
	if err != nil {
		t.Fatal(err)
	}
	if !result.Processed {
		t.Fatalf("result = %+v, want a full_name fallback match", result)
	}
}

// TestTriggerScopedSyncGithubAmbiguousIntegrationSourceIsUnroutable covers
// the github (org-scoped) ambiguity branch, the sibling of gitlab's
// unscoped one already covered above -- a data-hygiene edge case (two
// sources for the same org+provider+external_id) this PR treats as
// unroutable rather than picking one arbitrarily.
func TestTriggerScopedSyncGithubAmbiguousIntegrationSourceIsUnroutable(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	pool, domain, coordinator := startSyncDispatchDatabase(ctx, t)
	store := &PostgresStore{pool: domain}

	insertGithubInstallation(ctx, t, pool, 555, "org-1")
	insertIntegrationSource(ctx, t, pool, "org-1", "github", "42", "full-chaos/dev-health")
	insertIntegrationSource(ctx, t, pool, "org-1", "github", "42", "full-chaos/dev-health-duplicate")

	payload := []byte(`{"installation":{"id":555},"repository":{"id":42,"full_name":"full-chaos/dev-health"}}`)
	result, err := triggerAndMint(ctx, t, store, coordinator, "github", "push", payload, time.Now().UTC())
	if err != nil {
		t.Fatal(err)
	}
	if result.Processed || result.Reason != "webhook_sync_unroutable:ambiguous_integration_source" {
		t.Fatalf("result = %+v", result)
	}
}

// TestTriggerScopedSyncCreatesActiveScheduledJobWhenExplicitCronConfigured
// covers ensureScheduledJobForSyncConfig's OTHER branch: a config that DOES
// carry an explicit sync_options.schedule_cron creates its scheduled_job
// ACTIVE, matching _ensure_scheduled_job_for_config's status rule exactly
// (is_active AND explicit_cron -> ACTIVE, else PAUSED -- see
// TestTriggerScopedSyncGithubCreatesOccurrenceAndManualTrigger for the
// PAUSED case).
func TestTriggerScopedSyncCreatesActiveScheduledJobWhenExplicitCronConfigured(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	pool, domain, coordinator := startSyncDispatchDatabase(ctx, t)
	store := &PostgresStore{pool: domain}

	insertGithubInstallation(ctx, t, pool, 555, "org-1")
	sourceID := insertIntegrationSource(ctx, t, pool, "org-1", "github", "42", "full-chaos/dev-health")
	configID := insertSyncConfiguration(ctx, t, pool, "org-1", "github", sourceID, `{"schedule_cron":"*/15 * * * *","timezone":"America/New_York"}`)

	payload := []byte(`{"installation":{"id":555},"repository":{"id":42,"full_name":"full-chaos/dev-health"}}`)
	result, err := triggerAndMint(ctx, t, store, coordinator, "github", "push", payload, time.Now().UTC())
	if err != nil {
		t.Fatal(err)
	}
	if !result.Processed {
		t.Fatalf("result = %+v", result)
	}
	var status int
	var cron, tz string
	if err := pool.QueryRow(ctx, `SELECT status, schedule_cron, timezone FROM public.scheduled_jobs WHERE sync_config_id = $1`, configID).Scan(&status, &cron, &tz); err != nil {
		t.Fatalf("read scheduled job: %v", err)
	}
	if status != 0 || cron != "*/15 * * * *" || tz != "America/New_York" {
		t.Fatalf("status=%d cron=%q tz=%q, want ACTIVE(0) with the config's own cron/timezone", status, cron, tz)
	}
}

func TestTriggerScopedSyncAmbiguousIntegrationSourceIsUnroutable(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	pool, domain, coordinator := startSyncDispatchDatabase(ctx, t)
	store := &PostgresStore{pool: domain}

	// Two different orgs both track the same external gitlab project id --
	// deliberately unresolvable by external_id alone; must fail loud rather
	// than pick one (team-lead ruling: never guess tenant scope).
	insertIntegrationSource(ctx, t, pool, "org-a", "gitlab", "99", "group/project-mirror-a")
	insertIntegrationSource(ctx, t, pool, "org-b", "gitlab", "99", "group/project-mirror-b")

	payload := []byte(`{"project":{"id":99,"path_with_namespace":"group/project-mirror-a"}}`)
	result, err := triggerAndMint(ctx, t, store, coordinator, "gitlab", "push", payload, time.Now().UTC())
	if err != nil {
		t.Fatal(err)
	}
	if result.Processed || result.Reason != "webhook_sync_unroutable:ambiguous_integration_source" {
		t.Fatalf("result = %+v", result)
	}
}

func TestTriggerScopedSyncIsIdempotentOnRetry(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	pool, domain, coordinator := startSyncDispatchDatabase(ctx, t)
	store := &PostgresStore{pool: domain}

	insertGithubInstallation(ctx, t, pool, 555, "org-1")
	sourceID := insertIntegrationSource(ctx, t, pool, "org-1", "github", "42", "full-chaos/dev-health")
	insertSyncConfiguration(ctx, t, pool, "org-1", "github", sourceID, `{}`)

	// The SAME delivery's own stable created_at, exactly as Work() would
	// pass it on a River retry after e.g. the first attempt's manual-trigger
	// insert failed partway.
	deliveredAt := time.Date(2026, 9, 6, 12, 0, 0, 0, time.UTC)
	payload := []byte(`{"installation":{"id":555},"repository":{"id":42,"full_name":"full-chaos/dev-health"}}`)

	first, err := triggerAndMint(ctx, t, store, coordinator, "github", "push", payload, deliveredAt)
	if err != nil {
		t.Fatal(err)
	}
	second, err := triggerAndMint(ctx, t, store, coordinator, "github", "push", payload, deliveredAt)
	if err != nil {
		t.Fatal(err)
	}
	if first.OccurrenceID != second.OccurrenceID {
		t.Fatalf("a retry of the same delivery must resolve to the same occurrence_id: %q != %q", first.OccurrenceID, second.OccurrenceID)
	}
	if countRows(ctx, t, pool, "public.scheduled_sync_occurrences") != 1 {
		t.Fatal("a retried delivery must never mint a second occurrence row")
	}
	if countRows(ctx, t, pool, "public.sync_manual_triggers") != 1 {
		t.Fatal("a retried delivery must never mint a second manual-trigger row")
	}
}

// startSyncDispatchDatabase builds the real schema and the three runtime
// roles exactly as a deployment's migration grants them (the river hook for
// the domain and queue roles, the coordinator posture for the coordinator
// role), and returns an admin pool (seeding and assertions), a pool logged in
// as the domain role (the webhook worker's) and one logged in as the
// coordinator role (the scheduler's). CHAOS-6695: the webhook path must work
// on exactly these roles.
func startSyncDispatchDatabase(ctx context.Context, t *testing.T) (admin, domain, coordinator *pgxpool.Pool) {
	t.Helper()
	instance, err := containers.StartPostgres(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = instance.Close(context.Background()) })
	config, err := pgxpool.ParseConfig(instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	config.MaxConns = 8
	admin, err = pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(admin.Close)
	applySyncDispatchSchema(ctx, t, admin)

	roles := map[string]string{}
	for _, name := range []string{"domain", "queue", "coordinator"} {
		role, err := containers.RoleName("sync_dispatch_"+name, instance)
		if err != nil {
			t.Fatal(err)
		}
		roles[name] = role
		t.Cleanup(func() { containers.DropRole(admin, role, t.Logf) })
		if _, err := admin.Exec(ctx, "CREATE ROLE "+role+
			" LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS PASSWORD 'sync-dispatch'"); err != nil {
			t.Fatal(err)
		}
	}
	posture := postgresstore.CoordinatorPosture()
	grants := make([]riverstore.TableGrant, 0, len(posture.RequiredTables))
	for _, table := range posture.RequiredTables {
		grants = append(grants, riverstore.TableGrant{TableName: table.TableName,
			AllowInsert: table.AllowInsert, AllowUpdate: table.AllowUpdate, AllowDelete: table.AllowDelete})
	}
	columns := make([]riverstore.ColumnGrant, 0, len(posture.ColumnScoped))
	for _, column := range posture.ColumnScoped {
		columns = append(columns, riverstore.ColumnGrant{TableName: column.TableName, ColumnName: column.ColumnName, Privilege: column.Privilege})
	}
	if _, err := riverstore.ApplyPinnedMigrations(ctx, admin, riverstore.MigrationOptions{
		Schema: "river", DomainRole: roles["domain"], QueueRole: roles["queue"],
		CoordinatorRole: roles["coordinator"], CoordinatorGrants: grants, CoordinatorColumnGrants: columns,
		CoordinatorSequences: append([]string(nil), posture.RequiredSequences...),
	}); err != nil {
		t.Fatal(err)
	}
	connect := func(role string) *pgxpool.Pool {
		roleConfig, err := pgxpool.ParseConfig(instance.URI)
		if err != nil {
			t.Fatal(err)
		}
		roleConfig.ConnConfig.User, roleConfig.ConnConfig.Password = role, "sync-dispatch"
		pool, err := pgxpool.NewWithConfig(ctx, roleConfig)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(pool.Close)
		return pool
	}
	return admin, connect(roles["domain"]), connect(roles["coordinator"])
}

// triggerAndMint runs one webhook delivery the way production does: the
// worker's TriggerScopedSync on the domain role records the request, and the
// scheduler's minter on the coordinator role mints it. The delivery id is
// derived from the delivery itself, so a retry of the same delivery is the
// same delivery.
func triggerAndMint(
	ctx context.Context, t *testing.T, store *PostgresStore, coordinator *pgxpool.Pool,
	provider, eventType string, payload []byte, deliveredAt time.Time,
) (SyncDispatchResult, error) {
	t.Helper()
	deliveryID := uuid.NewSHA1(uuid.NameSpaceOID,
		[]byte(provider+"\x00"+eventType+"\x00"+deliveredAt.UTC().Format(time.RFC3339Nano)+"\x00"+string(payload))).String()
	result, err := store.TriggerScopedSync(ctx, deliveryID, provider, eventType, payload, deliveredAt)
	if err != nil || !result.Processed {
		return result, err
	}
	if err := schedsync.MintWebhookSyncRequests(ctx, coordinator, time.Now().UTC(), 10); err != nil {
		t.Fatalf("mint the webhook sync request: %v", err)
	}
	return result, nil
}

// TestTriggerScopedSyncRetriedDeliveryAfterRerouteMintsNothingTwice (r1): the
// request row is what makes a delivery idempotent, so it is kept after the
// mint; a retry that would now route to a DIFFERENT configuration finds the
// row and writes nothing.
func TestTriggerScopedSyncRetriedDeliveryAfterRerouteMintsNothingTwice(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	pool, domain, coordinator := startSyncDispatchDatabase(ctx, t)
	store := &PostgresStore{pool: domain}

	insertGithubInstallation(ctx, t, pool, 555, "org-1")
	sourceID := insertIntegrationSource(ctx, t, pool, "org-1", "github", "42", "full-chaos/dev-health")
	first := insertSyncConfiguration(ctx, t, pool, "org-1", "github", sourceID, `{}`)

	deliveredAt := time.Date(2026, 9, 6, 12, 0, 0, 0, time.UTC)
	payload := []byte(`{"installation":{"id":555},"repository":{"id":42,"full_name":"full-chaos/dev-health"}}`)
	if _, err := triggerAndMint(ctx, t, store, coordinator, "github", "push", payload, deliveredAt); err != nil {
		t.Fatal(err)
	}
	// The routing moves: the first configuration is deactivated and another
	// child configuration of the same source takes its place.
	if _, err := pool.Exec(ctx, `UPDATE public.sync_configurations SET is_active = false WHERE id = $1::uuid`, first); err != nil {
		t.Fatal(err)
	}
	var second string
	if err := pool.QueryRow(ctx, `
INSERT INTO public.sync_configurations (id, org_id, name, provider, source_id, sync_targets, sync_options, is_active,
	planner_managed, created_at, updated_at)
VALUES (gen_random_uuid(), 'org-1', 'rerouted-child', 'github', $1::uuid, '[]', '{}'::json, true, false, now(), now()) RETURNING id::text`,
		sourceID).Scan(&second); err != nil {
		t.Fatal(err)
	}
	retried, err := triggerAndMint(ctx, t, store, coordinator, "github", "push", payload, deliveredAt)
	if err != nil {
		t.Fatal(err)
	}
	if got := countRows(ctx, t, pool, "public.scheduled_sync_occurrences"); got != 1 {
		t.Fatalf("a retried delivery after a re-route minted %d occurrences (second config %s, reported %s), want 1", got, second, retried.SyncConfigID)
	}
	if got := countRows(ctx, t, pool, "public.webhook_sync_requests"); got != 1 {
		t.Fatalf("webhook_sync_requests rows = %d, want the one delivery's row kept", got)
	}
	var mintedAt *time.Time
	var occurrenceID *string
	if err := pool.QueryRow(ctx, `SELECT minted_at, occurrence_id FROM public.webhook_sync_requests`).Scan(&mintedAt, &occurrenceID); err != nil {
		t.Fatal(err)
	}
	if mintedAt == nil || occurrenceID == nil || *occurrenceID == "" {
		t.Fatalf("the kept row is not marked minted: minted_at=%v occurrence_id=%v", mintedAt, occurrenceID)
	}
}

// TestTriggerScopedSyncDistinctDeliveriesAtOneInstantEachGetAnOccurrence (r1):
// two DIFFERENT deliveries stamped with the same microsecond must not be
// merged into one occurrence (the second one's sources would be lost).
func TestTriggerScopedSyncDistinctDeliveriesAtOneInstantEachGetAnOccurrence(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	pool, domain, coordinator := startSyncDispatchDatabase(ctx, t)
	store := &PostgresStore{pool: domain}

	insertGithubInstallation(ctx, t, pool, 555, "org-1")
	sourceID := insertIntegrationSource(ctx, t, pool, "org-1", "github", "42", "full-chaos/dev-health")
	insertSyncConfiguration(ctx, t, pool, "org-1", "github", sourceID, `{}`)

	deliveredAt := time.Date(2026, 9, 6, 12, 0, 0, 123456000, time.UTC)
	payload := []byte(`{"installation":{"id":555},"repository":{"id":42,"full_name":"full-chaos/dev-health"}}`)
	for _, event := range []string{"push", "pull_request", "issues"} {
		if _, err := triggerAndMint(ctx, t, store, coordinator, "github", event, payload, deliveredAt); err != nil {
			t.Fatal(err)
		}
	}
	if occurrences, triggers := countRows(ctx, t, pool, "public.scheduled_sync_occurrences"), countRows(ctx, t, pool, "public.sync_manual_triggers"); occurrences != 3 || triggers != 3 {
		t.Fatalf("occurrences=%d triggers=%d, want one of each per distinct delivery (3)", occurrences, triggers)
	}
	var distinct int
	if err := pool.QueryRow(ctx, `SELECT count(DISTINCT occurrence_id) FROM public.webhook_sync_requests WHERE minted_at IS NOT NULL`).Scan(&distinct); err != nil || distinct != 3 {
		t.Fatalf("distinct minted occurrence ids = %d (%v), want 3", distinct, err)
	}
}
