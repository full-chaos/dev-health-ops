//go:build integration

package operational

import (
	"context"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/pgmigrate"
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
	instance, err := containers.StartPostgres(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer instance.Close(context.Background())
	pool, err := pgxpool.New(ctx, instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	applySyncDispatchSchema(ctx, t, pool)
	store := &PostgresStore{pool: pool}

	insertGithubInstallation(ctx, t, pool, 555, "org-1")
	sourceID := insertIntegrationSource(ctx, t, pool, "org-1", "github", "42", "full-chaos/dev-health")
	configID := insertSyncConfiguration(ctx, t, pool, "org-1", "github", sourceID, `{}`)

	deliveredAt := time.Date(2026, 9, 6, 12, 0, 0, 0, time.UTC)
	payload := []byte(`{"installation":{"id":555},"repository":{"id":42,"full_name":"full-chaos/dev-health"}}`)
	result, err := store.TriggerScopedSync(ctx, "github", "push", payload, deliveredAt)
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
	if identityVersion != syncOccurrenceIdentityVersion || orgID != "org-1" || syncConfigID != configID {
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
	if jobStatus != jobStatusPaused {
		t.Fatalf("a freshly created job with no explicit schedule_cron must be PAUSED(%d), got %d", jobStatusPaused, jobStatus)
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
	instance, err := containers.StartPostgres(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer instance.Close(context.Background())
	pool, err := pgxpool.New(ctx, instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	applySyncDispatchSchema(ctx, t, pool)
	store := &PostgresStore{pool: pool}

	sourceID := insertIntegrationSource(ctx, t, pool, "org-2", "gitlab", "99", "group/project")
	configID := insertSyncConfiguration(ctx, t, pool, "org-2", "gitlab", sourceID, `{}`)

	deliveredAt := time.Date(2026, 9, 6, 12, 0, 0, 0, time.UTC)
	payload := []byte(`{"project":{"id":99,"path_with_namespace":"group/project"}}`)
	result, err := store.TriggerScopedSync(ctx, "gitlab", "merge_request", payload, deliveredAt)
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
	instance, err := containers.StartPostgres(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer instance.Close(context.Background())
	pool, err := pgxpool.New(ctx, instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	applySyncDispatchSchema(ctx, t, pool)
	store := &PostgresStore{pool: pool}

	insertGithubInstallation(ctx, t, pool, 555, "org-1")
	// An IntegrationSource exists (the repo is discovered) but NO
	// SyncConfiguration at all -- neither a child scoped to it nor an org
	// parent config for this provider.
	insertIntegrationSource(ctx, t, pool, "org-1", "github", "42", "full-chaos/dev-health")

	payload := []byte(`{"installation":{"id":555},"repository":{"id":42,"full_name":"full-chaos/dev-health"}}`)
	result, err := store.TriggerScopedSync(ctx, "github", "push", payload, time.Now().UTC())
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
	instance, err := containers.StartPostgres(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer instance.Close(context.Background())
	pool, err := pgxpool.New(ctx, instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	applySyncDispatchSchema(ctx, t, pool)
	store := &PostgresStore{pool: pool}

	insertGithubInstallation(ctx, t, pool, 555, "org-1")
	insertIntegrationSource(ctx, t, pool, "org-1", "github", "42", "full-chaos/dev-health")
	parentConfigID := insertParentSyncConfig(ctx, t, pool, "org-1", "github", true)

	payload := []byte(`{"installation":{"id":555},"repository":{"id":42,"full_name":"full-chaos/dev-health"}}`)
	result, err := store.TriggerScopedSync(ctx, "github", "push", payload, time.Now().UTC())
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
	instance, err := containers.StartPostgres(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer instance.Close(context.Background())
	pool, err := pgxpool.New(ctx, instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	applySyncDispatchSchema(ctx, t, pool)
	store := &PostgresStore{pool: pool}

	insertIntegrationSource(ctx, t, pool, "org-2", "gitlab", "99", "group/project")
	parentConfigID := insertParentSyncConfig(ctx, t, pool, "org-2", "gitlab", true)

	payload := []byte(`{"project":{"id":99,"path_with_namespace":"group/project"}}`)
	result, err := store.TriggerScopedSync(ctx, "gitlab", "merge_request", payload, time.Now().UTC())
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
	instance, err := containers.StartPostgres(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer instance.Close(context.Background())
	pool, err := pgxpool.New(ctx, instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	applySyncDispatchSchema(ctx, t, pool)
	store := &PostgresStore{pool: pool}

	insertIntegrationSource(ctx, t, pool, "org-2", "gitlab", "99", "group/project")
	insertParentSyncConfig(ctx, t, pool, "org-2", "gitlab", false)

	payload := []byte(`{"project":{"id":99,"path_with_namespace":"group/project"}}`)
	result, err := store.TriggerScopedSync(ctx, "gitlab", "merge_request", payload, time.Now().UTC())
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
	instance, err := containers.StartPostgres(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer instance.Close(context.Background())
	pool, err := pgxpool.New(ctx, instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	applySyncDispatchSchema(ctx, t, pool)
	store := &PostgresStore{pool: pool}

	insertIntegrationSource(ctx, t, pool, "org-2", "gitlab", "99", "group/project")
	insertParentSyncConfig(ctx, t, pool, "org-2", "gitlab", true)
	insertParentSyncConfig(ctx, t, pool, "org-2", "gitlab", true)

	payload := []byte(`{"project":{"id":99,"path_with_namespace":"group/project"}}`)
	result, err := store.TriggerScopedSync(ctx, "gitlab", "merge_request", payload, time.Now().UTC())
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
	instance, err := containers.StartPostgres(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer instance.Close(context.Background())
	pool, err := pgxpool.New(ctx, instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	applySyncDispatchSchema(ctx, t, pool)
	store := &PostgresStore{pool: pool}

	insertIntegrationSource(ctx, t, pool, "org-3", "jira", "CHAOS", "Full Chaos")
	parentConfigID := insertParentSyncConfig(ctx, t, pool, "org-3", "jira", true)

	payload := []byte(`{"issue":{"key":"CHAOS-1","fields":{"project":{"key":"CHAOS","name":"Full Chaos"}}}}`)
	result, err := store.TriggerScopedSync(ctx, "jira", "issue_updated", payload, time.Now().UTC())
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
	instance, err := containers.StartPostgres(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer instance.Close(context.Background())
	pool, err := pgxpool.New(ctx, instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	applySyncDispatchSchema(ctx, t, pool)
	store := &PostgresStore{pool: pool}

	insertIntegrationSource(ctx, t, pool, "org-a", "jira", "OPS", "Ops Team A")
	insertIntegrationSource(ctx, t, pool, "org-b", "jira", "OPS", "Ops Team B")

	payload := []byte(`{"issue":{"key":"OPS-1","fields":{"project":{"key":"OPS","name":"Ops Team A"}}}}`)
	result, err := store.TriggerScopedSync(ctx, "jira", "issue_created", payload, time.Now().UTC())
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
	instance, err := containers.StartPostgres(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer instance.Close(context.Background())
	pool, err := pgxpool.New(ctx, instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	applySyncDispatchSchema(ctx, t, pool)
	store := &PostgresStore{pool: pool}

	payload := []byte(`{"issue":{"key":"CHAOS-1","fields":{}}}`)
	result, err := store.TriggerScopedSync(ctx, "jira", "issue_updated", payload, time.Now().UTC())
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
	instance, err := containers.StartPostgres(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer instance.Close(context.Background())
	pool, err := pgxpool.New(ctx, instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	applySyncDispatchSchema(ctx, t, pool)
	store := &PostgresStore{pool: pool}

	insertGithubInstallation(ctx, t, pool, 555, "org-1")
	sourceID := insertIntegrationSource(ctx, t, pool, "org-1", "github", "42", "full-chaos/dev-health")
	childConfigID := insertSyncConfiguration(ctx, t, pool, "org-1", "github", sourceID, `{}`)
	insertParentSyncConfig(ctx, t, pool, "org-1", "github", true)

	payload := []byte(`{"installation":{"id":555},"repository":{"id":42,"full_name":"full-chaos/dev-health"}}`)
	result, err := store.TriggerScopedSync(ctx, "github", "push", payload, time.Now().UTC())
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
	instance, err := containers.StartPostgres(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer instance.Close(context.Background())
	pool, err := pgxpool.New(ctx, instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	applySyncDispatchSchema(ctx, t, pool)
	store := &PostgresStore{pool: pool}

	payload := []byte(`{"installation":{"id":999},"repository":{"id":1,"full_name":"a/b"}}`)
	result, err := store.TriggerScopedSync(ctx, "github", "push", payload, time.Now().UTC())
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
	instance, err := containers.StartPostgres(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer instance.Close(context.Background())
	pool, err := pgxpool.New(ctx, instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	applySyncDispatchSchema(ctx, t, pool)
	store := &PostgresStore{pool: pool}

	insertGithubInstallation(ctx, t, pool, 555, "org-1")
	sourceID := insertIntegrationSource(ctx, t, pool, "org-1", "github", "42", "full-chaos/dev-health")
	insertSyncConfiguration(ctx, t, pool, "org-1", "github", sourceID, `{}`)

	payload := []byte(`{"installation":{"id":555},"repository":{"full_name":"full-chaos/dev-health"}}`)
	result, err := store.TriggerScopedSync(ctx, "github", "push", payload, time.Now().UTC())
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
	instance, err := containers.StartPostgres(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer instance.Close(context.Background())
	pool, err := pgxpool.New(ctx, instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	applySyncDispatchSchema(ctx, t, pool)
	store := &PostgresStore{pool: pool}

	insertGithubInstallation(ctx, t, pool, 555, "org-1")
	insertIntegrationSource(ctx, t, pool, "org-1", "github", "42", "full-chaos/dev-health")
	insertIntegrationSource(ctx, t, pool, "org-1", "github", "42", "full-chaos/dev-health-duplicate")

	payload := []byte(`{"installation":{"id":555},"repository":{"id":42,"full_name":"full-chaos/dev-health"}}`)
	result, err := store.TriggerScopedSync(ctx, "github", "push", payload, time.Now().UTC())
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
	instance, err := containers.StartPostgres(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer instance.Close(context.Background())
	pool, err := pgxpool.New(ctx, instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	applySyncDispatchSchema(ctx, t, pool)
	store := &PostgresStore{pool: pool}

	insertGithubInstallation(ctx, t, pool, 555, "org-1")
	sourceID := insertIntegrationSource(ctx, t, pool, "org-1", "github", "42", "full-chaos/dev-health")
	configID := insertSyncConfiguration(ctx, t, pool, "org-1", "github", sourceID, `{"schedule_cron":"*/15 * * * *","timezone":"America/New_York"}`)

	payload := []byte(`{"installation":{"id":555},"repository":{"id":42,"full_name":"full-chaos/dev-health"}}`)
	result, err := store.TriggerScopedSync(ctx, "github", "push", payload, time.Now().UTC())
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
	if status != jobStatusActive || cron != "*/15 * * * *" || tz != "America/New_York" {
		t.Fatalf("status=%d cron=%q tz=%q, want ACTIVE(%d) with the config's own cron/timezone", status, cron, tz, jobStatusActive)
	}
}

func TestTriggerScopedSyncAmbiguousIntegrationSourceIsUnroutable(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	instance, err := containers.StartPostgres(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer instance.Close(context.Background())
	pool, err := pgxpool.New(ctx, instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	applySyncDispatchSchema(ctx, t, pool)
	store := &PostgresStore{pool: pool}

	// Two different orgs both track the same external gitlab project id --
	// deliberately unresolvable by external_id alone; must fail loud rather
	// than pick one (team-lead ruling: never guess tenant scope).
	insertIntegrationSource(ctx, t, pool, "org-a", "gitlab", "99", "group/project-mirror-a")
	insertIntegrationSource(ctx, t, pool, "org-b", "gitlab", "99", "group/project-mirror-b")

	payload := []byte(`{"project":{"id":99,"path_with_namespace":"group/project-mirror-a"}}`)
	result, err := store.TriggerScopedSync(ctx, "gitlab", "push", payload, time.Now().UTC())
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
	instance, err := containers.StartPostgres(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer instance.Close(context.Background())
	pool, err := pgxpool.New(ctx, instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	applySyncDispatchSchema(ctx, t, pool)
	store := &PostgresStore{pool: pool}

	insertGithubInstallation(ctx, t, pool, 555, "org-1")
	sourceID := insertIntegrationSource(ctx, t, pool, "org-1", "github", "42", "full-chaos/dev-health")
	insertSyncConfiguration(ctx, t, pool, "org-1", "github", sourceID, `{}`)

	// The SAME delivery's own stable created_at, exactly as Work() would
	// pass it on a River retry after e.g. the first attempt's manual-trigger
	// insert failed partway.
	deliveredAt := time.Date(2026, 9, 6, 12, 0, 0, 0, time.UTC)
	payload := []byte(`{"installation":{"id":555},"repository":{"id":42,"full_name":"full-chaos/dev-health"}}`)

	first, err := store.TriggerScopedSync(ctx, "github", "push", payload, deliveredAt)
	if err != nil {
		t.Fatal(err)
	}
	second, err := store.TriggerScopedSync(ctx, "github", "push", payload, deliveredAt)
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
