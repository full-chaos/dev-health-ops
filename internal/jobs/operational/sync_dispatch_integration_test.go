//go:build integration

package operational

import (
	"context"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

// applySyncDispatchSchema creates the tables TriggerScopedSync reads and
// writes, matching the SQLAlchemy models in src/dev_health_ops/models/
// integrations.py (IntegrationSource) and settings.py (SyncConfiguration,
// ScheduledJob, ScheduledSyncOccurrence, SyncManualTrigger) -- the same
// "create just what this test touches" pattern
// github_app_events_integration_test.go's applyGithubAppEventSchema uses,
// rather than running the full alembic chain against a throwaway container.
// FK constraints to tables this PR never reads (integrations, job_runs,
// sync_runs) are deliberately omitted.
func applySyncDispatchSchema(ctx context.Context, t *testing.T, pool *pgxpool.Pool) {
	t.Helper()
	applyGithubAppEventSchema(ctx, t, pool)
	statements := []string{
		`CREATE TABLE public.integration_sources (
			id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
			org_id TEXT NOT NULL,
			integration_id UUID NOT NULL,
			provider TEXT NOT NULL,
			source_type TEXT NOT NULL DEFAULT 'repository',
			external_id TEXT NOT NULL,
			name TEXT NOT NULL DEFAULT '',
			full_name TEXT NOT NULL DEFAULT ''
		)`,
		`CREATE TABLE public.sync_configurations (
			id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
			org_id TEXT NOT NULL DEFAULT '',
			name TEXT NOT NULL,
			provider TEXT NOT NULL,
			sync_targets JSONB NOT NULL DEFAULT '[]',
			sync_options JSONB NOT NULL DEFAULT '{}',
			is_active BOOLEAN NOT NULL DEFAULT TRUE,
			planner_managed BOOLEAN NOT NULL DEFAULT FALSE,
			integration_id UUID,
			source_id UUID
		)`,
		`CREATE TABLE public.scheduled_jobs (
			id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
			org_id TEXT NOT NULL DEFAULT '',
			name TEXT NOT NULL,
			job_type TEXT NOT NULL,
			provider TEXT NOT NULL DEFAULT '',
			schedule_cron TEXT NOT NULL,
			timezone TEXT NOT NULL DEFAULT 'UTC',
			job_config JSONB NOT NULL DEFAULT '{}',
			sync_config_id UUID,
			status INTEGER NOT NULL DEFAULT 0,
			UNIQUE (org_id, sync_config_id, job_type)
		)`,
		`CREATE TABLE public.scheduled_sync_occurrences (
			occurrence_id TEXT PRIMARY KEY,
			identity_version TEXT NOT NULL,
			org_id TEXT NOT NULL,
			sync_config_id UUID NOT NULL,
			scheduled_job_id UUID NOT NULL,
			scheduled_for TIMESTAMPTZ NOT NULL,
			job_run_id UUID,
			sync_run_id UUID,
			reconcile_attempt_count INTEGER NOT NULL DEFAULT 0,
			reconcile_status VARCHAR(16) NOT NULL DEFAULT 'pending',
			UNIQUE (sync_config_id, scheduled_for)
		)`,
		`CREATE TABLE public.sync_manual_triggers (
			occurrence_id TEXT PRIMARY KEY REFERENCES public.scheduled_sync_occurrences(occurrence_id) ON DELETE CASCADE,
			mode TEXT NOT NULL,
			since TIMESTAMPTZ,
			before TIMESTAMPTZ,
			source_ids TEXT[],
			dataset_keys TEXT[],
			triggered_by TEXT NOT NULL,
			created_at TIMESTAMPTZ NOT NULL DEFAULT now()
		)`,
	}
	for _, statement := range statements {
		if _, err := pool.Exec(ctx, statement); err != nil {
			t.Fatalf("apply schema: %v\n%s", err, statement)
		}
	}
}

func insertIntegrationSource(
	ctx context.Context, t *testing.T, pool *pgxpool.Pool, orgID, provider, externalID, fullName string,
) string {
	t.Helper()
	var id string
	err := pool.QueryRow(ctx, `
INSERT INTO public.integration_sources (org_id, integration_id, provider, external_id, full_name)
VALUES ($1, gen_random_uuid(), $2, $3, $4) RETURNING id::text`,
		orgID, provider, externalID, fullName,
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
INSERT INTO public.sync_configurations (org_id, name, provider, source_id, sync_options)
VALUES ($1, $2, $3, $4, $5::jsonb) RETURNING id::text`,
		orgID, "webhook-child-"+sourceID, provider, sourceID, syncOptions,
	).Scan(&id)
	if err != nil {
		t.Fatalf("insert sync configuration: %v", err)
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

func TestTriggerScopedSyncNoChildConfigIsUnroutable(t *testing.T) {
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
	// SyncConfiguration is scoped to it -- the "org-level parent only, no
	// child" gap team-lead's ruling explicitly calls out.
	insertIntegrationSource(ctx, t, pool, "org-1", "github", "42", "full-chaos/dev-health")

	payload := []byte(`{"installation":{"id":555},"repository":{"id":42,"full_name":"full-chaos/dev-health"}}`)
	result, err := store.TriggerScopedSync(ctx, "github", "push", payload, time.Now().UTC())
	if err != nil {
		t.Fatal(err)
	}
	if result.Processed || result.Reason != "webhook_sync_unroutable:no_child_sync_config" {
		t.Fatalf("result = %+v", result)
	}
	if countRows(ctx, t, pool, "public.scheduled_sync_occurrences") != 0 {
		t.Fatal("an unroutable event must never write an occurrence row")
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
