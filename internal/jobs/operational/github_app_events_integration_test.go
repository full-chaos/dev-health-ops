//go:build integration

package operational

import (
	"context"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

// applyGithubAppEventSchema creates the two tables UpsertGithubAppEvent reads
// and writes, matching src/dev_health_ops/alembic/versions/
// 0014_add_github_app_installations.py and the IntegrationCredential model's
// columns exactly -- the same "create just what this test touches" pattern
// internal/storage/postgres's own integration tests use, rather than running
// the full alembic chain against a throwaway container.
func applyGithubAppEventSchema(ctx context.Context, t *testing.T, pool *pgxpool.Pool) {
	t.Helper()
	statements := []string{
		`CREATE EXTENSION IF NOT EXISTS pgcrypto`,
		`CREATE TABLE public.github_app_installations (
			id UUID PRIMARY KEY,
			installation_id BIGINT NOT NULL UNIQUE,
			account_login TEXT,
			account_type TEXT,
			org_id TEXT,
			suspended_at TIMESTAMPTZ,
			created_at TIMESTAMPTZ NOT NULL,
			updated_at TIMESTAMPTZ NOT NULL
		)`,
		`CREATE TABLE public.integration_credentials (
			id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
			org_id TEXT NOT NULL DEFAULT '',
			provider TEXT NOT NULL,
			name TEXT NOT NULL,
			is_active BOOLEAN NOT NULL DEFAULT TRUE,
			updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
		)`,
	}
	for _, statement := range statements {
		if _, err := pool.Exec(ctx, statement); err != nil {
			t.Fatalf("apply schema: %v\n%s", err, statement)
		}
	}
}

func installationRow(ctx context.Context, t *testing.T, pool *pgxpool.Pool, installationID int64) (login, accountType *string, orgID *string, suspendedAt *time.Time) {
	t.Helper()
	err := pool.QueryRow(ctx, `
SELECT account_login, account_type, org_id, suspended_at
FROM public.github_app_installations WHERE installation_id = $1`, installationID,
	).Scan(&login, &accountType, &orgID, &suspendedAt)
	if err != nil {
		t.Fatalf("read installation row: %v", err)
	}
	return
}

func TestUpsertGithubAppEventCreatesInstallationAndTracksTransitions(t *testing.T) {
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
	applyGithubAppEventSchema(ctx, t, pool)
	store := &PostgresStore{pool: pool}
	now := time.Date(2026, 9, 6, 12, 0, 0, 0, time.UTC)

	created := []byte(`{"action":"created","installation":{"id":123,"account":{"login":"full-chaos","type":"Organization"}}}`)
	result, err := store.UpsertGithubAppEvent(ctx, "installation", created, now)
	if err != nil {
		t.Fatal(err)
	}
	if !result.Processed || result.InstallationID != 123 || result.Action != "created" {
		t.Fatalf("created result = %+v", result)
	}
	login, accountType, _, suspendedAt := installationRow(ctx, t, pool, 123)
	if login == nil || *login != "full-chaos" || accountType == nil || *accountType != "Organization" || suspendedAt != nil {
		t.Fatalf("after created: login=%v type=%v suspendedAt=%v", login, accountType, suspendedAt)
	}

	suspend := []byte(`{"action":"suspend","installation":{"id":123}}`)
	if _, err := store.UpsertGithubAppEvent(ctx, "installation", suspend, now.Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	_, _, _, suspendedAt = installationRow(ctx, t, pool, 123)
	if suspendedAt == nil {
		t.Fatal("after suspend: suspended_at is still NULL")
	}

	unsuspend := []byte(`{"action":"unsuspend","installation":{"id":123}}`)
	if _, err := store.UpsertGithubAppEvent(ctx, "installation", unsuspend, now.Add(2*time.Minute)); err != nil {
		t.Fatal(err)
	}
	_, _, _, suspendedAt = installationRow(ctx, t, pool, 123)
	if suspendedAt != nil {
		t.Fatalf("after unsuspend: suspended_at = %v, want NULL", *suspendedAt)
	}
}

func TestUpsertGithubAppEventDeletedDeactivatesMatchingCredential(t *testing.T) {
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
	applyGithubAppEventSchema(ctx, t, pool)
	store := &PostgresStore{pool: pool}
	now := time.Date(2026, 9, 6, 12, 0, 0, 0, time.UTC)

	if _, err := pool.Exec(ctx, `
INSERT INTO public.github_app_installations (id, installation_id, org_id, created_at, updated_at)
VALUES (gen_random_uuid(), 999, 'org-1', $1, $1)`, now); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `
INSERT INTO public.integration_credentials (org_id, provider, name, is_active)
VALUES ('org-1', 'github', 'github-app', TRUE)`); err != nil {
		t.Fatal(err)
	}

	deleted := []byte(`{"action":"deleted","installation":{"id":999}}`)
	result, err := store.UpsertGithubAppEvent(ctx, "installation", deleted, now.Add(time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	if !result.Processed || result.Action != "deleted" {
		t.Fatalf("deleted result = %+v", result)
	}

	_, _, orgID, suspendedAt := installationRow(ctx, t, pool, 999)
	if orgID == nil || *orgID != "org-1" || suspendedAt == nil {
		t.Fatalf("after deleted: orgID=%v suspendedAt=%v", orgID, suspendedAt)
	}
	var active bool
	if err := pool.QueryRow(ctx, `
SELECT is_active FROM public.integration_credentials WHERE org_id = 'org-1' AND provider = 'github' AND name = 'github-app'`,
	).Scan(&active); err != nil {
		t.Fatal(err)
	}
	if active {
		t.Fatal("matching github-app credential is still active after deletion")
	}
}

func TestUpsertGithubAppEventMissingInstallationIDIsNotProcessed(t *testing.T) {
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
	applyGithubAppEventSchema(ctx, t, pool)
	store := &PostgresStore{pool: pool}

	result, err := store.UpsertGithubAppEvent(ctx, "installation", []byte(`{"action":"created"}`), time.Now().UTC())
	if err != nil {
		t.Fatal(err)
	}
	if result.Processed || result.Reason != "missing_installation_id" {
		t.Fatalf("result = %+v, want processed=false reason=missing_installation_id", result)
	}
	var count int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM public.github_app_installations`).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 0 {
		t.Fatalf("a row was written despite the missing installation id: count=%d", count)
	}
}

func TestUpsertGithubAppEventMarketplacePurchaseIsANoOpWithNoWrite(t *testing.T) {
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
	applyGithubAppEventSchema(ctx, t, pool)
	store := &PostgresStore{pool: pool}

	result, err := store.UpsertGithubAppEvent(ctx, "marketplace_purchase", []byte(`{"action":"purchased"}`), time.Now().UTC())
	if err != nil {
		t.Fatal(err)
	}
	if !result.Processed || result.Action != "marketplace_purchase" {
		t.Fatalf("result = %+v", result)
	}
	var count int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM public.github_app_installations`).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 0 {
		t.Fatalf("marketplace_purchase must never write github_app_installations, count=%d", count)
	}
}

// TestUpsertGithubAppEventConcurrentInstallConvergesToOneRow is the native
// counterpart of Python's
// test_installation_webhook_recovers_when_callback_created_row_concurrently
// -- but where Python needs to RECOVER from a TOCTOU race (its own select,
// insert, catch-IntegrityError, re-select dance), the native
// `INSERT ... ON CONFLICT DO NOTHING` upsert has no such window: concurrent
// callers racing the same installation_id converge to exactly one row by
// construction. This proves that property directly rather than asserting
// the absence of a recovery path that no longer needs to exist.
func TestUpsertGithubAppEventConcurrentInstallConvergesToOneRow(t *testing.T) {
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
	applyGithubAppEventSchema(ctx, t, pool)
	store := &PostgresStore{pool: pool}
	now := time.Now().UTC()

	const workers = 8
	errs := make(chan error, workers)
	for i := 0; i < workers; i++ {
		go func() {
			payload := []byte(`{"action":"created","installation":{"id":555,"account":{"login":"full-chaos","type":"Organization"}}}`)
			_, err := store.UpsertGithubAppEvent(ctx, "installation", payload, now)
			errs <- err
		}()
	}
	for i := 0; i < workers; i++ {
		if err := <-errs; err != nil {
			t.Fatal(err)
		}
	}
	var count int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM public.github_app_installations WHERE installation_id = 555`).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Fatalf("concurrent installs converged to %d rows, want exactly 1", count)
	}
}
