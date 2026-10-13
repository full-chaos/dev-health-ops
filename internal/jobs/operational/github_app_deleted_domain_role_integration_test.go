//go:build integration

package operational

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
)

// CHAOS-6747: the installation "deleted" event deactivates the org's
// github-app credential on the domain pool. The domain role must hold a
// column-scoped UPDATE (is_active, updated_at) on integration_credentials and
// nothing wider: credentials_encrypted stays unwritable by the worker.

func TestDeletedInstallationDeactivatesCredentialAsTheDomainRole(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	admin, domain, _ := startSyncDispatchDatabase(ctx, t)
	store := &PostgresStore{pool: domain}
	now := time.Date(2026, 10, 13, 12, 0, 0, 0, time.UTC)

	if _, err := admin.Exec(ctx, `
INSERT INTO public.github_app_installations (id, installation_id, org_id, created_at, updated_at)
VALUES (gen_random_uuid(), 999, 'org-1', $1, $1)`, now); err != nil {
		t.Fatal(err)
	}
	if _, err := admin.Exec(ctx, `
INSERT INTO public.integration_credentials (id, org_id, provider, name, is_active, credentials_encrypted, created_at, updated_at)
VALUES (gen_random_uuid(), 'org-1', 'github', 'github-app', TRUE, 'sealed-before', $1, $1)`, now); err != nil {
		t.Fatal(err)
	}

	result, err := store.UpsertGithubAppEvent(ctx, "installation",
		[]byte(`{"action":"deleted","installation":{"id":999}}`), now.Add(time.Minute))
	if err != nil {
		t.Fatalf("deleted installation event as the domain role: %v", err)
	}
	if !result.Processed || result.Action != "deleted" {
		t.Fatalf("deleted result = %+v", result)
	}

	var (
		active    bool
		sealed    *string
		updatedAt time.Time
		suspended *time.Time
	)
	if err := admin.QueryRow(ctx, `
SELECT is_active, credentials_encrypted, updated_at
FROM public.integration_credentials WHERE org_id = 'org-1' AND provider = 'github' AND name = 'github-app'`,
	).Scan(&active, &sealed, &updatedAt); err != nil {
		t.Fatal(err)
	}
	if active {
		t.Fatal("github-app credential is still active after the installation was deleted")
	}
	if sealed == nil || *sealed != "sealed-before" {
		t.Fatalf("credentials_encrypted changed: %v", sealed)
	}
	if !updatedAt.Equal(now.Add(time.Minute)) {
		t.Fatalf("updated_at = %v, want the event time", updatedAt)
	}
	if err := admin.QueryRow(ctx,
		`SELECT suspended_at FROM public.github_app_installations WHERE installation_id = 999`,
	).Scan(&suspended); err != nil || suspended == nil {
		t.Fatalf("installation transaction did not commit: suspended_at=%v err=%v", suspended, err)
	}
}

// The grant check: of every column of integration_credentials, the domain role
// may UPDATE exactly is_active and updated_at, and may INSERT, reference or
// delete nothing. Every statement that would widen it is refused with 42501.
func TestDomainRoleUpdatesOnlyTheDeactivationColumnsOfIntegrationCredentials(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	admin, domain, _ := startSyncDispatchDatabase(ctx, t)

	rows, err := domain.Query(ctx, `
SELECT attribute.attname,
	has_column_privilege(current_user, attribute.attrelid, attribute.attname, 'UPDATE'),
	has_column_privilege(current_user, attribute.attrelid, attribute.attname, 'INSERT'),
	has_column_privilege(current_user, attribute.attrelid, attribute.attname, 'REFERENCES'),
	has_column_privilege(current_user, attribute.attrelid, attribute.attname, 'SELECT')
FROM pg_catalog.pg_attribute AS attribute
WHERE attribute.attrelid = 'public.integration_credentials'::regclass
	AND attribute.attnum > 0 AND NOT attribute.attisdropped
ORDER BY attribute.attnum`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	updatable := map[string]bool{}
	columns := 0
	for rows.Next() {
		var name string
		var update, insert, references, selectable bool
		if err := rows.Scan(&name, &update, &insert, &references, &selectable); err != nil {
			t.Fatal(err)
		}
		columns++
		if update {
			updatable[name] = true
		}
		if insert || references {
			t.Errorf("column %s: INSERT=%v REFERENCES=%v, want neither", name, insert, references)
		}
		if !selectable {
			t.Errorf("column %s: the domain role lost SELECT", name)
		}
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if columns < 5 || !updatable["is_active"] || !updatable["updated_at"] || len(updatable) != 2 {
		t.Fatalf("updatable columns = %v of %d, want exactly is_active and updated_at", updatable, columns)
	}
	if _, found := updatable["credentials_encrypted"]; found {
		t.Fatal("credentials_encrypted is updatable by the domain role")
	}

	if _, err := admin.Exec(ctx, `
INSERT INTO public.integration_credentials (id, org_id, provider, name, is_active, credentials_encrypted, created_at, updated_at)
VALUES (gen_random_uuid(), 'org-2', 'github', 'github-app', TRUE, 'sealed', now(), now())`); err != nil {
		t.Fatal(err)
	}
	for name, statement := range map[string]string{
		"credentials_encrypted": `UPDATE public.integration_credentials SET credentials_encrypted = 'forged' WHERE org_id = 'org-2'`,
		"config":                `UPDATE public.integration_credentials SET config = '{}'::json WHERE org_id = 'org-2'`,
		"org_id":                `UPDATE public.integration_credentials SET org_id = 'org-3' WHERE org_id = 'org-2'`,
		"mixed with is_active":  `UPDATE public.integration_credentials SET is_active = false, credentials_encrypted = 'forged' WHERE org_id = 'org-2'`,
		"delete":                `DELETE FROM public.integration_credentials WHERE org_id = 'org-2'`,
		"insert": `INSERT INTO public.integration_credentials (id, org_id, provider, name, is_active, created_at, updated_at)
			VALUES (gen_random_uuid(), 'org-4', 'github', 'x', TRUE, now(), now())`,
	} {
		_, err := domain.Exec(ctx, statement)
		var pgErr *pgconn.PgError
		if !errors.As(err, &pgErr) || pgErr.Code != "42501" {
			t.Errorf("%s: err = %v, want SQLSTATE 42501", name, err)
		}
	}
	var sealed string
	if err := admin.QueryRow(ctx,
		`SELECT credentials_encrypted FROM public.integration_credentials WHERE org_id = 'org-2'`).Scan(&sealed); err != nil || sealed != "sealed" {
		t.Fatalf("credentials_encrypted after refused writes = %q, err %v", sealed, err)
	}
}
