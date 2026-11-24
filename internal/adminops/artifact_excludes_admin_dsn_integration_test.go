//go:build integration

package adminops

import (
	"bytes"
	"context"
	"net/url"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"

	"github.com/full-chaos/dev-health-ops/internal/cli"
	"github.com/full-chaos/dev-health-ops/internal/storage/postgres"
)

const (
	adminDSNOrg         = "f1456aaa-1456-4000-8000-0000000000e1"
	adminDSNIntegration = "f1456a06-1456-4000-8000-0000000000a6"
	adminDSNConfig      = "c1456aaa-1456-4000-8000-0000000000c1"
)

// adminDSNRole creates a login role holding, on integrations and sync_configurations, exactly the privileges posture
// declares for them (SELECT plus its INSERT/UPDATE/DELETE flags), and returns its DSN on the test database.
func adminDSNRole(ctx context.Context, t *testing.T, db *database, name string, posture postgres.RolePosture) string {
	t.Helper()
	const password = "role_pw_14565"
	if _, err := db.conn.Exec(ctx, "CREATE ROLE "+pgx.Identifier{name}.Sanitize()+" LOGIN PASSWORD '"+password+"'"); err != nil {
		t.Fatal(err)
	}
	for _, table := range posture.RequiredTables {
		if table.TableName != "integrations" && table.TableName != "sync_configurations" {
			continue
		}
		privileges := []string{"SELECT"}
		if table.AllowInsert {
			privileges = append(privileges, "INSERT")
		}
		if table.AllowUpdate {
			privileges = append(privileges, "UPDATE")
		}
		if table.AllowDelete {
			privileges = append(privileges, "DELETE")
		}
		if _, err := db.conn.Exec(ctx, "GRANT "+strings.Join(privileges, ", ")+" ON public."+pgx.Identifier{table.TableName}.Sanitize()+
			" TO "+pgx.Identifier{name}.Sanitize()); err != nil {
			t.Fatal(err)
		}
	}
	parsed, err := url.Parse(db.uri)
	if err != nil {
		t.Fatal(err)
	}
	parsed.User = url.UserPassword(name, password)
	return parsed.String()
}

func adminDSNSeed(ctx context.Context, t *testing.T, db *database) {
	t.Helper()
	for _, statement := range []struct {
		sql  string
		args []any
	}{
		{`INSERT INTO organizations (id, slug, name, tier, is_active, created_at, updated_at)
VALUES ($1::uuid, 'org-' || $1::text, 'org', 'enterprise', true, now(), now())`, []any{adminDSNOrg}},
		{`INSERT INTO integrations (id, org_id, provider, name, config, is_active, created_at, updated_at)
VALUES ($1::uuid, $2, 'github', 'fx', '{"fetch_comments": true}'::json, true, now(), now())`, []any{adminDSNIntegration, adminDSNOrg}},
		{`INSERT INTO sync_configurations (id, org_id, name, provider, sync_targets, sync_options, is_active, planner_managed, integration_id, created_at, updated_at)
VALUES ($1::uuid, $2, 'fx', 'github', '["tests"]'::json, '{}'::json, true, true, $3::uuid, now(), now())`, []any{adminDSNConfig, adminDSNOrg, adminDSNIntegration}},
	} {
		if _, err := db.conn.Exec(ctx, statement.sql, statement.args...); err != nil {
			t.Fatalf("seed: %v", err)
		}
	}
}

func adminDSNConfigText(ctx context.Context, t *testing.T, db *database) string {
	t.Helper()
	var text string
	if err := db.conn.QueryRow(ctx, `SELECT config::text FROM integrations WHERE id = $1::uuid`, adminDSNIntegration).Scan(&text); err != nil {
		t.Fatal(err)
	}
	return text
}

func adminDSNSet(values map[string]string) (int, string, string) {
	var stdout, stderr bytes.Buffer
	code := runArtifactExcludesSet(context.Background(), cli.Env{
		Args:   []string{"--config-id", adminDSNConfig, "--prefix", "go-images-", "--prefix", "go-test-timings-", "--prefix", "go-junit-part-", "--reason", "CHAOS-14565 cell"},
		Lookup: func(key string) (string, bool) { value, ok := values[key]; return value, ok },
		Stdout: &stdout, Stderr: &stderr,
	})
	return code, stdout.String(), stderr.String()
}

// CHAOS-14565: in a go-api-shaped environment (API_DATABASE_URI only, the api role with its declared grants) the verb
// writes the list. In a worker-shaped environment (POSTGRES_URI only, the domain role, which has no UPDATE on
// integrations) it fails loud with the database's permission error and writes nothing; the base verb picked
// POSTGRES_URI there too (migrationDSN's fallback), so that case behaves as before.
func TestArtifactExcludesVerbRunsAsTheAPIRoleAndFailsLoudAsTheDomainRole(t *testing.T) {
	ctx := context.Background()
	db := startDatabase(t)
	adminDSNSeed(ctx, t, db)
	before := adminDSNConfigText(ctx, t, db)

	domain := adminDSNRole(ctx, t, db, "domain_14565", postgres.DomainPosture())
	code, stdout, stderr := adminDSNSet(map[string]string{"POSTGRES_URI": domain})
	if code != cli.ExitFailure || !strings.Contains(stdout, "permission denied for table integrations") || strings.Contains(stdout+stderr, "role_pw_14565") {
		t.Errorf("worker-shaped env: exit %d stdout %q stderr %q, want exit 1 and the permission error, no password", code, stdout, stderr)
	}
	if got := adminDSNConfigText(ctx, t, db); got != before {
		t.Fatalf("the refused run wrote %s", got)
	}

	api := adminDSNRole(ctx, t, db, "api_14565", postgres.APIPosture(false))
	code, stdout, stderr = adminDSNSet(map[string]string{"API_DATABASE_URI": api})
	if code != cli.ExitOK || stderr != "" || !strings.Contains(stdout, `"integration_config_changed":true`) {
		t.Fatalf("go-api-shaped env: exit %d stdout %q stderr %q, want the write", code, stdout, stderr)
	}
	if got, want := adminDSNConfigText(ctx, t, db), `{"fetch_comments": true, "ci_artifact_exclude_prefixes": ["go-images-", "go-test-timings-", "go-junit-part-"]}`; got != want {
		t.Fatalf("stored %s, want %s", got, want)
	}
}
