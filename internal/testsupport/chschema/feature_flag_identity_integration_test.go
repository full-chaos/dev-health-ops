//go:build integration

package chschema_test

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	clickhousestore "github.com/full-chaos/dev-health-ops/internal/storage/clickhouse"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/chschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

type featureFlagIntegrationRow struct {
	orgID       string
	provider    string
	flagKey     string
	projectKey  string
	repoID      string
	environment string
	flagType    string
	lastSynced  time.Time
}

// This test deliberately does not author feature_flag DDL.  The table and
// its ReplacingMergeTree key come from the complete production migration
// chain, including migration 074.
func TestFeatureFlagEnvironmentIdentityAgainstMigrationChain(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Minute)
	defer cancel()

	instance, err := containers.StartClickHouse(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		closeContext, closeCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer closeCancel()
		if err := instance.Close(closeContext); err != nil {
			t.Errorf("terminate ClickHouse: %v", err)
		}
	})

	chschema.Apply(ctx, t, instance)
	conn, err := clickhousestore.Open(ctx, clickhousestore.DefaultConfig(instance.URI))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })

	var migrationCount uint64
	if err := conn.QueryRow(
		ctx,
		"SELECT count() FROM schema_migrations WHERE version = '074_feature_flag_environment_identity.py'",
	).Scan(&migrationCount); err != nil {
		t.Fatal(err)
	}
	if migrationCount != 1 {
		t.Fatalf("migration 074 ledger rows=%d, want 1", migrationCount)
	}

	var sortingKey string
	if err := conn.QueryRow(
		ctx,
		"SELECT sorting_key FROM system.tables WHERE database = currentDatabase() AND name = 'feature_flag'",
	).Scan(&sortingKey); err != nil {
		t.Fatal(err)
	}
	if sortingKey != "org_id, provider, project_key, flag_key, environment" {
		t.Fatalf("feature_flag sorting_key=%q, want environment-aware identity", sortingKey)
	}

	now := time.Date(2026, 8, 10, 12, 0, 0, 0, time.UTC)
	rows := []featureFlagIntegrationRow{
		{orgID: "chaos-3703-org-a", provider: "gitlab", flagKey: "checkout-v2", projectKey: "group/project", repoID: "repo-a", environment: "production", flagType: "boolean", lastSynced: now},
		{orgID: "chaos-3703-org-a", provider: "gitlab", flagKey: "checkout-v2", projectKey: "group/project", repoID: "repo-a", environment: "staging", flagType: "boolean", lastSynced: now},
		// Same natural key and environment, but a different tenant: org_id
		// remains the first identity component after the environment repair.
		{orgID: "chaos-3703-org-b", provider: "gitlab", flagKey: "checkout-v2", projectKey: "group/project", repoID: "repo-b", environment: "production", flagType: "boolean", lastSynced: now},
	}

	batch, err := conn.PrepareBatch(ctx, "INSERT INTO feature_flag (org_id, provider, flag_key, project_key, repo_id, environment, flag_type, created_at, archived_at, last_synced)")
	if err != nil {
		t.Fatal(err)
	}
	for _, row := range rows {
		if err := batch.Append(
			row.orgID,
			row.provider,
			row.flagKey,
			row.projectKey,
			row.repoID,
			row.environment,
			row.flagType,
			row.lastSynced,
			nil,
			row.lastSynced,
		); err != nil {
			t.Fatal(err)
		}
	}
	if err := batch.Send(); err != nil {
		t.Fatal(err)
	}

	// A rerun writes the same rows again.  FINAL must retain both environments
	// for org-a and the independent tenant row for org-b.
	if err := conn.Exec(ctx, "INSERT INTO feature_flag SELECT * FROM feature_flag"); err != nil {
		t.Fatal(err)
	}
	if err := conn.Exec(ctx, "OPTIMIZE TABLE feature_flag FINAL"); err != nil {
		t.Fatal(err)
	}

	rowsResult, err := conn.Query(
		ctx,
		"SELECT org_id, environment FROM feature_flag FINAL WHERE flag_key = 'checkout-v2' ORDER BY org_id, environment",
	)
	if err != nil {
		t.Fatal(err)
	}
	defer rowsResult.Close()
	got := make(map[string]struct{})
	for rowsResult.Next() {
		var orgID, environment string
		if err := rowsResult.Scan(&orgID, &environment); err != nil {
			t.Fatal(err)
		}
		got[orgID+"/"+environment] = struct{}{}
	}
	if err := rowsResult.Err(); err != nil {
		t.Fatal(err)
	}
	want := map[string]struct{}{
		"chaos-3703-org-a/production": {},
		"chaos-3703-org-a/staging":    {},
		"chaos-3703-org-b/production": {},
	}
	if len(got) != len(want) {
		t.Fatalf("feature_flag FINAL rows=%v, want %v", got, want)
	}
	for key := range want {
		if _, ok := got[key]; !ok {
			t.Errorf("feature_flag FINAL missing %s (got %v)", key, got)
		}
	}
}

// TestFeatureFlagEnvironmentMigrationPreservesPreexistingRows executes the
// production 074 module against a real table created from the production 034
// statement.  The first test proves the complete chain reaches the new head;
// this one proves the forward upgrade does not discard rows that already
// existed under the legacy key.
func TestFeatureFlagEnvironmentMigrationPreservesPreexistingRows(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Minute)
	defer cancel()

	instance, err := containers.StartClickHouse(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		closeContext, closeCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer closeCancel()
		if err := instance.Close(closeContext); err != nil {
			t.Errorf("terminate ClickHouse: %v", err)
		}
	})

	conn, err := clickhousestore.Open(ctx, clickhousestore.DefaultConfig(instance.URI))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })

	legacyDDL := featureFlagLegacyDDL(t)
	if err := conn.Exec(ctx, legacyDDL); err != nil {
		t.Fatalf("create feature_flag from migration 034: %v", err)
	}
	// Freeze the legacy table while staging the pre-migration rows.  Separate
	// inserts model rows that arrived over time and prevent ClickHouse from
	// merging the old four-column identity before migration 074 can copy it.
	if err := conn.Exec(ctx, "SYSTEM STOP MERGES feature_flag"); err != nil {
		t.Fatal(err)
	}
	dsn, err := containers.ClickHouseHTTPDSN(ctx, instance)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 8, 10, 13, 0, 0, 0, time.UTC)
	rows := []featureFlagIntegrationRow{
		{orgID: "chaos-3703-old-org-a", provider: "gitlab", flagKey: "checkout-v2", projectKey: "group/project", repoID: "repo-a", environment: "production", flagType: "boolean", lastSynced: now},
		{orgID: "chaos-3703-old-org-a", provider: "gitlab", flagKey: "checkout-v2", projectKey: "group/project", repoID: "repo-a", environment: "staging", flagType: "boolean", lastSynced: now},
		{orgID: "chaos-3703-old-org-b", provider: "gitlab", flagKey: "checkout-v2", projectKey: "group/project", repoID: "repo-b", environment: "production", flagType: "boolean", lastSynced: now},
	}
	for _, row := range rows {
		insertFeatureFlagRows(t, ctx, conn, []featureFlagIntegrationRow{row})
	}

	var physicalBefore uint64
	if err := conn.QueryRow(ctx, "SELECT count() FROM feature_flag").Scan(&physicalBefore); err != nil {
		t.Fatal(err)
	}
	if physicalBefore != uint64(len(rows)) {
		t.Fatalf("legacy feature_flag physical rows=%d before migration, want %d", physicalBefore, len(rows))
	}

	applyFeatureFlagMigration074(t, ctx, dsn)
	if err := conn.Exec(ctx, "SYSTEM START MERGES feature_flag"); err != nil {
		t.Fatal(err)
	}

	// Model a crashed post-exchange runner: its old side is a real legacy-key
	// shadow with a row that was written after the snapshot.  A rerun of the
	// production 074 module must drain that shadow before returning success.
	const rerunShadow = "feature_flag_074_new_real_rerun"
	shadowDDL := strings.Replace(
		legacyDDL,
		"CREATE TABLE IF NOT EXISTS feature_flag",
		"CREATE TABLE IF NOT EXISTS "+rerunShadow,
		1,
	)
	if err := conn.Exec(ctx, shadowDDL); err != nil {
		t.Fatalf("create legacy rerun shadow: %v", err)
	}
	insertFeatureFlagRowsIntoTable(t, ctx, conn, rerunShadow, []featureFlagIntegrationRow{
		{orgID: "chaos-3703-old-org-a", provider: "gitlab", flagKey: "checkout-v2", projectKey: "group/project", repoID: "repo-a", environment: "canary", flagType: "boolean", lastSynced: now.Add(time.Minute)},
	})
	applyFeatureFlagMigration074(t, ctx, dsn)
	var shadowCount uint64
	if err := conn.QueryRow(
		ctx,
		"SELECT count() FROM system.tables WHERE database = currentDatabase() AND name = '"+rerunShadow+"'",
	).Scan(&shadowCount); err != nil {
		t.Fatal(err)
	}
	if shadowCount != 0 {
		t.Fatalf("rerun legacy shadow remains after migration 074: %d", shadowCount)
	}
	if err := conn.Exec(ctx, "OPTIMIZE TABLE feature_flag FINAL"); err != nil {
		t.Fatal(err)
	}

	var sortingKey string
	if err := conn.QueryRow(
		ctx,
		"SELECT sorting_key FROM system.tables WHERE database = currentDatabase() AND name = 'feature_flag'",
	).Scan(&sortingKey); err != nil {
		t.Fatal(err)
	}
	if sortingKey != "org_id, provider, project_key, flag_key, environment" {
		t.Fatalf("upgraded feature_flag sorting_key=%q", sortingKey)
	}

	result, err := conn.Query(
		ctx,
		"SELECT org_id, environment FROM feature_flag FINAL WHERE flag_key = 'checkout-v2' ORDER BY org_id, environment",
	)
	if err != nil {
		t.Fatal(err)
	}
	defer result.Close()
	got := make(map[string]struct{})
	for result.Next() {
		var orgID, environment string
		if err := result.Scan(&orgID, &environment); err != nil {
			t.Fatal(err)
		}
		got[orgID+"/"+environment] = struct{}{}
	}
	if err := result.Err(); err != nil {
		t.Fatal(err)
	}
	want := map[string]struct{}{
		"chaos-3703-old-org-a/production": {},
		"chaos-3703-old-org-a/staging":    {},
		"chaos-3703-old-org-a/canary":     {},
		"chaos-3703-old-org-b/production": {},
	}
	if len(got) != len(want) {
		t.Fatalf("upgraded feature_flag FINAL rows=%v, want %v", got, want)
	}
	for key := range want {
		if _, ok := got[key]; !ok {
			t.Errorf("upgraded feature_flag FINAL missing %s (got %v)", key, got)
		}
	}
}

func insertFeatureFlagRows(t *testing.T, ctx context.Context, conn driver.Conn, rows []featureFlagIntegrationRow) {
	insertFeatureFlagRowsIntoTable(t, ctx, conn, "feature_flag", rows)
}

func insertFeatureFlagRowsIntoTable(t *testing.T, ctx context.Context, conn driver.Conn, table string, rows []featureFlagIntegrationRow) {
	t.Helper()
	batch, err := conn.PrepareBatch(ctx, "INSERT INTO `"+table+"` (org_id, provider, flag_key, project_key, repo_id, environment, flag_type, created_at, archived_at, last_synced)")
	if err != nil {
		t.Fatal(err)
	}
	for _, row := range rows {
		if err := batch.Append(row.orgID, row.provider, row.flagKey, row.projectKey, row.repoID, row.environment, row.flagType, row.lastSynced, nil, row.lastSynced); err != nil {
			t.Fatal(err)
		}
	}
	if err := batch.Send(); err != nil {
		t.Fatal(err)
	}
}

func featureFlagLegacyDDL(t *testing.T) string {
	t.Helper()
	root := featureFlagIntegrationRepoRoot(t)
	path := filepath.Join(root, "src", "dev_health_ops", "migrations", "clickhouse", "034_feature_flag_user_impact_tables.sql")
	contents, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	text := string(contents)
	startMarker := "CREATE TABLE IF NOT EXISTS feature_flag"
	start := strings.Index(text, startMarker)
	if start < 0 {
		t.Fatalf("migration 034 has no feature_flag statement")
	}
	rest := text[start:]
	endMarker := ";\n\nCREATE TABLE IF NOT EXISTS feature_flag_event"
	end := strings.Index(rest, endMarker)
	if end < 0 {
		t.Fatalf("migration 034 feature_flag statement boundary changed")
	}
	return strings.TrimSpace(rest[:end+1])
}

func applyFeatureFlagMigration074(t *testing.T, ctx context.Context, dsn string) {
	t.Helper()
	root := featureFlagIntegrationRepoRoot(t)
	python := filepath.Join(root, ".venv", "bin", "python")
	if _, err := os.Stat(python); err != nil {
		python, err = exec.LookPath("python3")
		if err != nil {
			t.Fatalf("find Python for migration 074: %v", err)
		}
	}
	const script = `
import importlib.util
import sys
import clickhouse_connect

path = "src/dev_health_ops/migrations/clickhouse/074_feature_flag_environment_identity.py"
spec = importlib.util.spec_from_file_location("feature_flag_identity", path)
assert spec is not None and spec.loader is not None
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)
client = clickhouse_connect.get_client(dsn=sys.argv[1])
try:
    module.upgrade(client)
finally:
    client.close()
print("FEATURE_FLAG_MIGRATION_074_APPLIED")
`
	command := exec.CommandContext(ctx, python, "-c", script, dsn)
	command.Dir = root
	command.Env = append(os.Environ(), "PYTHONPATH="+filepath.Join(root, "src")+string(os.PathListSeparator)+os.Getenv("PYTHONPATH"))
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("migration 074 against legacy table failed: %v\n%s", err, output)
	}
	if !strings.Contains(string(output), "FEATURE_FLAG_MIGRATION_074_APPLIED") {
		t.Fatalf("migration 074 produced no completion marker:\n%s", output)
	}
}

func featureFlagIntegrationRepoRoot(t *testing.T) string {
	t.Helper()
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("locate feature-flag integration test")
	}
	directory := filepath.Dir(file)
	for {
		migrations := filepath.Join(directory, "src", "dev_health_ops", "migrations", "clickhouse")
		if info, err := os.Stat(migrations); err == nil && info.IsDir() {
			return directory
		}
		parent := filepath.Dir(directory)
		if parent == directory {
			t.Fatalf("no repository root above %s", file)
		}
		directory = parent
	}
}
