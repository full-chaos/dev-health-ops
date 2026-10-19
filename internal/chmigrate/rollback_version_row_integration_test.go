//go:build integration

package chmigrate_test

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/chmigrate"
	clickhousestore "github.com/full-chaos/dev-health-ops/internal/storage/clickhouse"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/chschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

// rollbackRehearsal is one operator rollback file and what its migration leaves
// in the schema: the type of one column the migration adds or widens, as the
// upgrade leaves it ("*" is any type: the column exists) and as the rollback
// leaves it ("" is no column).
type rollbackRehearsal struct {
	down               string
	table, column      string
	afterUp, afterDown string
}

var rollbackRehearsals = []rollbackRehearsal{
	{"124_team_daily_counters_float64.down.sql", "team_metrics_daily", "commits_count", "Float64", "UInt32"},
	{"125_change_failure_deployment_status.down.sql", "repo_change_failure_daily", "failed_deployments_status", "*", ""},
	{"126_deployments_pull_request_tier.down.sql", "deployments", "pull_request_tier", "*", ""},
	{"127_projects_target_provenance.down.sql", "projects", "target_kind", "*", ""},
	{"128_ai_impact_counts_float64.down.sql", "ai_impact_metrics_daily", "prs_total", "Float64", "UInt32"},
	{"129_testops_test_flake_counts.down.sql", "testops_test_metrics_daily", "distinct_cases", "*", ""},
	{"130_work_unit_issue_size.down.sql", "work_unit_issue_size", "issue_size", "*", ""},
	{"131_deployment_incident_edges_repo_key.down.sql", "work_graph_deployment_incident_edges", "version_rank", "*", ""},
	{"132_pr_commit_native_wins.down.sql", "work_graph_pr_commit", "version_rank", "*", ""},
	{"137_teams_provider_identity.down.sql", "teams", "provider_team_id", "*", ""},
	{"134_repos_provider_repo_id.down.sql", "repos", "provider_repo_id", "*", ""},
	{"138_repos_identity_path.down.sql", "repos", "identity_path", "*", ""},
	{"135_work_item_metrics_nullable.down.sql", "issue_type_metrics_daily", "lead_p50_hours", "Nullable(Float64)", "Float64"},
}

// Every operator rollback file is rehearsed through the production migrator: the
// upgrade records the migration's version and makes its change, the rollback
// file undoes the change and removes the version row, and the next upgrade makes
// the change again. With the version row left, that last upgrade reads the
// migration as applied and skips it.
func TestRollbackFilesLeaveNoVersionRowSoTheNextUpgradeAppliesTheMigrationAgain(t *testing.T) {
	files, err := filepath.Glob("rollback/*.down.sql")
	if err != nil {
		t.Fatal(err)
	}
	rehearsed := map[string]bool{}
	for _, rehearsal := range rollbackRehearsals {
		rehearsed[rehearsal.down] = true
	}
	for _, file := range files {
		if !rehearsed[filepath.Base(file)] {
			t.Errorf("%s has no row in rollbackRehearsals: a rollback file is rehearsed through the migrator before it ships", filepath.Base(file))
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	defer cancel()
	instance, err := containers.StartClickHouse(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		closeCtx, closeCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer closeCancel()
		_ = instance.Close(closeCtx)
	})
	conn, err := clickhousestore.Open(ctx, clickhousestore.DefaultConfig(instance.URI))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	chschema.Apply(ctx, t, instance)

	columnType := func(table, column string) string {
		t.Helper()
		var out string
		if err := conn.QueryRow(ctx,
			"SELECT ifNull(max(type), '') FROM system.columns WHERE database = currentDatabase() AND table = ? AND name = ?",
			table, column).Scan(&out); err != nil {
			t.Fatal(err)
		}
		return out
	}
	versionRows := func(version string) uint64 {
		t.Helper()
		var n uint64
		if err := conn.QueryRow(ctx, "SELECT count() FROM schema_migrations WHERE version = ?", version).Scan(&n); err != nil {
			t.Fatal(err)
		}
		return n
	}
	matches := func(got, want string) bool {
		if want == "*" {
			return got != ""
		}
		return got == want
	}

	for _, rehearsal := range rollbackRehearsals {
		rehearsal := rehearsal
		version := strings.TrimSuffix(rehearsal.down, ".down.sql") + ".sql"
		t.Run(rehearsal.down, func(t *testing.T) {
			if got := columnType(rehearsal.table, rehearsal.column); !matches(got, rehearsal.afterUp) || versionRows(version) == 0 {
				t.Fatalf("after the upgrade: %s.%s is %q (want %q), version rows %d (want at least 1)", rehearsal.table, rehearsal.column, got, rehearsal.afterUp, versionRows(version))
			}
			body, err := os.ReadFile(filepath.Join("rollback", rehearsal.down))
			if err != nil {
				t.Fatal(err)
			}
			for _, statement := range chmigrate.SplitStatements(string(body)) {
				if err := conn.Exec(ctx, statement); err != nil {
					t.Fatalf("%s: %v", statement, err)
				}
			}
			if got := columnType(rehearsal.table, rehearsal.column); !matches(got, rehearsal.afterDown) || versionRows(version) != 0 {
				t.Fatalf("after the rollback: %s.%s is %q (want %q), version rows %d (want 0)", rehearsal.table, rehearsal.column, got, rehearsal.afterDown, versionRows(version))
			}
			chschema.Apply(ctx, t, instance)
			if got := columnType(rehearsal.table, rehearsal.column); !matches(got, rehearsal.afterUp) || versionRows(version) == 0 {
				t.Fatalf("the upgrade after the rollback: %s.%s is %q (want %q), version rows %d (want at least 1)", rehearsal.table, rehearsal.column, got, rehearsal.afterUp, versionRows(version))
			}
		})
	}
}
