//go:build integration

package chmigrate_test

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/chmigrate"
	clickhousestore "github.com/full-chaos/dev-health-ops/internal/storage/clickhouse"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

// CHAOS-9737: migration 129 adds testops_test_metrics_daily.flake_cases and distinct_cases and
// rollback/129_*.down.sql drops them. A row written before the upgrade reads NULL counts (not 0), a row written after
// carries them, the downgrade keeps flake_rate, and the upgrade runs again (and twice).
func TestTestopsTestFlakeCountsMigrationUpgradesAndDowngrades(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
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
	exec := func(statement string) {
		t.Helper()
		if err := conn.Exec(ctx, statement); err != nil {
			t.Fatalf("%s: %v", statement, err)
		}
	}
	run := func(path string) {
		t.Helper()
		body, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		for _, statement := range chmigrate.SplitStatements(string(body)) {
			exec(statement)
		}
	}
	columnType := func(column string) string {
		t.Helper()
		var out string
		if err := conn.QueryRow(ctx, "SELECT type FROM system.columns WHERE database = currentDatabase() AND table = 'testops_test_metrics_daily' AND name = ?", column).Scan(&out); err != nil {
			return ""
		}
		return out
	}
	readString := func(query string) string {
		t.Helper()
		var out string
		if err := conn.QueryRow(ctx, query).Scan(&out); err != nil {
			t.Fatalf("%s: %v", query, err)
		}
		return out
	}

	exec(`CREATE TABLE testops_test_metrics_daily (
    repo_id UUID, day Date, flake_rate Float64, org_id String
) ENGINE = MergeTree ORDER BY (org_id, repo_id, day)`)
	exec(`INSERT INTO testops_test_metrics_daily (repo_id, day, flake_rate, org_id) VALUES ('c7198fbc-1945-3717-05d8-eb78866b4e79', '2026-10-01', 0.25, 'o')`)

	const up = "sql/129_testops_test_flake_counts.sql"
	const down = "rollback/129_testops_test_flake_counts.down.sql"
	versionRows := useSchemaMigrations(t, ctx, conn, "129_testops_test_flake_counts.sql")
	run(up)
	for _, column := range []string{"flake_cases", "distinct_cases"} {
		if got := columnType(column); got != "Nullable(UInt32)" {
			t.Fatalf("after the upgrade %s is %q, want Nullable(UInt32)", column, got)
		}
	}
	if got := readString(`SELECT concat(toString(flake_rate), '|', toString(isNull(flake_cases)), '|', toString(isNull(distinct_cases))) FROM testops_test_metrics_daily WHERE day = '2026-10-01'`); got != "0.25|1|1" {
		t.Errorf("a row written before the upgrade reads %q, want 0.25 with NULL counts", got)
	}
	exec(`INSERT INTO testops_test_metrics_daily (repo_id, day, flake_rate, flake_cases, distinct_cases, org_id) VALUES ('c7198fbc-1945-3717-05d8-eb78866b4e79', '2026-10-02', 0.25, 2, 8, 'o')`)
	if got := readString(`SELECT concat(toString(flake_cases), '/', toString(distinct_cases)) FROM testops_test_metrics_daily WHERE day = '2026-10-02'`); got != "2/8" {
		t.Errorf("counts written after the upgrade read %q, want 2/8", got)
	}
	run(up)

	run(down)
	if n := versionRows(); n != 0 {
		t.Errorf("after the rollback the version row is still recorded (%d rows)", n)
	}
	for _, column := range []string{"flake_cases", "distinct_cases"} {
		if got := columnType(column); got != "" {
			t.Errorf("after the downgrade %s is still %q", column, got)
		}
	}
	if got := readString(`SELECT toString(flake_rate) FROM testops_test_metrics_daily WHERE day = '2026-10-02'`); got != "0.25" {
		t.Errorf("flake_rate after the downgrade = %q, want 0.25 unchanged", got)
	}
	run(down)
	if n := versionRows(); n != 0 {
		t.Errorf("after the rollback the version row is still recorded (%d rows)", n)
	}

	run(up)
	if got := columnType("flake_cases"); got != "Nullable(UInt32)" {
		t.Errorf("after a second upgrade flake_cases is %q, want Nullable(UInt32)", got)
	}
}
