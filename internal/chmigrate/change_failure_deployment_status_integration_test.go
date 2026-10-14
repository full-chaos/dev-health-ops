//go:build integration

package chmigrate_test

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/chmigrate"
	clickhousestore "github.com/full-chaos/dev-health-ops/internal/storage/clickhouse"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

// CHAOS-9621: migration 125 adds failed_deployments_status and
// deployments_status_known to repo_change_failure_daily and comments
// deployments_count; rollback/125_*.down.sql drops the two columns and clears
// the comment. The test rehearses both on the pre-125 shape: the upgrade adds
// the columns with a 0 default and keeps the rows, the downgrade drops them and
// keeps the older columns, and the upgrade runs again.
func TestChangeFailureDeploymentStatusMigrationUpgradesAndDowngrades(t *testing.T) {
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
	columns := func() map[string]string {
		t.Helper()
		rows, err := conn.Query(ctx, "SELECT name, type FROM system.columns WHERE database = currentDatabase() AND table = 'repo_change_failure_daily'")
		if err != nil {
			t.Fatal(err)
		}
		defer rows.Close()
		out := map[string]string{}
		for rows.Next() {
			var name, kind string
			if err := rows.Scan(&name, &kind); err != nil {
				t.Fatal(err)
			}
			out[name] = kind
		}
		if err := rows.Err(); err != nil {
			t.Fatal(err)
		}
		return out
	}
	comment := func(column string) string {
		t.Helper()
		var out string
		if err := conn.QueryRow(ctx, "SELECT comment FROM system.columns WHERE database = currentDatabase() AND table = 'repo_change_failure_daily' AND name = ?", column).Scan(&out); err != nil {
			t.Fatal(err)
		}
		return out
	}
	readUint := func(query string) uint64 {
		t.Helper()
		var out uint64
		if err := conn.QueryRow(ctx, "SELECT toUInt64(("+query+"))").Scan(&out); err != nil {
			t.Fatalf("%s: %v", query, err)
		}
		return out
	}

	exec(`CREATE TABLE repo_change_failure_daily (
    org_id String,
    repo_id UUID,
    day Date,
    deployments_count UInt32,
    failed_deployments_native UInt32,
    failed_deployments_heuristic UInt32,
    incidents_direct UInt32,
    incidents_via_deployment UInt32,
    computed_at DateTime64(3, 'UTC')
) ENGINE = ReplacingMergeTree(computed_at)
PARTITION BY toYYYYMM(day)
ORDER BY (org_id, repo_id, day)`)
	exec(`INSERT INTO repo_change_failure_daily VALUES ('o', '00000000-0000-0000-0000-000000000001', '2026-08-01', 7, 1, 2, 3, 4, now64(3))`)
	before := columns()
	if len(before) != 9 {
		t.Fatalf("pre-125 shape has %d columns, want 9", len(before))
	}
	if got := comment("deployments_count"); got != "" {
		t.Fatalf("pre-125 deployments_count comment = %q, want none", got)
	}

	const up = "sql/125_change_failure_deployment_status.sql"
	const down = "rollback/125_change_failure_deployment_status.down.sql"
	run(up)
	after := columns()
	for _, column := range []string{"failed_deployments_status", "deployments_status_known"} {
		if got := after[column]; got != "UInt32" {
			t.Errorf("after the upgrade %s is %q, want UInt32", column, got)
		}
		if got := readUint(`SELECT ` + column + ` FROM repo_change_failure_daily`); got != 0 {
			t.Errorf("a row written before the upgrade reads %s = %d, want the default 0", column, got)
		}
	}
	if !strings.Contains(comment("deployments_count"), "cancelled") {
		t.Errorf("after the upgrade deployments_count has no cancelled-deployment comment: %q", comment("deployments_count"))
	}
	exec(`INSERT INTO repo_change_failure_daily (org_id, repo_id, day, deployments_count, failed_deployments_status, deployments_status_known, computed_at) VALUES ('o', '00000000-0000-0000-0000-000000000002', '2026-08-01', 5, 2, 4, now64(3))`)

	run(down)
	afterDown := columns()
	for _, column := range []string{"failed_deployments_status", "deployments_status_known"} {
		if _, ok := afterDown[column]; ok {
			t.Errorf("after the downgrade %s is still there", column)
		}
	}
	if len(afterDown) != len(before) {
		t.Errorf("after the downgrade the table has %d columns, want the %d it had before", len(afterDown), len(before))
	}
	for name, kind := range before {
		if afterDown[name] != kind {
			t.Errorf("after the downgrade %s is %q, want %q", name, afterDown[name], kind)
		}
	}
	if got := comment("deployments_count"); got != "" {
		t.Errorf("after the downgrade deployments_count comment = %q, want none", got)
	}
	if got := readUint(`SELECT count() FROM repo_change_failure_daily`); got != 2 {
		t.Errorf("after the downgrade the table holds %d rows, want 2 kept", got)
	}
	if got := readUint(`SELECT sum(deployments_count) FROM repo_change_failure_daily`); got != 12 {
		t.Errorf("after the downgrade deployments_count sums to %d, want 12 kept", got)
	}

	run(down)

	run(up)
	again := columns()
	if len(again) != len(after) {
		t.Errorf("after a second upgrade the table has %d columns, want %d", len(again), len(after))
	}
	for name, kind := range after {
		if again[name] != kind {
			t.Errorf("after a second upgrade %s is %q, want %q", name, again[name], kind)
		}
	}
	if got := readUint(`SELECT sum(deployments_status_known) FROM repo_change_failure_daily`); got != 0 {
		t.Errorf("after a second upgrade the dropped status counts read %d, want the default 0", got)
	}
}
