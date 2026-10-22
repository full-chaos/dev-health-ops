//go:build integration

package chmigrate_test

import (
	"context"
	"os"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/chmigrate"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

// INVARIANT (CHAOS-8863): the real chain, applied to a fresh database, leaves the three aggregate work-item daily
// tables with row_view as the LAST part of the sorting key, so a legacy row, an 'org' row and a 'scope' row of one
// key are three rows; a writer that does not name the column still inserts, as a legacy row with the empty view;
// and migration 141 is re-runnable: a second run of every statement is a no-op.
func TestWorkItemRowViewMigration(t *testing.T) {
	ctx := context.Background()
	instance, err := containers.StartClickHouse(ctx)
	if err != nil {
		t.Fatalf("start clickhouse: %v", err)
	}
	t.Cleanup(func() {
		if err := instance.Close(context.Background()); err != nil {
			t.Errorf("close clickhouse: %v", err)
		}
	})
	admin := openDatabase(t, instance.URI, "")
	name := scratchDatabase(t, admin)
	conn := openDatabase(t, instance.URI, name)
	db, _, err := chmigrate.NewConnDB(ctx, conn)
	if err != nil {
		t.Fatal(err)
	}
	baseline, err := chmigrate.LoadBaseline()
	if err != nil {
		t.Fatal(err)
	}
	chain, err := chmigrate.LoadChain()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := chmigrate.Upgrade(ctx, db, baseline, chain); err != nil {
		t.Fatalf("real chain: %v", err)
	}
	str := func(query string) string {
		t.Helper()
		var value string
		if err := conn.QueryRow(ctx, query).Scan(&value); err != nil {
			t.Fatalf("%s: %v", query, err)
		}
		return value
	}
	count := func(query string) uint64 {
		t.Helper()
		var value uint64
		if err := conn.QueryRow(ctx, query).Scan(&value); err != nil {
			t.Fatalf("%s: %v", query, err)
		}
		return value
	}

	for table, want := range map[string]string{
		"work_item_metrics_daily":         "org_id, provider, day, work_scope_id, team_id, row_view",
		"work_item_state_durations_daily": "org_id, provider, work_scope_id, team_id, status, day, row_view",
		"work_item_user_metrics_daily":    "org_id, provider, work_scope_id, user_identity, day, row_view",
	} {
		if got := str("SELECT sorting_key FROM system.tables WHERE database = currentDatabase() AND name = '" + table + "'"); got != want {
			t.Fatalf("%s sorting key = %q, want %q", table, got, want)
		}
		if got := str("SELECT type FROM system.columns WHERE database = currentDatabase() AND table = '" + table + "' AND name = 'row_view'"); got != "LowCardinality(String)" {
			t.Fatalf("%s.row_view is %q, want LowCardinality(String)", table, got)
		}
	}

	file, err := os.ReadFile("sql/141_work_item_row_view.sql")
	if err != nil {
		t.Fatal(err)
	}
	statements := chmigrate.SplitStatements(string(file))
	if len(statements) != 3 {
		t.Fatalf("migration 141 has %d statements, want one ALTER for each of the three tables", len(statements))
	}
	for _, statement := range statements {
		if err := db.Exec(ctx, statement); err != nil {
			t.Fatalf("re-run: %v", err)
		}
	}

	// The old writer's column list (no row_view) still inserts: a legacy row.
	const oldColumns = "day, provider, work_scope_id, team_id, team_name, items_started, items_completed, items_started_unassigned, " +
		"items_completed_unassigned, wip_count_end_of_day, wip_unassigned_end_of_day, bug_completed_ratio, story_points_completed, " +
		"computed_at, org_id"
	row := func(view string) string {
		const prefix = "('2026-10-01', 'jira', 'P1', 't1', 'T', 0, 1, 0, 0, 0, 0, 0, 0, '2026-10-01 12:00:00', 'o1'"
		if view == "" {
			return prefix + ")"
		}
		return prefix + ", '" + view + "')"
	}
	if err := conn.Exec(ctx, "INSERT INTO work_item_metrics_daily ("+oldColumns+") VALUES "+row("")); err != nil {
		t.Fatal(err)
	}
	if view := str("SELECT row_view FROM work_item_metrics_daily FINAL WHERE org_id = 'o1'"); view != "" {
		t.Fatalf("an old-writer row has row_view %q, want the empty legacy view", view)
	}
	for _, view := range []string{"org", "scope"} {
		if err := conn.Exec(ctx, "INSERT INTO work_item_metrics_daily ("+oldColumns+", row_view) VALUES "+row(view)); err != nil {
			t.Fatal(err)
		}
	}
	if n := count("SELECT count() FROM work_item_metrics_daily FINAL WHERE org_id = 'o1'"); n != 3 {
		t.Fatalf("a legacy, an org and a scope row of one key = %d rows, want 3", n)
	}
	if err := conn.Exec(ctx, "OPTIMIZE TABLE work_item_metrics_daily FINAL"); err != nil {
		t.Fatal(err)
	}
	if n := count("SELECT count() FROM work_item_metrics_daily WHERE org_id = 'o1'"); n != 3 {
		t.Fatalf("after OPTIMIZE FINAL the table holds %d rows of the key, want 3", n)
	}
}
