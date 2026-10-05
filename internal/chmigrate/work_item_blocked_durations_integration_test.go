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

// INVARIANT (CHAOS-8489): a blocked-duration daily row is a snapshot per
// (org, day, provider, work item). The latest snapshot carries its scope and
// team with its duration. A zero snapshot replaces an earlier positive result,
// so a reader that groups by the stable identity before filtering positives
// does not report an item that is no longer blocked.
func TestWorkItemBlockedDurationsMigrationReplacesAStalePositive(t *testing.T) {
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

	sql, err := os.ReadFile("sql/104_work_item_blocked_durations.sql")
	if err != nil {
		t.Fatal(err)
	}
	statements := chmigrate.SplitStatements(string(sql))
	if len(statements) != 1 {
		t.Fatalf("migration 104 has %d statements, want its one table", len(statements))
	}
	for _, statement := range statements {
		exec(statement)
	}

	insert := func(org, scope, teamID, teamName, item, duration, computedAt string) {
		t.Helper()
		exec(`INSERT INTO work_item_blocked_durations_daily
    (day, provider, work_scope_id, team_id, team_name, work_item_id, duration_hours, computed_at, org_id)
VALUES ('2026-10-04', 'github', '` + scope + `', '` + teamID + `', '` + teamName + `', '` + item + `', ` + duration + `, '` + computedAt + `', '` + org + `')`)
	}
	// The first row becomes stale when the item moves and its next compute
	// finds no blocked interval. Another organization may use the same item id.
	insert("org-a", "old-scope", "old-team", "Old Team", "gh:acme/api#42", "4", "2026-10-04 01:00:00.000")
	insert("org-a", "new-scope", "new-team", "New Team", "gh:acme/api#42", "0", "2026-10-04 02:00:00.000")
	insert("org-b", "other-scope", "other-team", "Other Team", "gh:acme/api#42", "6", "2026-10-04 02:00:00.000")

	var scope, teamID, teamName string
	var duration float64
	if err := conn.QueryRow(ctx, `
SELECT work_scope_id, team_id, team_name, duration_hours
FROM work_item_blocked_durations_daily FINAL
WHERE org_id = 'org-a' AND day = '2026-10-04' AND provider = 'github' AND work_item_id = 'gh:acme/api#42'`,
	).Scan(&scope, &teamID, &teamName, &duration); err != nil {
		t.Fatal(err)
	}
	if scope != "new-scope" || teamID != "new-team" || teamName != "New Team" || duration != 0 {
		t.Fatalf("latest org-a snapshot = (%q, %q, %q, %v), want the moved zero snapshot", scope, teamID, teamName, duration)
	}

	positiveCount := func(org string) uint64 {
		t.Helper()
		var count uint64
		if err := conn.QueryRow(ctx, `
SELECT count()
FROM (
    SELECT argMax(duration_hours, computed_at) AS duration_hours
    FROM work_item_blocked_durations_daily
    WHERE org_id = ? AND day = '2026-10-04'
    GROUP BY day, provider, work_item_id
)
WHERE duration_hours > 0`, org).Scan(&count); err != nil {
			t.Fatal(err)
		}
		return count
	}
	if got := positiveCount("org-a"); got != 0 {
		t.Fatalf("org-a positive count = %d, want 0 after its zero snapshot", got)
	}
	if got := positiveCount("org-b"); got != 1 {
		t.Fatalf("org-b positive count = %d, want 1 for its independent row", got)
	}

	// The migration is re-runnable: the table and all snapshots remain valid.
	for _, statement := range statements {
		exec(statement)
	}
	if got := positiveCount("org-a"); got != 0 {
		t.Fatalf("after migration rerun, org-a positive count = %d, want 0", got)
	}
}
