//go:build integration

package report

import (
	"context"
	"testing"
	"time"

	clickhousestore "github.com/full-chaos/dev-health-ops/internal/storage/clickhouse"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/chschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

// CHAOS-8863: the report source of work_item_metrics_daily takes the org view. One key holds a legacy row that a later
// run retracted with the NEWEST row of the key (a row of zeros), an org row and a scope row. LIMIT 1 BY without
// row_view would keep the retraction and the key would read as dead; with it the org row is read. A legacy-only day
// reads as before.
func TestReportSourceOfWorkItemMetricsTakesTheOrgViewWithoutADeadKey(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	instance, err := containers.StartClickHouse(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = instance.Close(context.Background()) }()
	chschema.Apply(ctx, t, instance)
	conn, err := clickhousestore.Open(ctx, clickhousestore.DefaultConfig(instance.URI))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	insert := `INSERT INTO work_item_metrics_daily
		(day, provider, work_scope_id, team_id, team_name, items_started, items_completed, items_started_unassigned,
		 items_completed_unassigned, wip_count_end_of_day, wip_unassigned_end_of_day, bug_completed_ratio,
		 story_points_completed, computed_at, org_id, row_view)
		VALUES (?, 'jira', 'S', 't1', 'T', 0, ?, 0, 0, 0, 0, 0, 0, ?, 'org-8863', ?)`
	for _, row := range []struct {
		day, at   string
		completed uint32
		view      string
	}{
		{"2026-01-02", "2026-01-02 09:00:00", 2, ""},
		{"2026-01-03", "2026-01-03 09:00:00", 10, ""},
		{"2026-01-03", "2026-01-03 10:00:00", 4, "org"},
		{"2026-01-03", "2026-01-03 10:00:00", 6, "scope"},
		{"2026-01-03", "2026-01-03 11:00:00", 0, ""},
	} {
		if err := conn.Exec(ctx, insert, row.day, row.completed, row.at, row.view); err != nil {
			t.Fatal(err)
		}
	}

	var total, days uint64
	query := "SELECT sum(items_completed), uniqExact(day) FROM " + dedupFromSource("work_item_metrics_daily") + " WHERE org_id = 'org-8863'"
	if err := conn.QueryRow(ctx, query).Scan(&total, &days); err != nil {
		t.Fatal(err)
	}
	if total != 6 || days != 2 {
		t.Fatalf("report source: sum=%d days=%d, want 6 = 2 (legacy-only day) + 4 (the org row) over 2 days", total, days)
	}
}
