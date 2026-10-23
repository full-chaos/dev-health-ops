//go:build integration

package capacityforecast

import (
	"context"
	"reflect"
	"testing"
	"time"

	stdclickhouse "github.com/ClickHouse/clickhouse-go/v2"
	dhclickhouse "github.com/full-chaos/dev-health-go/clickhouse"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/chschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

// CHAOS-8863: one key of work_item_metrics_daily holds a legacy row that a later run retracted (a row of zeros), an
// org row and a scope row. The capacity reads must take the org row only: the legacy row is retracted, and the scope
// row counts the same items again. A day that only has a legacy row (not recomputed) still reads as it did.
func TestCapacityReadsTakeTheOrgViewOfWorkItemMetrics(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	ch, err := containers.StartClickHouse(ctx)
	if err != nil {
		t.Fatalf("start ClickHouse test dependency: %v", err)
	}
	defer func() { _ = ch.Close(context.Background()) }()
	chschema.Apply(ctx, t, ch)

	options, err := stdclickhouse.ParseDSN(ch.URI)
	if err != nil {
		t.Fatalf("parse ClickHouse DSN: %v", err)
	}
	admin, err := stdclickhouse.Open(options)
	if err != nil {
		t.Fatalf("open ClickHouse admin connection: %v", err)
	}
	defer func() { _ = admin.Close() }()

	const org = "org-8863"
	today := time.Date(2026, 10, 20, 0, 0, 0, 0, time.UTC)
	older := today.AddDate(0, 0, -2)
	newer := today.AddDate(0, 0, -1)
	first := time.Date(2026, 10, 20, 1, 0, 0, 0, time.UTC)
	second := first.Add(time.Hour)
	third := second.Add(time.Hour)

	insert := `INSERT INTO work_item_metrics_daily
		(day, provider, work_scope_id, team_id, team_name, items_started, items_completed, items_started_unassigned,
		 items_completed_unassigned, wip_count_end_of_day, wip_unassigned_end_of_day, bug_completed_ratio,
		 story_points_completed, computed_at, org_id, row_view)
		VALUES (?, 'jira', 'S', 't1', 'T', 0, ?, 0, 0, ?, 0, 0, 0, ?, ?, ?)`
	for _, row := range []struct {
		day            time.Time
		completed, wip uint32
		at             time.Time
		view           string
		description    string
	}{
		{older, 2, 1, first, "", "legacy row of a day that was never recomputed"},
		{newer, 7, 5, first, "", "legacy row of the recomputed day"},
		{newer, 0, 0, third, "", "the retraction of that legacy row, the NEWEST row of the day"},
		{newer, 4, 3, second, "org", "org row of the recomputed day"},
		{newer, 6, 4, second, "scope", "scope row of the recomputed day"},
	} {
		if err := admin.Exec(ctx, insert, row.day, row.completed, row.wip, row.at, org, row.view); err != nil {
			t.Fatalf("insert %s: %v", row.description, err)
		}
	}

	client, err := dhclickhouse.NewClickHouseQueryClientWithOptions(dhclickhouse.Options{DSN: ch.URI})
	if err != nil {
		t.Fatalf("construct query client: %v", err)
	}
	defer func() { _ = client.Close() }()

	history, err := loadThroughput(ctx, client, org, []string{"t1"}, nil, 10, today)
	if err != nil {
		t.Fatalf("loadThroughput: %v", err)
	}
	if want := []int{2, 4}; !reflect.DeepEqual(history, want) {
		t.Fatalf("throughput history = %v, want %v (the org row of the recomputed day, the legacy row of the other)", history, want)
	}
	backlog, err := loadBacklog(ctx, client, org, []string{"t1"}, nil)
	if err != nil {
		t.Fatalf("loadBacklog: %v", err)
	}
	if backlog != 3 {
		t.Fatalf("backlog = %d, want 3 (the org row of the newest day)", backlog)
	}
}
