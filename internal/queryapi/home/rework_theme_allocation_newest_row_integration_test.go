//go:build integration

package home

import (
	"context"
	"testing"
	"time"

	stdclickhouse "github.com/ClickHouse/clickhouse-go/v2"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/chquery"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/chschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

// TestFetchReworkThemeAllocationTakesTheNewestRowOfEachKey is the reader half
// of the append-only contract of investment_metrics_daily (CHAOS-8810), for
// the "Rework by theme" read. The store holds, for one day, the rows of a full
// sync, of one hourly sync unit with one changed item, and of the daily job:
//
//	key (repo, no team, quality, general)
//	  10:00  3 completed   the full sync
//	  11:00  1 completed   the hourly unit: only the one item it fetched
//	  11:05  4 completed   the daily job, from stored rows
//	key (repo, team-x, quality, general)
//	  10:00  2 completed   the full sync
//	  11:05  0 completed   the daily job: the completion left this key
//
// The reader must return an allocation of 4 for the quality theme. A read with
// no dedupe returns 10.
func TestFetchReworkThemeAllocationTakesTheNewestRowOfEachKey(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()

	inst, err := containers.StartClickHouse(ctx)
	if err != nil {
		t.Fatalf("start ClickHouse test dependency: %v", err)
	}
	defer func() { _ = inst.Close(context.Background()) }()
	chschema.Apply(ctx, t, inst)

	opts, err := stdclickhouse.ParseDSN(inst.URI)
	if err != nil {
		t.Fatalf("parse ClickHouse DSN: %v", err)
	}
	conn, err := stdclickhouse.Open(opts)
	if err != nil {
		t.Fatalf("open raw ClickHouse connection: %v", err)
	}
	defer func() { _ = conn.Close() }()
	client, err := chquery.NewProductionClient(inst.URI)
	if err != nil {
		t.Fatalf("construct ClickHouse query client: %v", err)
	}
	defer func() { _ = client.Close() }()

	const org = "org-8810-rework-theme"
	const repo = "11111111-1111-4111-8111-111111111111"
	day := time.Date(2026, 8, 3, 0, 0, 0, 0, time.UTC)
	at := func(hour, minute int) time.Time {
		return day.Add(time.Duration(hour)*time.Hour + time.Duration(minute)*time.Minute)
	}
	insert := `INSERT INTO investment_metrics_daily
		(repo_id, day, team_id, investment_area, project_stream, delivery_units,
		 work_items_completed, prs_merged, churn_loc, cycle_p50_hours, computed_at, org_id)
		VALUES (?, ?, ?, 'quality', 'general', ?, ?, 0, 0, 0, ?, ?)`
	for _, row := range []struct {
		team       string
		completed  uint32
		computedAt time.Time
	}{
		{"", 3, at(10, 0)},
		{"", 1, at(11, 0)},
		{"", 4, at(11, 5)},
		{"team-x", 2, at(10, 0)},
		{"team-x", 0, at(11, 5)},
	} {
		if err := conn.Exec(ctx, insert, repo, day, row.team, row.completed, row.completed, row.computedAt, org); err != nil {
			t.Fatalf("insert %+v: %v", row, err)
		}
	}

	got, err := fetchReworkThemeAllocation(ctx, client, day, day.AddDate(0, 0, 1), "", nil, "", nil, org)
	if err != nil {
		t.Fatalf("fetchReworkThemeAllocation: %v", err)
	}
	if len(got) != 1 || got[0].Theme != "quality" {
		t.Fatalf("fetchReworkThemeAllocation = %+v, want one row of the quality theme", got)
	}
	if got[0].Allocation != 4 {
		t.Fatalf("quality allocation = %v, want 4: the newest row of each key (4 + 0). 10 is every row summed", got[0].Allocation)
	}
}
