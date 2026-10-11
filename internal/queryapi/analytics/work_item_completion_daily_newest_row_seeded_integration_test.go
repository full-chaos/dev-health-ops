//go:build integration

package analytics

import (
	"context"
	"testing"
	"time"

	stdclickhouse "github.com/ClickHouse/clickhouse-go/v2"
	dhclickhouse "github.com/full-chaos/dev-health-go/clickhouse"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/chschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

// TestBreakdownOverWorkItemCompletionDailyTakesTheNewestRowOfEachKey is the
// reader half of the append-only contract of work_item_completion_daily
// (CHAOS-8810), for the default analytics source
// (workItemCompletionDedupSource), through the real breakdown compile and
// execute path, on the schema of the migration chain.
//
// The store holds, for one day, the rows of a full sync, of one hourly sync
// unit with one changed item, and of the daily job:
//
//	key (repo, team-a)
//	  10:00  3 completed   the full sync
//	  11:00  1 completed   the hourly unit: only the one item it fetched
//	  11:05  4 completed   the daily job, from stored rows
//	key (repo, team-x)
//	  10:00  2 completed   the full sync
//	  11:05  0 completed   the daily job: the completion left this key
//
// The breakdown by theme must return 4 for quality. A read with no dedupe
// returns 10.
func TestBreakdownOverWorkItemCompletionDailyTakesTheNewestRowOfEachKey(t *testing.T) {
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
	client, err := dhclickhouse.NewClickHouseQueryClientWithOptions(dhclickhouse.Options{DSN: inst.URI})
	if err != nil {
		t.Fatalf("construct ClickHouse query client: %v", err)
	}
	defer func() { _ = client.Close() }()

	const org = "org-8810-analytics"
	const repo = "11111111-1111-4111-8111-111111111111"
	day := time.Date(2026, 8, 3, 0, 0, 0, 0, time.UTC)
	at := func(hour, minute int) time.Time {
		return day.Add(time.Duration(hour)*time.Hour + time.Duration(minute)*time.Minute)
	}
	insert := `INSERT INTO work_item_completion_daily
		(repo_id, day, team_id, delivery_units,
		 work_items_completed, prs_merged, churn_loc, cycle_p50_hours, computed_at, org_id)
		VALUES (?, ?, ?, ?, ?, 0, 0, 0, ?, ?)`
	for _, row := range []struct {
		team       string
		completed  uint32
		computedAt time.Time
	}{
		{"team-a", 3, at(10, 0)},
		{"team-a", 1, at(11, 0)},
		{"team-a", 4, at(11, 5)},
		{"team-x", 2, at(10, 0)},
		{"team-x", 0, at(11, 5)},
	} {
		if err := conn.Exec(ctx, insert, repo, day, row.team, row.completed, row.completed, row.computedAt, org); err != nil {
			t.Fatalf("insert %+v: %v", row, err)
		}
	}

	request := BreakdownRequest{
		Dimension: DimensionTeam,
		Measure:   MeasureCount,
		StartDate: mustGraphQLDate("2026-08-01"),
		EndDate:   mustGraphQLDate("2026-08-08"),
		TopN:      10,
	}
	query, err := compileBreakdownOver(request, "team_id", org, queryTimeoutSecs, false, nil)
	if err != nil {
		t.Fatalf("CompileBreakdown: %v", err)
	}
	result, err := ExecuteBreakdown(ctx, client, org, query, "theme", "count")
	if err != nil {
		t.Fatalf("ExecuteBreakdown: %v", err)
	}
	if len(result.Items) != 1 || result.Items[0].Key != "team-a" {
		t.Fatalf("breakdown items = %+v, want one item of team-a (team-x holds a retraction row only)", result.Items)
	}
	if result.Items[0].Value == nil {
		t.Fatal("team-a count is absent, want 4")
	}
	if *result.Items[0].Value != 4 {
		t.Fatalf("team-a count = %v, want 4: the newest row of the key. 8 is every row summed", *result.Items[0].Value)
	}
}

// TestCycleTimeOverWorkItemCompletionDailyLeavesOutRowsWithNoCompletedItem
// holds the cycle time measure of the default analytics source against a row
// of zeros, through the real breakdown compile and execute path, on the
// schema of the migration chain.
//
// The store holds, for one day:
//
//	key (repo, team-a)
//	  10:00  1 completed, cycle p50 34 h
//	key (repo, team-x)
//	  10:00  1 completed, cycle p50 20 h   the sync
//	  11:05  0 completed, cycle p50 0 h    the daily job: the completion left this key
//	key (repo, team-y)
//	  10:00  1 completed, cycle p50 12 h   the sync
//	  11:05  0 completed, cycle p50 0 h    the daily job: the completion left this key
//
// One key of the quality area has a completed item, with a cycle time of 34
// hours: the cycle time of the area is 34. A mean over every newest row is 17
// (the row of zeros counted as a measured 0), and a mean over every row with
// a completion, old or new, is 27. The one key of the security area holds a
// row of zeros as its newest row: a retraction row, which reads as no row, so
// the area is not a bucket at all.
func TestCycleTimeOverWorkItemCompletionDailyLeavesOutRowsWithNoCompletedItem(t *testing.T) {
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
	client, err := dhclickhouse.NewClickHouseQueryClientWithOptions(dhclickhouse.Options{DSN: inst.URI})
	if err != nil {
		t.Fatalf("construct ClickHouse query client: %v", err)
	}
	defer func() { _ = client.Close() }()

	const org = "org-8810-cycle-time"
	const repo = "11111111-1111-4111-8111-111111111111"
	day := time.Date(2026, 8, 3, 0, 0, 0, 0, time.UTC)
	at := func(hour, minute int) time.Time {
		return day.Add(time.Duration(hour)*time.Hour + time.Duration(minute)*time.Minute)
	}
	insert := `INSERT INTO work_item_completion_daily
		(repo_id, day, team_id, delivery_units,
		 work_items_completed, prs_merged, churn_loc, cycle_p50_hours, computed_at, org_id)
		VALUES (?, ?, ?, ?, ?, 0, 0, ?, ?, ?)`
	for _, row := range []struct {
		team       string
		completed  uint32
		cycleP50   float64
		computedAt time.Time
	}{
		{"team-a", 1, 34, at(10, 0)},
		{"team-x", 1, 20, at(10, 0)},
		{"team-x", 0, 0, at(11, 5)},
		{"team-y", 1, 12, at(10, 0)},
		{"team-y", 0, 0, at(11, 5)},
	} {
		if err := conn.Exec(ctx, insert, repo, day, row.team, row.completed, row.completed,
			row.cycleP50, row.computedAt, org); err != nil {
			t.Fatalf("insert %+v: %v", row, err)
		}
	}

	request := BreakdownRequest{
		Dimension: DimensionTeam,
		Measure:   MeasureCycleTimeHours,
		StartDate: mustGraphQLDate("2026-08-01"),
		EndDate:   mustGraphQLDate("2026-08-08"),
		TopN:      10,
	}
	query, err := compileBreakdownOver(request, "team_id", org, queryTimeoutSecs, false, nil)
	if err != nil {
		t.Fatalf("CompileBreakdown: %v", err)
	}
	result, err := ExecuteBreakdown(ctx, client, org, query, "team", "cycle_time_hours")
	if err != nil {
		t.Fatalf("ExecuteBreakdown: %v", err)
	}
	values := map[string]*float64{}
	for _, item := range result.Items {
		values[item.Key] = item.Value
	}
	if len(values) != 1 {
		t.Fatalf("breakdown items = %+v, want team-a only", result.Items)
	}
	teamA, found := values["team-a"]
	if !found || teamA == nil {
		t.Fatalf("team-a cycle time is absent (items %+v), want 34", result.Items)
	}
	if *teamA != 34 {
		t.Fatalf("team-a cycle time = %v, want 34: its one key with a completed item", *teamA)
	}
	// The keys of team-x and team-y hold a retraction row as their newest
	// row. It reads as no row, so the team is not a bucket: it has no cycle
	// time of 0 and no bucket with an absent value either.
	for _, retracted := range []string{"team-x", "team-y"} {
		if value, found := values[retracted]; found {
			t.Fatalf("%s is a bucket (value %v), want no bucket: its one key holds a retraction row", retracted, value)
		}
	}
}
