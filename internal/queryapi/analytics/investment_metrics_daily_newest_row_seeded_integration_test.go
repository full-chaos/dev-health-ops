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

// TestBreakdownOverInvestmentMetricsDailyTakesTheNewestRowOfEachKey is the
// reader half of the append-only contract of investment_metrics_daily
// (CHAOS-8810), for the default analytics source
// (investmentMetricsDailyDedupSource), through the real breakdown compile and
// execute path, on the schema of the migration chain.
//
// The store holds, for one day, the rows of a full sync, of one hourly sync
// unit with one changed item, and of the daily job:
//
//	key (repo, no team, quality, general)
//	  10:00  3 completed   the full sync
//	  11:00  1 completed   the hourly unit: only the one item it fetched
//	  11:05  4 completed   the daily job, from stored rows
//	key (repo, team-x, quality, general)
//	  10:00  2 completed   the full sync
//	  11:05  0 completed   the daily job: the completion left this key
//
// The breakdown by theme must return 4 for quality. A read with no dedupe
// returns 10.
func TestBreakdownOverInvestmentMetricsDailyTakesTheNewestRowOfEachKey(t *testing.T) {
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

	request := BreakdownRequest{
		Dimension: DimensionTheme,
		Measure:   MeasureCount,
		StartDate: mustGraphQLDate("2026-08-01"),
		EndDate:   mustGraphQLDate("2026-08-08"),
		TopN:      10,
	}
	query, err := CompileBreakdown(request, org, queryTimeoutSecs, false, nil)
	if err != nil {
		t.Fatalf("CompileBreakdown: %v", err)
	}
	result, err := ExecuteBreakdown(ctx, client, org, query, "theme", "count")
	if err != nil {
		t.Fatalf("ExecuteBreakdown: %v", err)
	}
	if len(result.Items) != 1 || result.Items[0].Key != "quality" {
		t.Fatalf("breakdown items = %+v, want one item of the quality area", result.Items)
	}
	if result.Items[0].Value == nil {
		t.Fatal("quality count is absent, want 4")
	}
	if *result.Items[0].Value != 4 {
		t.Fatalf("quality count = %v, want 4: the newest row of each key (4 + 0). 10 is every row summed", *result.Items[0].Value)
	}
}

// TestCycleTimeOverInvestmentMetricsDailyLeavesOutRowsWithNoCompletedItem
// holds the cycle time measure of the default analytics source against a row
// of zeros, through the real breakdown compile and execute path, on the
// schema of the migration chain.
//
// The store holds, for one day:
//
//	key (repo, no team, quality, general)
//	  10:00  1 completed, cycle p50 34 h
//	key (repo, team-x, quality, general)
//	  10:00  1 completed, cycle p50 20 h   the sync
//	  11:05  0 completed, cycle p50 0 h    the daily job: the completion left this key
//	key (repo, no team, security, general)
//	  10:00  1 completed, cycle p50 12 h   the sync
//	  11:05  0 completed, cycle p50 0 h    the daily job: the completion left this key
//
// One key of the quality area has a completed item, with a cycle time of 34
// hours: the cycle time of the area is 34. A mean over every newest row is 17
// (the row of zeros counted as a measured 0), and a mean over every row with
// a completion, old or new, is 27. The one key of the security area holds a
// row of zeros as its newest row: a retraction row, which reads as no row, so
// the area is not a bucket at all.
func TestCycleTimeOverInvestmentMetricsDailyLeavesOutRowsWithNoCompletedItem(t *testing.T) {
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
	insert := `INSERT INTO investment_metrics_daily
		(repo_id, day, team_id, investment_area, project_stream, delivery_units,
		 work_items_completed, prs_merged, churn_loc, cycle_p50_hours, computed_at, org_id)
		VALUES (?, ?, ?, ?, 'general', ?, ?, 0, 0, ?, ?, ?)`
	for _, row := range []struct {
		team       string
		area       string
		completed  uint32
		cycleP50   float64
		computedAt time.Time
	}{
		{"", "quality", 1, 34, at(10, 0)},
		{"team-x", "quality", 1, 20, at(10, 0)},
		{"team-x", "quality", 0, 0, at(11, 5)},
		{"", "security", 1, 12, at(10, 0)},
		{"", "security", 0, 0, at(11, 5)},
	} {
		if err := conn.Exec(ctx, insert, repo, day, row.team, row.area, row.completed, row.completed,
			row.cycleP50, row.computedAt, org); err != nil {
			t.Fatalf("insert %+v: %v", row, err)
		}
	}

	request := BreakdownRequest{
		Dimension: DimensionTheme,
		Measure:   MeasureCycleTimeHours,
		StartDate: mustGraphQLDate("2026-08-01"),
		EndDate:   mustGraphQLDate("2026-08-08"),
		TopN:      10,
	}
	query, err := CompileBreakdown(request, org, queryTimeoutSecs, false, nil)
	if err != nil {
		t.Fatalf("CompileBreakdown: %v", err)
	}
	result, err := ExecuteBreakdown(ctx, client, org, query, "theme", "cycle_time_hours")
	if err != nil {
		t.Fatalf("ExecuteBreakdown: %v", err)
	}
	values := map[string]*float64{}
	for _, item := range result.Items {
		values[item.Key] = item.Value
	}
	if len(values) != 1 {
		t.Fatalf("breakdown items = %+v, want the quality area only", result.Items)
	}
	quality, found := values["quality"]
	if !found || quality == nil {
		t.Fatalf("quality cycle time is absent (items %+v), want 34", result.Items)
	}
	if *quality != 34 {
		t.Fatalf("quality cycle time = %v, want 34: the one key with a completed item. "+
			"17 counts the row of zeros as a measured 0; 27 counts the older row of the key that lost its completion",
			*quality)
	}
	// The one key of the security area holds a retraction row as its newest
	// row. It reads as no row, so the area is not a bucket: it has no cycle
	// time of 0 and no bucket with an absent value either.
	if security, found := values["security"]; found {
		t.Fatalf("security area is a bucket (value %v), want no bucket: its one key holds a retraction row", security)
	}
}
