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

// TestChurnLOCReadersServeNullForNoDataAndZeroForData holds the two analytics readers of
// work_item_completion_daily.churn_loc (breakdown and time series) against the rule of CHAOS-9920: the column is
// Nullable, NULL is "no sized pull request" and is served as null, a stored 0 is data and is served as 0. The
// driver scans a NULL into a plain float64 as 0 with no error, so a reader on a plain destination serves 0 for
// the NULL team and these assertions go red.
func TestChurnLOCReadersServeNullForNoDataAndZeroForData(t *testing.T) {
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

	const org = "org-9920-churn"
	const repo = "11111111-1111-4111-8111-111111111111"
	day := time.Date(2026, 8, 3, 0, 0, 0, 0, time.UTC)
	computedAt := day.Add(10 * time.Hour)
	insert := `INSERT INTO work_item_completion_daily
		(repo_id, day, team_id, delivery_units, work_items_completed, prs_merged, churn_loc, cycle_p50_hours, computed_at, org_id)
		VALUES (?, ?, ?, 1, 1, 0, ?, 0, ?, ?)`
	nine := uint64(9)
	zero := uint64(0)
	seven := uint64(7)
	for _, row := range []struct {
		team       string
		churn      *uint64
		computedAt time.Time
	}{
		{"team-nodata", nil, computedAt}, {"team-zero", &zero, computedAt}, {"team-nine", &nine, computedAt},
		// A recompute that finds no sized pull request stores NULL over an older measured row. argMax skips a NULL
		// argument, so a reader on plain argMax serves the older 7 here.
		{"team-stale", &seven, computedAt.Add(-time.Hour)}, {"team-stale", nil, computedAt},
	} {
		if err := conn.Exec(ctx, insert, repo, day, row.team, row.churn, row.computedAt, org); err != nil {
			t.Fatalf("insert %+v: %v", row, err)
		}
	}

	start, end := mustGraphQLDate("2026-08-01"), mustGraphQLDate("2026-08-08")
	breakdownQuery, err := compileBreakdownOver(BreakdownRequest{
		Dimension: DimensionTeam, Measure: MeasureChurnLOC, StartDate: start, EndDate: end, TopN: 10,
	}, "team_id", org, queryTimeoutSecs, false, nil)
	if err != nil {
		t.Fatalf("compile breakdown: %v", err)
	}
	breakdown, err := ExecuteBreakdown(ctx, client, org, breakdownQuery, "team", "churn_loc")
	if err != nil {
		t.Fatalf("ExecuteBreakdown: %v", err)
	}
	got := map[string]*float64{}
	for _, item := range breakdown.Items {
		got[item.Key] = item.Value
	}
	if v, ok := got["team-nodata"]; !ok || v != nil {
		t.Errorf("breakdown team-nodata = %v (present %v), want present with a null value: NULL churn is no data, not 0", v, ok)
	}
	if v, ok := got["team-stale"]; !ok || v != nil {
		t.Errorf("breakdown team-stale = %v (present %v), want a null value: the newest row is NULL, the older 7 is superseded", v, ok)
	}
	if v := got["team-zero"]; v == nil || *v != 0 {
		t.Errorf("breakdown team-zero = %v, want 0: a stored 0 is data", v)
	}
	if v := got["team-nine"]; v == nil || *v != 9 {
		t.Errorf("breakdown team-nine = %v, want 9", v)
	}

	seriesQuery, err := CompileTimeseries(TimeseriesRequest{
		Dimension: DimensionTeam, Measure: MeasureChurnLOC, Interval: BucketIntervalDay, StartDate: start, EndDate: end,
	}, org, queryTimeoutSecs, false, nil)
	if err != nil {
		t.Fatalf("compile timeseries: %v", err)
	}
	series, err := ExecuteTimeseries(ctx, client, seriesQuery, "team", "churn_loc")
	if err != nil {
		t.Fatalf("ExecuteTimeseries: %v", err)
	}
	byTeam := map[string][]*float64{}
	for _, result := range series {
		for _, bucket := range result.Buckets {
			byTeam[result.DimensionValue] = append(byTeam[result.DimensionValue], bucket.Value)
		}
	}
	if values := byTeam["team-nodata"]; len(values) != 1 || values[0] != nil {
		t.Errorf("timeseries team-nodata = %v, want one null bucket", values)
	}
	if values := byTeam["team-zero"]; len(values) != 1 || values[0] == nil || *values[0] != 0 {
		t.Errorf("timeseries team-zero = %v, want one bucket of 0", values)
	}
}
