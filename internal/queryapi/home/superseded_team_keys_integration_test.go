//go:build integration

package home

import (
	"context"
	"reflect"
	"sort"
	"testing"
	"time"

	stdclickhouse "github.com/ClickHouse/clickhouse-go/v2"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/chquery"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/chschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

// The home metric reads against a day that was computed again after a team id
// changed, on the schema of the migration chain.
//
// work_item_metrics_daily then holds, for one work scope and day:
//
//   - the row of the team under its new id (a measure in every column);
//   - under the old id, the row of the first compute and, newer, the row of
//     zeros that the recompute wrote over it;
//   - the row of another team whose WIP ratio is 0 and that has items.
//
// A key whose newest row is a row of zeros holds no measure. The averages
// must not take it as a sample of 0, the row count must not count it, and the
// driver read must not list its team. A real row with a ratio of 0 stays a
// sample.
//
// The rows are stored by plain INSERT statements, so the same file runs on a
// tree without the writer rule and without the reader clause.
func TestHomeMetricReadsLeaveOutAKeyThatARecomputeSuperseded(t *testing.T) {
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

	const org = "home-superseded-keys-it"
	for _, statement := range []string{
		// The first compute, under the old team id.
		`INSERT INTO work_item_metrics_daily
    (day, provider, work_scope_id, team_id, team_name, items_started, items_completed, wip_count_end_of_day,
     cycle_time_p50_hours, wip_congestion_ratio, computed_at, org_id)
VALUES ('2026-01-02', 'linear', 'proj', 'platform', 'Platform', 2, 3, 4, 48, 0.6, '2026-01-03 06:00:00', '` + org + `')`,
		// The recompute: the same work under the new id, and a row of zeros
		// over the old key.
		`INSERT INTO work_item_metrics_daily
    (day, provider, work_scope_id, team_id, team_name, items_started, items_completed, wip_count_end_of_day,
     cycle_time_p50_hours, wip_congestion_ratio, computed_at, org_id)
VALUES ('2026-01-02', 'linear', 'proj', 'linear:platform', 'Platform', 2, 3, 4, 48, 0.6, '2026-01-09 06:00:00', '` + org + `')`,
		`INSERT INTO work_item_metrics_daily (day, provider, work_scope_id, team_id, computed_at, org_id)
VALUES ('2026-01-02', 'linear', 'proj', 'platform', '2026-01-09 06:00:00', '` + org + `')`,
		// Another team: items in progress, a WIP ratio of 0, no cycle time.
		`INSERT INTO work_item_metrics_daily
    (day, provider, work_scope_id, team_id, team_name, items_started, wip_count_end_of_day, wip_congestion_ratio, computed_at, org_id)
VALUES ('2026-01-02', 'linear', 'proj', 'linear:apps', 'Apps', 1, 1, 0, '2026-01-09 06:00:00', '` + org + `')`,
		// The comparison window of the driver read: the two teams of today.
		`INSERT INTO work_item_metrics_daily
    (day, provider, work_scope_id, team_id, team_name, items_started, wip_count_end_of_day, wip_congestion_ratio, computed_at, org_id)
VALUES ('2025-12-30', 'linear', 'proj', 'linear:platform', 'Platform', 1, 1, 0.3, '2026-01-09 06:00:00', '` + org + `'),
       ('2025-12-30', 'linear', 'proj', 'linear:apps', 'Apps', 1, 1, 0.1, '2026-01-09 06:00:00', '` + org + `')`,
	} {
		if err := conn.Exec(ctx, statement); err != nil {
			t.Fatalf("exec %q: %v", statement, err)
		}
	}
	start, end := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC), time.Date(2026, 1, 3, 0, 0, 0, 0, time.UTC)
	const table = "work_item_metrics_daily"

	saturation, err := fetchMetricValue(ctx, client, table, "wip_congestion_ratio", start, end, "", nil, "avg", org)
	if err != nil {
		t.Fatalf("wip_saturation: %v", err)
	}
	// Two keys hold a measure: 0.6 and 0. The superseded key is no sample.
	if !saturation.HasData || saturation.Value < 0.2999 || saturation.Value > 0.3001 {
		t.Errorf("wip_saturation = %+v, want the average 0.3 of the two keys that hold a measure", saturation)
	}
	cycle, err := fetchMetricValue(ctx, client, table, "cycle_time_p50_hours", start, end, "", nil, "avg", org)
	if err != nil {
		t.Fatalf("cycle_time: %v", err)
	}
	if cycle.Value != 48 {
		t.Errorf("cycle_time = %+v, want 48: the one key with a cycle time", cycle)
	}
	throughput, err := fetchMetricValue(ctx, client, table, "items_completed", start, end, "", nil, "sum", org)
	if err != nil {
		t.Fatalf("throughput: %v", err)
	}
	if throughput.Value != 3 {
		t.Errorf("throughput = %+v, want 3: the day is counted once", throughput)
	}
	series, err := fetchMetricSeries(ctx, client, table, "wip_congestion_ratio", start, end, "", nil, "avg", org)
	if err != nil {
		t.Fatalf("wip_saturation series: %v", err)
	}
	if len(series) != 1 || series[0].Value < 0.2999 || series[0].Value > 0.3001 {
		t.Errorf("wip_saturation series = %+v, want one day with 0.3", series)
	}
	drivers, err := fetchMetricDriverDelta(ctx, client, table, "wip_congestion_ratio", "team_id", start, end,
		time.Date(2025, 12, 29, 0, 0, 0, 0, time.UTC), time.Date(2025, 12, 31, 0, 0, 0, 0, time.UTC), "", nil, org, 10)
	if err != nil {
		t.Fatalf("wip_saturation drivers: %v", err)
	}
	var teams []string
	for _, driver := range drivers {
		teams = append(teams, driver.ID)
	}
	sort.Strings(teams)
	if want := []string{"linear:apps", "linear:platform"}; !reflect.DeepEqual(teams, want) {
		t.Errorf("the driver read lists the teams %v, want %v: the old id holds no measure for the day", teams, want)
	}

	// Only rows of zeros: no data, not a value of 0.
	if err := conn.Exec(ctx, `INSERT INTO work_item_metrics_daily (day, provider, work_scope_id, team_id, computed_at, org_id)
VALUES ('2026-01-02', 'linear', 'proj', 'platform', '2026-01-09 06:00:00', '`+org+`-only-zeros')`); err != nil {
		t.Fatal(err)
	}
	empty, err := fetchMetricValue(ctx, client, table, "items_completed", start, end, "", nil, "sum", org+"-only-zeros")
	if err != nil {
		t.Fatalf("throughput of an organization with only rows of zeros: %v", err)
	}
	if empty.HasData {
		t.Errorf("an organization whose only row is a row of zeros reports data: %+v", empty)
	}
}
