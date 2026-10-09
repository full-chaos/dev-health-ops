//go:build integration

package explain

import (
	"context"
	"reflect"
	"sort"
	"testing"
	"time"
)

// The explain metric reads against a day that was computed again after a team
// id changed, on the schema of the migration chain.
//
// work_item_metrics_daily then holds, for one work scope and day: the row of
// the team under its new id; under the old id the row of the first compute
// and, newer, the row of zeros that the recompute wrote over it; and the row
// of another team whose WIP ratio is 0 and that has items.
//
// A key whose newest row is a row of zeros holds no measure: the average must
// not take it as a sample of 0, and the contributor and driver reads must not
// list its team. A real row with a ratio of 0 stays a sample.
//
// The rows are stored by plain INSERT statements, so the same file runs on a
// tree without the writer rule and without the reader clause.
func TestExplainMetricReadsLeaveOutAKeyThatARecomputeSuperseded(t *testing.T) {
	ctx := context.Background()
	admin, client := newExplainTestClickHouse(ctx, t)
	const org = "explain-superseded-keys-it"
	for _, statement := range []string{
		`INSERT INTO work_item_metrics_daily
    (day, provider, work_scope_id, team_id, team_name, items_started, items_completed, wip_count_end_of_day,
     cycle_time_p50_hours, wip_congestion_ratio, computed_at, org_id)
VALUES ('2026-01-02', 'linear', 'proj', 'platform', 'Platform', 2, 3, 4, 48, 0.6, '2026-01-03 06:00:00', '` + org + `')`,
		`INSERT INTO work_item_metrics_daily
    (day, provider, work_scope_id, team_id, team_name, items_started, items_completed, wip_count_end_of_day,
     cycle_time_p50_hours, wip_congestion_ratio, computed_at, org_id)
VALUES ('2026-01-02', 'linear', 'proj', 'linear:platform', 'Platform', 2, 3, 4, 48, 0.6, '2026-01-09 06:00:00', '` + org + `')`,
		`INSERT INTO work_item_metrics_daily (day, provider, work_scope_id, team_id, computed_at, org_id)
VALUES ('2026-01-02', 'linear', 'proj', 'platform', '2026-01-09 06:00:00', '` + org + `')`,
		`INSERT INTO work_item_metrics_daily
    (day, provider, work_scope_id, team_id, team_name, items_started, wip_count_end_of_day, wip_congestion_ratio, computed_at, org_id)
VALUES ('2026-01-02', 'linear', 'proj', 'linear:apps', 'Apps', 1, 1, 0, '2026-01-09 06:00:00', '` + org + `')`,
		`INSERT INTO work_item_metrics_daily
    (day, provider, work_scope_id, team_id, team_name, items_started, wip_count_end_of_day, wip_congestion_ratio, computed_at, org_id)
VALUES ('2025-12-30', 'linear', 'proj', 'linear:platform', 'Platform', 1, 1, 0.3, '2026-01-09 06:00:00', '` + org + `'),
       ('2025-12-30', 'linear', 'proj', 'linear:apps', 'Apps', 1, 1, 0.1, '2026-01-09 06:00:00', '` + org + `')`,
	} {
		if err := admin.Exec(ctx, statement); err != nil {
			t.Fatalf("exec %q: %v", statement, err)
		}
	}
	reader, err := NewReader(client)
	if err != nil {
		t.Fatalf("NewReader: %v", err)
	}
	start, end := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC), time.Date(2026, 1, 3, 0, 0, 0, 0, time.UTC)
	compareStart, compareEnd := time.Date(2025, 12, 29, 0, 0, 0, 0, time.UTC), time.Date(2025, 12, 31, 0, 0, 0, 0, time.UTC)
	const table, column = "work_item_metrics_daily", "wip_congestion_ratio"

	value, known, err := reader.fetchMetricValue(ctx, table, column, "avg", start, end, "", nil, org)
	if err != nil {
		t.Fatalf("wip_saturation: %v", err)
	}
	if !known || value < 0.2999 || value > 0.3001 {
		t.Errorf("wip_saturation = %v (known %v), want the average 0.3 of the two keys that hold a measure", value, known)
	}
	ids := func(rows []metricRow) []string {
		var teams []string
		for _, row := range rows {
			teams = append(teams, row.ID)
		}
		sort.Strings(teams)
		return teams
	}
	want := []string{"linear:apps", "linear:platform"}
	contributors, err := reader.fetchMetricContributors(ctx, table, column, "team_id", "avg", start, end, "", nil, org)
	if err != nil {
		t.Fatalf("contributors: %v", err)
	}
	if got := ids(contributors); !reflect.DeepEqual(got, want) {
		t.Errorf("the contributor read lists the teams %v, want %v: the old id holds no measure for the day", got, want)
	}
	drivers, err := reader.fetchMetricDriverDelta(ctx, table, column, "team_id", "avg", start, end, compareStart, compareEnd, "", nil, org)
	if err != nil {
		t.Fatalf("drivers: %v", err)
	}
	if got := ids(drivers); !reflect.DeepEqual(got, want) {
		t.Errorf("the driver read lists the teams %v, want %v: the old id holds no measure for the day", got, want)
	}
}
