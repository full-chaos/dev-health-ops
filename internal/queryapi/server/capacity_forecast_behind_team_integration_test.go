//go:build integration

// CHAOS-8498: a selected team whose newest metrics row is older than another
// selected team's must still add its own newest backlog. The states seeded here
// are ones the real work_item_metrics_daily writer produces: a daily run writes
// one batch per repository partition, so a day can be present for some teams
// and not yet for others, and a team with no event and no open work gets no row.
package server

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/capacityforecast"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/graph/model"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/throughputforecast"
)

type behindTeamRow struct {
	team      string
	scope     string
	dayOffset int
	wip       int
}

func behindTeamSeed(anchor time.Time, rows []behindTeamRow) string {
	values := make([]string, 0, len(rows))
	for _, row := range rows {
		values = append(values, fmt.Sprintf(
			"(toDate('%s'), 'jira', '%s', '%s', 5, %d, toDateTime('2026-09-01 00:00:00'), 'org-mine')",
			anchor.AddDate(0, 0, row.dayOffset).Format("2006-01-02"), scopeOf(row), row.team, row.wip,
		))
	}
	return `INSERT INTO work_item_metrics_daily
        (day, provider, work_scope_id, team_id, items_completed, wip_count_end_of_day, computed_at, org_id)
        VALUES ` + strings.Join(values, ", ")
}

func scopeOf(row behindTeamRow) string {
	if row.scope == "" {
		return "scope-x"
	}
	return row.scope
}

func scopedDailyRows(scope, team string, firstOffset, lastOffset, wip int) []behindTeamRow {
	rows := dailyRows(team, firstOffset, lastOffset, wip)
	for index := range rows {
		rows[index].scope = scope
	}
	return rows
}

func dailyRows(team string, firstOffset, lastOffset, wip int) []behindTeamRow {
	var rows []behindTeamRow
	for offset := firstOffset; offset <= lastOffset; offset++ {
		rows = append(rows, behindTeamRow{team: team, dayOffset: offset, wip: wip})
	}
	return rows
}

func TestCapacityForecastBacklogUsesEachSelectedTeamsNewestRow(t *testing.T) {
	anchor := time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC)
	cases := []struct {
		name    string
		rows    []behindTeamRow
		teamIDs []string
		scope   string
		want    int
	}{
		{
			name: "mid-run: one team has the newest day, the other not yet",
			rows: append(dailyRows("team-current", -29, 0, 40),
				dailyRows("team-behind", -29, -1, 30)...),
			teamIDs: []string{"team-current", "team-behind"},
			want:    70,
		},
		{
			name: "unscoped read sums every team's newest row",
			rows: append(dailyRows("team-current", -29, 0, 40),
				dailyRows("team-behind", -29, -1, 30)...),
			teamIDs: nil,
			want:    70,
		},
		{
			name: "team with no open work for days adds its last WIP of zero",
			rows: append(dailyRows("team-current", -29, 0, 40),
				dailyRows("team-idle", -29, -5, 0)...),
			teamIDs: []string{"team-current", "team-idle"},
			want:    40,
		},
		{
			name: "project scope: two teams on one work scope, different newest days",
			rows: append(scopedDailyRows("scope-x", "team-current", -29, 0, 40),
				scopedDailyRows("scope-x", "team-behind", -29, -1, 30)...),
			teamIDs: []string{"team-current", "team-behind"},
			scope:   "scope-x",
			want:    70,
		},
		{
			name: "one team on two work scopes with different newest days",
			rows: append(scopedDailyRows("scope-x", "team-current", -29, 0, 40),
				scopedDailyRows("scope-y", "team-current", -29, -1, 30)...),
			teamIDs: []string{"team-current"},
			want:    70,
		},
		{
			name:    "one team gives the same answer as before",
			rows:    dailyRows("team-current", -29, 0, 40),
			teamIDs: []string{"team-current"},
			want:    40,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			_, client, raw := migratedClickHouse(ctx, t)
			if err := raw.Exec(ctx, behindTeamSeed(anchor, tc.rows)); err != nil {
				t.Fatalf("seed work_item_metrics_daily: %v", err)
			}
			targetItems := 1
			got, err := capacityforecast.ResolveForecast(ctx, client, "org-mine", &model.CapacityForecastInput{
				TeamIds: tc.teamIDs, WorkScopeID: scopePtr(tc.scope), TargetItems: &targetItems, HistoryDays: 30, Simulations: 1,
			}, anchor)
			if err != nil {
				t.Fatalf("ResolveForecast: %v", err)
			}
			if got == nil {
				t.Fatal("got nil, want a forecast")
			}
			if got.BacklogSize != tc.want {
				t.Fatalf("backlogSize: got %d, want %d", got.BacklogSize, tc.want)
			}
		})
	}
}

func TestThroughputForecastBacklogUsesEachSelectedTeamsNewestRow(t *testing.T) {
	ctx := context.Background()
	_, client, raw := migratedClickHouse(ctx, t)
	anchor := time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC)
	rows := append(dailyRows("team-current", -29, 0, 40), dailyRows("team-behind", -29, -1, 30)...)
	if err := raw.Exec(ctx, behindTeamSeed(anchor, rows)); err != nil {
		t.Fatalf("seed work_item_metrics_daily: %v", err)
	}
	got, err := throughputforecast.Resolve(ctx, client, "org-mine", model.ThroughputForecastInput{
		TeamIds: []string{"team-current", "team-behind"}, HistoryWeeks: 4,
	}, anchor)
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	if got == nil {
		t.Fatal("got nil, want a forecast")
	}
	if got.BacklogSize != 70 {
		t.Fatalf("backlogSize: got %d, want 70 (40 + the behind team's newest 30)", got.BacklogSize)
	}
}

func scopePtr(scope string) *string {
	if scope == "" {
		return nil
	}
	return &scope
}
