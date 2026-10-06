//go:build integration

package operatingreview

import (
	"reflect"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/graph/model"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/graphqldate"
)

// CHAOS-8516 on a migrated ClickHouse: the review of several teams is the review
// of the union of their stored rows. Org "org-8516" holds teams A, B and C and a
// row with no team; org "org-8516-ab" holds teams A and B only.
func TestRealClickHouse_ReviewOfSeveralTeamsIsTheUnionOfTheirRows(t *testing.T) {
	ctx, admin, client := startOperatingReviewSchema(t)
	const full, onlyAB = "org-8516", "org-8516-ab"
	repoID := "85168516-8516-4516-8516-851685168516"
	week := time.Date(2026, 8, 24, 0, 0, 0, 0, time.UTC)
	computed := time.Date(2026, 8, 25, 1, 0, 0, 0, time.UTC)

	workItems := func(org, team string, completed, wip int, cycle float64) {
		if err := admin.Exec(ctx, `INSERT INTO work_item_metrics_daily
			(day, provider, work_scope_id, team_id, items_started, items_completed, wip_count_end_of_day,
			 cycle_time_p50_hours, cycle_time_p90_hours, wip_age_p50_hours, wip_age_p90_hours, org_id, computed_at)
			VALUES (?, 'github', ?, ?, 0, ?, ?, ?, ?, ?, ?, ?, ?)`,
			week, "scope-"+team, team, uint32(completed), uint32(wip), cycle, cycle, cycle, cycle, org, computed); err != nil {
			t.Fatalf("insert work items %s/%s: %v", org, team, err)
		}
	}
	investment := func(org, team string, units int) {
		if err := admin.Exec(ctx, `INSERT INTO investment_metrics_daily
			(repo_id, day, team_id, investment_area, project_stream, delivery_units, work_items_completed, prs_merged, churn_loc, cycle_p50_hours, org_id, computed_at)
			VALUES (?, ?, ?, 'ktlo', 'stream', ?, 0, 0, 0, 0, ?, ?)`, repoID, week, team, uint32(units), org, computed); err != nil {
			t.Fatalf("insert investment %s/%s: %v", org, team, err)
		}
	}
	for _, org := range []string{full, onlyAB} {
		workItems(org, "team-a", 3, 4, 10)
		workItems(org, "team-b", 5, 9, 30)
		investment(org, "team-a", 3)
		investment(org, "team-b", 4)
		if err := admin.Exec(ctx, `INSERT INTO deploy_metrics_daily
			(repo_id, day, deployments_count, failed_deployments_count, org_id, computed_at)
			VALUES (?, ?, 5, 1, ?, ?)`, repoID, week, org, computed); err != nil {
			t.Fatalf("insert deploy row: %v", err)
		}
	}
	workItems(full, "team-c", 7, 2, 50)
	workItems(full, "", 11, 1, 70) // a row with no team: in the all-teams review only
	investment(full, "team-c", 10)

	weekDate := graphqldate.New(week)
	review := func(org string, teamID *string, teamIDs []string) *model.OperatingReview {
		t.Helper()
		got, err := ResolveInput(ctx, client, org, teamID, teamIDs, weekDate)
		if err != nil {
			t.Fatal(err)
		}
		return got
	}
	metric := func(r *model.OperatingReview, key string) model.OperatingReviewMetric {
		t.Helper()
		for _, section := range r.Sections {
			for _, m := range section.Metrics {
				if m.Key == key {
					return m
				}
			}
		}
		t.Fatalf("no metric %s", key)
		return model.OperatingReviewMetric{}
	}

	ab := review(full, nil, []string{"team-a", "team-b"})
	if ab.TeamID != nil {
		t.Errorf("teamId = %q for two teams, want null", *ab.TeamID)
	}
	for key, want := range map[string]float64{
		"throughput":           8,  // 3 + 5: a count is a sum; team C and the team-less row are out
		"wip_count":            9,  // the largest stored WIP of the two teams
		"cycle_time_p50_hours": 20, // the mean of the two stored values of the day
		"ktlo_units":           7,  // 3 + 4
		"deployments_count":    5,  // the organisation's: no team narrows it
	} {
		if m := metric(ab, key); m.Value != want || !m.HasData {
			t.Errorf("[A, B] %s = %v (hasData %v), want %v", key, m.Value, m.HasData, want)
		}
	}
	if s := metric(ab, "throughput").Scope; s != model.OperatingReviewMetricScopeTeam {
		t.Errorf("throughput scope = %s, want TEAM", s)
	}
	if s := metric(ab, "deployments_count").Scope; s != model.OperatingReviewMetricScopeOrganization {
		t.Errorf("deployments_count scope = %s, want ORGANIZATION", s)
	}

	// One id is the one-team review.
	team := "team-a"
	if one, byArgument := review(full, nil, []string{"team-a"}), review(full, &team, nil); !reflect.DeepEqual(one, byArgument) {
		t.Errorf("teamIds [A] is not the one-team review of A")
	} else if v := metric(one, "throughput").Value; v != 3 {
		t.Errorf("team A throughput = %v, want 3", v)
	}

	// Every team of an org with no team-less row is the all-teams review, but for the echoed teamId (null in both).
	if everyTeam, all := review(onlyAB, nil, []string{"team-a", "team-b"}), review(onlyAB, nil, nil); !reflect.DeepEqual(everyTeam, all) {
		t.Errorf("every team of the org is not the all-teams review")
	}

	// A row with no team is in the all-teams review only.
	everyTeam, all := review(full, nil, []string{"team-a", "team-b", "team-c"}), review(full, nil, nil)
	if got, want := metric(everyTeam, "throughput").Value, 15.0; got != want {
		t.Errorf("[A, B, C] throughput = %v, want %v", got, want)
	}
	if got, want := metric(all, "throughput").Value, 26.0; got != want {
		t.Errorf("all-teams throughput = %v, want %v (with the team-less row)", got, want)
	}

	// The organisation's value is the same in every mode.
	for name, r := range map[string]*model.OperatingReview{"one team": review(full, &team, nil), "two teams": ab, "all teams": all} {
		if v := metric(r, "deployments_count").Value; v != 5 {
			t.Errorf("%s: deployments_count = %v, want the organisation's 5", name, v)
		}
	}
}
