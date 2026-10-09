//go:build integration

package home

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/jobs/metrics/changefailure"
	"github.com/full-chaos/dev-health-ops/internal/jobs/metrics/daily/repouser"
)

// Change failure rate follows the view (CHAOS-8981): its window and its
// subject. The rows are written by the real writer of
// repo_change_failure_daily and read through the Home metric readers.
//
//	repo A   day 1: 1 deployment, 1 failed, 1 incident   (an older version of
//	                the same day says 1 deployment, 0 failed, 0 incidents)
//	         day 2: 9 deployments, 0 failed, 1 incident
//	repo B   day 1: 4 deployments, no incident evidence
//	repo C   day 1: 1 incident, no deployment
//	repo D   day 1: 5 deployments, 5 failed, 1 incident  -- owned by no team
//	other organization, repo A's id: 50 deployments, 50 failed
func TestHomeChangeFailureRate_FollowsTheViewsWindowAndSubject(t *testing.T) {
	ctx := context.Background()
	admin, client := newHomeTestClickHouse(ctx, t)

	const orgID = "home-org-change-failure"
	const teamAB, teamBC = "team-ab", "team-bc"
	repoA, repoB, repoC, repoD := uuid.New(), uuid.New(), uuid.New(), uuid.New()
	day1 := time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC)
	day2 := day1.AddDate(0, 0, 1)
	older := time.Date(2026, 9, 3, 6, 0, 0, 0, time.UTC)
	newer := older.Add(time.Hour)

	writer, err := repouser.NewWriter(admin)
	if err != nil {
		t.Fatal(err)
	}
	write := func(org string, repo uuid.UUID, day, computedAt time.Time, counts changefailure.Counts) {
		t.Helper()
		if _, err := writer.WriteChangeFailure(ctx, []repouser.ChangeFailureDaily{
			{RepoID: repo, Day: day, Counts: counts, ComputedAt: computedAt},
		}, org); err != nil {
			t.Fatal(err)
		}
	}
	write(orgID, repoA, day1, older, changefailure.Counts{Deployments: 1})
	write(orgID, repoA, day1, newer, changefailure.Counts{Deployments: 1, FailedHeuristic: 1, IncidentsDirect: 1})
	write(orgID, repoA, day2, newer, changefailure.Counts{Deployments: 9, IncidentsDirect: 1})
	write(orgID, repoB, day1, newer, changefailure.Counts{Deployments: 4})
	write(orgID, repoC, day1, newer, changefailure.Counts{IncidentsViaDeployment: 1})
	write(orgID, repoD, day1, newer, changefailure.Counts{Deployments: 5, FailedNative: 5, IncidentsDirect: 1})
	write("home-org-change-failure-other", repoA, day1, newer, changefailure.Counts{Deployments: 50, FailedHeuristic: 50, IncidentsDirect: 1})

	names := map[uuid.UUID]string{repoA: "acme/cfr-a", repoB: "acme/cfr-b", repoC: "acme/cfr-c", repoD: "acme/cfr-d"}
	for repo, name := range names {
		if err := admin.Exec(ctx, fmt.Sprintf(
			"INSERT INTO repos (id, repo, provider, org_id, created_at, last_synced) VALUES ('%s', '%s', 'github', '%s', now64(3), now64(3))",
			repo, name, orgID)); err != nil {
			t.Fatal(err)
		}
	}
	// The repository's full name is part of the ownership table's sorting key:
	// each owned repository needs its own.
	own := func(team string, repo uuid.UUID) {
		t.Helper()
		if err := admin.Exec(ctx, fmt.Sprintf(
			`INSERT INTO team_repo_ownership (org_id, provider, team_id, repo_id, repo_full_name, match_type, source, is_primary, specificity, priority, valid_from, valid_to, updated_at)
			 VALUES ('%s', 'github', '%s', '%s', '%s', 'exact', 'inferred', 0, 0, 0, toDateTime64('2026-01-01 00:00:00', 3), NULL, toDateTime64('2026-01-01 00:00:00', 3))`,
			orgID, team, repo, names[repo])); err != nil {
			t.Fatal(err)
		}
	}
	own(teamAB, repoA)
	own(teamAB, repoB)
	own(teamBC, repoB)
	own(teamBC, repoC)

	spec := metricSpec{}
	for _, candidate := range metrics {
		if candidate.Metric == "change_failure_rate" {
			spec = candidate
		}
	}
	if spec.Table != changefailure.Table {
		t.Fatalf("change_failure_rate reads %q, want %q", spec.Table, changefailure.Table)
	}
	// The production path of the Home delta: the window's summed counts
	// through the shared rule, served with the state. The unit is percent.
	delta := func(scope ScopeFilter, start, end time.Time) MetricDelta {
		t.Helper()
		got, err := computeMetricDelta(ctx, client, spec, start, end, start.AddDate(0, 0, -7), start, Filters{Scope: scope}, orgID, teamScopeReadAsOf)
		if err != nil {
			t.Fatal(err)
		}
		return got
	}
	repoScope := func(repo uuid.UUID) ScopeFilter { return ScopeFilter{Level: "repo", IDs: []string{repo.String()}} }
	teamScope := func(team string) ScopeFilter { return ScopeFilter{Level: "team", IDs: []string{team}} }
	after := day2.AddDate(0, 0, 1)
	const (
		measured      = "measured"
		unknown       = "unknown_no_incident_evidence"
		notApplicable = "not_applicable_no_deployments"
		noState       = "null"
	)

	for _, tc := range []struct {
		name       string
		scope      ScopeFilter
		start, end time.Time
		want       float64
		hasData    bool
		state      string
	}{
		// 1 failed of 10 over both days: the ratio of sums, not the 50% average of 1/1 and 0/9.
		{"repo A, both days", repoScope(repoA), day1, after, 10, true, measured},
		// The newest version of day 1 counts, not the older one that has no incident.
		{"repo A, day 1", repoScope(repoA), day1, day2, 100, true, measured},
		{"repo A, day 2: deployments and an incident, none failed", repoScope(repoA), day2, after, 0, true, measured},
		{"repo B: deployments and no incident evidence", repoScope(repoB), day1, after, 0, false, unknown},
		{"repo C: an incident and no deployment", repoScope(repoC), day1, after, 0, false, notApplicable},
		{"a window with no row", repoScope(repoA), after, after.AddDate(0, 0, 7), 0, false, noState},
		// A team is its owned repositories: A and B give 1 failed of 14.
		{"team owning A and B", teamScope(teamAB), day1, after, 100.0 / 14.0, true, measured},
		// B's deployments and C's incident are one team view: a measured 0 of 4.
		{"team owning B and C", teamScope(teamBC), day1, after, 0, true, measured},
		// The organization is every repository, the unowned D included: 6 failed of 19.
		{"organization", ScopeFilter{Level: "org"}, day1, after, 600.0 / 19.0, true, measured},
	} {
		got := delta(tc.scope, tc.start, tc.end)
		state := noState
		if got.RateState != nil {
			state = *got.RateState
		}
		if got.HasData != tc.hasData || !closeTo(got.Value, tc.want) || state != tc.state || got.Unit != "%" {
			t.Errorf("%s: value %v %s, hasData %v, rateState %s; want %v %%, %v, %s", tc.name, got.Value, got.Unit, got.HasData, state, tc.want, tc.hasData, tc.state)
		}
	}
	// The comparison window carries its own presence flag: the week before
	// day 2 holds day 1 (measured), the week before day 1 holds nothing.
	if got := delta(repoScope(repoA), day2, after); !got.HasPriorData {
		t.Errorf("repo A, day 2: hasPriorData false, want true (day 1 is measured)")
	}
	if got := delta(repoScope(repoA), day1, day2); got.HasPriorData {
		t.Errorf("repo A, day 1: hasPriorData true, want false (no stored counts before day 1)")
	}
	// A retraction row (zeros, newer) takes the repository back to "not
	// applicable": the older counts are not read again.
	write(orgID, repoD, day1, newer.Add(time.Hour), changefailure.Counts{})
	if got := delta(repoScope(repoD), day1, after); got.HasData || got.RateState == nil || *got.RateState != notApplicable {
		t.Errorf("repo D after its retraction row: %+v, want no data and not applicable", got)
	}
	write(orgID, repoD, day1, newer.Add(2*time.Hour), changefailure.Counts{Deployments: 5, FailedNative: 5, IncidentsDirect: 1})

	// The series leaves an undefined day out instead of drawing 0.
	series := func(scope ScopeFilter) []dayValueRow {
		t.Helper()
		filter, bindings, err := scopeFilterForMetric(ctx, client, spec.Scope, Filters{Scope: scope}, orgID, "team_id", "repo_id", teamScopeReadAsOf)
		if err != nil {
			t.Fatal(err)
		}
		points, err := fetchMetricSeries(ctx, client, spec.Table, spec.Column, day1, after, filter, bindings, spec.Aggregator, orgID)
		if err != nil {
			t.Fatal(err)
		}
		return points
	}
	if points := series(repoScope(repoB)); len(points) != 0 {
		t.Errorf("series of a repository with no incident evidence = %+v, want no point", points)
	}
	if points := series(repoScope(repoA)); len(points) != 2 || points[0].Value != 1 || points[1].Value != 0 {
		t.Errorf("series of repo A = %+v, want day 1 = 1 and day 2 = 0", points)
	}

	// Drivers: only a repository with a defined rate can drive the metric. B
	// (unknown) and C (not applicable) are not drivers at 0%.
	drivers, err := fetchMetricDriverDelta(ctx, client, spec.Table, spec.Column, "repo_id", day1, after, day1.AddDate(0, 0, -7), day1, "", nil, orgID, 10)
	if err != nil {
		t.Fatalf("fetchMetricDriverDelta: %v", err)
	}
	got := map[string]bool{}
	for _, driver := range drivers {
		got[driver.ID] = true
	}
	if len(got) != 2 || !got[repoA.String()] || !got[repoD.String()] {
		t.Errorf("change failure rate drivers = %v, want exactly repo A and repo D", got)
	}
}

func closeTo(got, want float64) bool {
	diff := got - want
	return diff < 1e-9 && diff > -1e-9
}
