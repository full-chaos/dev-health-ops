//go:build integration

package report

import (
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"
)

// userMetricsRow is one stored person-repository-day of user_metrics_daily: a value per report key.
type userMetricsRow map[string]float64

// userMetricsNullable are the Nullable columns of the table (internal/chmigrate/baseline/head.json).
var userMetricsNullable = map[string]bool{"pr_first_review_p50_hours": true, "pr_first_review_p90_hours": true,
	"pr_review_time_p50_hours": true, "pr_pickup_time_p50_hours": true}

// userMetricsWant is the value of a bucket of rows, or false for no point: a SUM key (a _count name or the count unit) is
// the sum, every other key the mean of the stored values (avgOrNull: no row is no point).
func userMetricsWant(metric string, rows []userMetricsRow) (float64, bool) {
	definition := supportedMetrics[metric]
	if strings.HasSuffix(metric, "_count") || definition.Unit == "count" {
		total := 0.0
		for _, row := range rows {
			total += row[metric]
		}
		return total, true
	}
	total, n := 0.0, 0
	for _, row := range rows {
		if value, ok := row[metric]; ok { // a missing Nullable column is NULL: no sample of the mean
			total, n = total+value, n+1
		}
	}
	if n == 0 {
		return 0, false
	}
	return total / float64(n), true
}

func (e *pooledEnv) insertUserMetricsRow(org, repo, person, storedTeam string, row userMetricsRow) {
	e.t.Helper()
	columns := make([]string, 0, len(row))
	for column := range row {
		columns = append(columns, column)
	}
	sort.Strings(columns)
	values := make([]string, 0, len(columns))
	for _, column := range columns {
		values = append(values, strconv.FormatFloat(row[column], 'f', -1, 64))
	}
	e.exec(fmt.Sprintf(`INSERT INTO user_metrics_daily (repo_id, day, author_email, identity_id, team_id, team_name, computed_at, org_id, %s)
VALUES (toUUID(?), '2026-01-02', ?, ?, ?, ?, '2026-01-02 12:00:00', ?, %s)`, strings.Join(columns, ", "), strings.Join(values, ", ")),
		repo, person, person, storedTeam, storedTeam, org)
}

func (e *pooledEnv) userMetricsChart(org, metric, groupBy, chartType, start, end string, teams ...string) ChartResult {
	e.t.Helper()
	adapter := &ClickHouseQueryAdapter{conn: e.conn, now: func() time.Time { return time.Date(2026, 3, 1, 0, 0, 0, 0, time.UTC) }}
	result, err := adapter.executeChart(e.ctx, ChartSpec{ChartType: chartType, Metric: metric, GroupBy: groupBy, OrganizationID: org,
		TimeRangeStart: start, TimeRangeEnd: end, FilterTeams: teams})
	if err != nil {
		e.t.Fatalf("%s chart: %v", metric, err)
	}
	if result.SourceTable != "user_metrics_daily" {
		e.t.Fatalf("%s chart names %s, want user_metrics_daily", metric, result.SourceTable)
	}
	return result
}

// mustEqualUserMetricsBuckets holds a chart to exactly the buckets of want; a SUM of no row is 0 on a scorecard.
func mustEqualUserMetricsBuckets(t *testing.T, what, metric string, got map[string]float64, want map[string][]userMetricsRow, chartType string) {
	t.Helper()
	points := map[string]float64{}
	for key, rows := range want {
		if len(rows) == 0 && chartType != "scorecard" {
			continue
		}
		if value, ok := userMetricsWant(metric, rows); ok {
			points[key] = value
		}
	}
	if len(got) != len(points) {
		t.Errorf("%s %s: got %v, want %v", metric, what, got, points)
		return
	}
	for key, value := range points {
		if math.IsNaN(got[key]) {
			t.Errorf("%s %s: %q is NaN, want %v", metric, what, key, value)
			continue
		}
		mustEqual(t, metric+" "+what, got, key, value)
	}
}

// userMetricsWorkItemKeys live on the person's row of no repository only (icfinalize merge.go: work items are never on a
// git row).
var userMetricsWorkItemKeys = map[string]bool{"work_items_completed": true, "work_items_active": true}

type userMetricsSeed struct {
	org                            string
	a1, a2, b, bc, n, d, synthetic userMetricsRow
	repoA, repoB, repoBC, repoN    string
	repoD                          string
}

// seedUserMetricsOwnership writes the rows as ic_finalize does: team_id is the person's membership label. At the run
// (2026-03-01) team-A owns A; team-B owns B; team-B and team-C co-own BC; nobody owns N; team-old owned D until
// 2026-02-01, team-new owns it since. p1 is a member of team-A only and works in A, B, BC, N and D; p2 has no team and
// works in A. p1's work items are on p1's row of no repository (the synthesized repository id, in no catalog).
func seedUserMetricsOwnership(e *pooledEnv) userMetricsSeed {
	s := userMetricsSeed{org: e.newOrg(),
		repoA: "14109000-0000-4000-8000-0000000000a1", repoB: "14109000-0000-4000-8000-0000000000b1",
		repoBC: "14109000-0000-4000-8000-0000000000bc", repoN: "14109000-0000-4000-8000-0000000000e1",
		repoD: "14109000-0000-4000-8000-0000000000d1",
	}
	keys := userMetricsKeys()
	git := func(seed int) userMetricsRow {
		row := userMetricsRow{}
		for index, key := range keys {
			row[key] = float64(seed*3 + index%4 + 1)
		}
		row["work_items_completed"], row["work_items_active"] = 0, 0
		row["delivery_units"] = row["prs_merged"]
		row["weekend_days"] = float64(seed % 2)
		return row
	}
	s.a1, s.a2, s.b, s.bc, s.n, s.d = git(1), git(2), git(3), git(4), git(5), git(6)
	s.synthetic = userMetricsRow{}
	for _, key := range keys {
		if !userMetricsNullable[key] {
			s.synthetic[key] = 0
		}
	}
	s.synthetic["work_items_completed"], s.synthetic["work_items_active"], s.synthetic["delivery_units"] = 7, 3, 7
	e.own(s.org, "team-A", s.repoA)
	e.own(s.org, "team-B", s.repoB)
	e.own(s.org, "team-B", s.repoBC)
	e.ownAlso(s.org, "team-C", s.repoBC, "")
	e.exec(`INSERT INTO repos (id, repo, provider, org_id, created_at, last_synced) VALUES (?, 'acme/0000e1', 'github', ?, now(), now())`, s.repoN, s.org)
	e.exec(`INSERT INTO repos (id, repo, provider, org_id, created_at, last_synced) VALUES (?, 'acme/0000d1', 'github', ?, now(), now())`, s.repoD, s.org)
	e.exec(`INSERT INTO team_repo_ownership (org_id, provider, team_id, repo_id, repo_full_name, match_type, source, is_primary, specificity, priority, valid_from, valid_to, updated_at) VALUES (?, 'github', 'team-old', ?, 'acme/0000d1', 'exact', 'inferred', 1, 0, 0, '2025-01-01 00:00:00', '2026-02-01 00:00:00', '2025-01-01 00:00:00')`, s.org, s.repoD)
	e.exec(`INSERT INTO team_repo_ownership (org_id, provider, team_id, repo_id, repo_full_name, match_type, source, is_primary, specificity, priority, valid_from, valid_to, updated_at) VALUES (?, 'github', 'team-new', ?, 'acme/0000d1', 'exact', 'inferred', 1, 0, 0, '2026-02-01 00:00:00', NULL, '2026-02-01 00:00:00')`, s.org, s.repoD)
	e.insertUserMetricsRow(s.org, s.repoA, "p1", "team-A", s.a1)
	e.insertUserMetricsRow(s.org, s.repoA, "p2", "unassigned", s.a2)
	e.insertUserMetricsRow(s.org, s.repoB, "p1", "team-A", s.b)
	e.insertUserMetricsRow(s.org, s.repoBC, "p1", "team-A", s.bc)
	e.insertUserMetricsRow(s.org, s.repoN, "p1", "team-A", s.n)
	e.insertUserMetricsRow(s.org, s.repoD, "p1", "team-A", s.d)
	e.insertUserMetricsRow(s.org, "14109000-0000-4000-8000-0000000000f1", "p1", "team-A", s.synthetic)
	return s
}

// A team of a per-person chart is the owner of the row's repository as of the report run (2026-03-01), never the stored
// membership label: a team filter keeps the filtered teams' repositories (a shared repository once), a team group is one
// bucket per owner team with a co-owned repository whole in each, a repository with no owner and the row of no repository
// are in no team, "unassigned" is no bucket, and a team that owned a repository only before the run has none of it. The
// organization chart is unchanged.
func TestRegistryUserMetricsTeamIsRepositoryOwnership(t *testing.T) {
	e := startPooledEnv(t)
	s := seedUserMetricsOwnership(e)
	of := func(rows ...userMetricsRow) []userMetricsRow { return rows }
	const start, end = "2026-01-01", "2026-01-06"
	all := of(s.a1, s.a2, s.b, s.bc, s.n, s.d, s.synthetic)
	for _, metric := range userMetricsKeys() {
		chart := func(groupBy, chartType string, teams ...string) map[string]float64 {
			return pointsOf(e.userMetricsChart(s.org, metric, groupBy, chartType, start, end, teams...))
		}
		for _, chartType := range []string{"scorecard", "bar"} {
			mustEqualUserMetricsBuckets(t, chartType+" organization", metric, chart("", chartType), map[string][]userMetricsRow{"total": all}, chartType)
		}
		mustEqualUserMetricsBuckets(t, "repo group", metric, chart("repo", "bar"), map[string][]userMetricsRow{
			s.repoA: of(s.a1, s.a2), s.repoB: of(s.b), s.repoBC: of(s.bc), s.repoN: of(s.n), s.repoD: of(s.d),
			"14109000-0000-4000-8000-0000000000f1": of(s.synthetic)}, "bar")
		if userMetricsWorkItemKeys[metric] {
			continue // TestRegistryUserMetricsWorkItemsHaveNoTeamValue
		}
		for _, chartType := range []string{"scorecard", "bar"} {
			mustEqualUserMetricsBuckets(t, chartType+" member team-A reads its owned A only", metric, chart("", chartType, "team-A"), map[string][]userMetricsRow{"total": of(s.a1, s.a2)}, chartType)
			mustEqualUserMetricsBuckets(t, chartType+" owner team-B", metric, chart("", chartType, "team-B"), map[string][]userMetricsRow{"total": of(s.b, s.bc)}, chartType)
			mustEqualUserMetricsBuckets(t, chartType+" co-owner team-C", metric, chart("", chartType, "team-C"), map[string][]userMetricsRow{"total": of(s.bc)}, chartType)
			mustEqualUserMetricsBuckets(t, chartType+" team-B and team-C, BC once", metric, chart("", chartType, "team-B", "team-C"), map[string][]userMetricsRow{"total": of(s.b, s.bc)}, chartType)
			mustEqualUserMetricsBuckets(t, chartType+" owner at the run team-new", metric, chart("", chartType, "team-new"), map[string][]userMetricsRow{"total": of(s.d)}, chartType)
			mustEqualUserMetricsBuckets(t, chartType+" team-old owns no repository at the run", metric, chart("", chartType, "team-old"), map[string][]userMetricsRow{"total": {}}, chartType)
		}
		mustEqualUserMetricsBuckets(t, "team group", metric, chart("team", "bar"),
			map[string][]userMetricsRow{"team-A": of(s.a1, s.a2), "team-B": of(s.b, s.bc), "team-C": of(s.bc), "team-new": of(s.d)}, "bar")
		mustEqualUserMetricsBuckets(t, "team group with filter team-C", metric, chart("team", "bar", "team-C"),
			map[string][]userMetricsRow{"team-C": of(s.bc)}, "bar")
	}
	rendered := renderChart(e.userMetricsChart(s.org, "commits_count", "team", "bar", start, end))
	if !strings.Contains(rendered, "| team-A |") || !strings.Contains(rendered, "| team-B |") || !strings.Contains(rendered, "| team-C |") ||
		!strings.Contains(rendered, "| team-new |") || strings.Contains(rendered, "unassigned") || strings.Contains(rendered, "team-old") {
		t.Errorf("rendered team chart:\n%s", rendered)
	}
}

// Work items belong to no repository: under a team filter or a team group, work_items_completed and work_items_active have
// no team value by repository ownership. The chart is "not applicable" with its reason, never 0 and never the stored
// membership team. delivery_units keeps its merged pull requests by repository ownership and names the left-out work-item
// part. The organization chart is unchanged.
func TestRegistryUserMetricsWorkItemsHaveNoTeamValue(t *testing.T) {
	e := startPooledEnv(t)
	s := seedUserMetricsOwnership(e)
	const start, end = "2026-01-01", "2026-01-06"
	for metric := range userMetricsWorkItemKeys {
		for _, spec := range []struct {
			name, groupBy, chartType string
			teams                    []string
		}{
			{"filter team-A", "", "scorecard", []string{"team-A"}},
			{"filter team-B", "", "bar", []string{"team-B"}},
			{"team group", "team", "bar", nil},
			{"team group with filter", "team", "bar", []string{"team-A"}},
			{"repo group with filter", "repo", "bar", []string{"team-A"}},
		} {
			result := e.userMetricsChart(s.org, metric, spec.groupBy, spec.chartType, start, end, spec.teams...)
			if len(result.DataPoints) != 0 {
				t.Errorf("%s %s: got points %v, want none (not applicable)", metric, spec.name, pointsOf(result))
			}
			if rendered := renderChart(result); !strings.Contains(rendered, "_Not applicable: ") || !strings.Contains(rendered, "work items belong to no repository") {
				t.Errorf("%s %s: rendered without the not-applicable reason:\n%s", metric, spec.name, rendered)
			}
		}
		organization := e.userMetricsChart(s.org, metric, "", "scorecard", start, end)
		mustEqualUserMetricsBuckets(t, "organization", metric, pointsOf(organization),
			map[string][]userMetricsRow{"total": {s.a1, s.a2, s.b, s.bc, s.n, s.d, s.synthetic}}, "scorecard")
		if rendered := renderChart(organization); strings.Contains(rendered, "Not applicable") || strings.Contains(rendered, "_Note: ") {
			t.Errorf("%s organization: rendered a team note:\n%s", metric, rendered)
		}
	}
	delivery := e.userMetricsChart(s.org, "delivery_units", "", "scorecard", start, end, "team-A")
	mustEqualUserMetricsBuckets(t, "filter team-A", "delivery_units", pointsOf(delivery), map[string][]userMetricsRow{"total": {s.a1, s.a2}}, "scorecard")
	if rendered := renderChart(delivery); !strings.Contains(rendered, "_Note: ") || !strings.Contains(rendered, "merged pull requests") {
		t.Errorf("delivery_units team-A: rendered without the work-item note:\n%s", rendered)
	}
	if rendered := renderChart(e.userMetricsChart(s.org, "delivery_units", "", "scorecard", start, end)); strings.Contains(rendered, "_Note: ") {
		t.Errorf("delivery_units organization: rendered a team note:\n%s", rendered)
	}
}

// A team that owns no repository: the 4 SUM keys give 0 on a scorecard, the mean keys no point (never NaN).
func TestRegistryUserMetricsTeamWithNoRepository(t *testing.T) {
	e := startPooledEnv(t)
	s := seedUserMetricsOwnership(e)
	for _, metric := range userMetricsKeys() {
		if userMetricsWorkItemKeys[metric] {
			continue
		}
		mustEqualUserMetricsBuckets(t, "team with no repository", metric,
			pointsOf(e.userMetricsChart(s.org, metric, "", "scorecard", "2026-01-01", "2026-01-06", "team-none")),
			map[string][]userMetricsRow{"total": {}}, "scorecard")
	}
}

// An organization chart over a window with no row: the SUM keys 0, the mean keys no point, never NaN.
func TestRegistryUserMetricsEmptyWindowIsNoPoint(t *testing.T) {
	e := startPooledEnv(t)
	s := seedUserMetricsOwnership(e)
	for _, metric := range userMetricsKeys() {
		for _, chartType := range []string{"scorecard", "trend_delta", "table"} {
			mustEqualUserMetricsBuckets(t, chartType+" organization, window with no row", metric,
				pointsOf(e.userMetricsChart(s.org, metric, "", chartType, "2026-02-01", "2026-02-06")), map[string][]userMetricsRow{"total": {}}, "scorecard")
		}
	}
}
