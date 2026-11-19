//go:build integration

package report

import (
	"fmt"
	"math"
	"testing"
	"time"
)

// testopsTestRow is one stored repository-day of testops_test_metrics_daily.
type testopsTestRow struct {
	passed, failed, skipped, quarantined, suites float64
	flake, retry, recurrence                     float64
}

// distinct is the row's distinct cases and flakeCases its flake cases (the stored flake rate of those cases).
func (r testopsTestRow) distinct() float64   { return r.passed + r.failed + r.skipped + r.quarantined }
func (r testopsTestRow) flakeCases() float64 { return math.Round(r.flake * r.distinct()) }

func (e *pooledEnv) insertTestopsTestRow(org, repo, storedTeam string, row testopsTestRow) {
	e.t.Helper()
	var team any
	if storedTeam != "" {
		team = storedTeam
	}
	e.exec(`INSERT INTO testops_test_metrics_daily (repo_id, day, total_cases, passed_count, failed_count, skipped_count, quarantined_count,
    pass_rate, failure_rate, flake_rate, retry_dependency_rate, total_suites, failure_recurrence_score, team_id, service_id, org_id, computed_at,
    flake_cases, distinct_cases)
VALUES (?, '2026-01-02', ?, ?, ?, ?, ?, 0.5, 0.5, ?, ?, ?, ?, ?, NULL, ?, '2026-01-02 23:00:00', ?, ?)`,
		repo, uint32(row.passed+row.failed+row.skipped), uint32(row.passed), uint32(row.failed), uint32(row.skipped), uint32(row.quarantined),
		row.flake, row.retry, uint32(row.suites), row.recurrence, team, org, uint32(row.flakeCases()), uint32(row.distinct()))
}

// testopsTestWant is the value of a bucket of rows, or false for no point: counts are sums, pass_rate is the summed passed
// over the summed passed and failed cases, flake_rate is the summed flake cases over the summed distinct cases,
// retry_dependency_rate the summed retry-dependent cases (rate times distinct cases) over the same, and
// failure_recurrence_score is still the mean of the stored repository-day values.
func testopsTestWant(metric string, rows []testopsTestRow) (float64, bool) {
	var sum testopsTestRow
	var flakeCases, retryCases, distinct float64
	for _, row := range rows {
		flakeCases, retryCases, distinct = flakeCases+row.flakeCases(), retryCases+row.retry*row.distinct(), distinct+row.distinct()
		sum.passed, sum.failed, sum.skipped, sum.quarantined, sum.suites = sum.passed+row.passed, sum.failed+row.failed, sum.skipped+row.skipped, sum.quarantined+row.quarantined, sum.suites+row.suites
		sum.flake, sum.retry, sum.recurrence = sum.flake+row.flake, sum.retry+row.retry, sum.recurrence+row.recurrence
	}
	n := float64(len(rows))
	switch metric {
	case "passed_count":
		return sum.passed, true
	case "failed_count":
		return sum.failed, true
	case "skipped_count":
		return sum.skipped, true
	case "quarantined_count":
		return sum.quarantined, true
	case "total_cases":
		return sum.passed + sum.failed + sum.skipped, true
	case "total_suites":
		return sum.suites, true
	case "pass_rate":
		if sum.passed+sum.failed == 0 {
			return 0, false
		}
		return sum.passed / (sum.passed + sum.failed), true
	}
	if len(rows) == 0 {
		return 0, false
	}
	switch metric {
	case "flake_rate":
		return flakeCases / distinct, true
	case "retry_dependency_rate":
		return retryCases / distinct, true
	default:
		return sum.recurrence / n, true
	}
}

func (e *pooledEnv) testopsTestChart(org, metric, groupBy, chartType, start, end string, teams ...string) map[string]float64 {
	e.t.Helper()
	adapter := &ClickHouseQueryAdapter{conn: e.conn, now: func() time.Time { return time.Date(2026, 3, 1, 0, 0, 0, 0, time.UTC) }}
	result, err := adapter.executeChart(e.ctx, ChartSpec{ChartType: chartType, Metric: metric, GroupBy: groupBy, OrganizationID: org,
		TimeRangeStart: start, TimeRangeEnd: end, FilterTeams: teams})
	if err != nil {
		e.t.Fatalf("%s chart: %v", metric, err)
	}
	if result.SourceTable != "testops_test_metrics_daily" {
		e.t.Fatalf("%s chart names %s, want testops_test_metrics_daily", metric, result.SourceTable)
	}
	return pointsOf(result)
}

// mustEqualTestopsBuckets holds a chart to exactly the buckets of want; a count of no row is 0 on a scorecard.
func mustEqualTestopsBuckets(t *testing.T, what, metric string, got map[string]float64, want map[string][]testopsTestRow, chartType string) {
	t.Helper()
	points := map[string]float64{}
	for key, rows := range want {
		if len(rows) == 0 && chartType != "scorecard" {
			continue
		}
		if value, ok := testopsTestWant(metric, rows); ok {
			points[key] = value
		}
	}
	if len(got) != len(points) {
		t.Errorf("%s %s: got %v, want %v", metric, what, got, points)
		return
	}
	for key, value := range points {
		mustEqual(t, metric+" "+what, got, key, value)
	}
}

type testopsTestSeed struct {
	org                    string
	rowA, rowB, rowC, rowD testopsTestRow
	repoA, repoB, repoC    string
	repoD                  string
}

// seedTestopsTestOwnership: team-1 owns A; team-1 and team-2 co-own B (the writer stores team-1); nobody owns C; team-old
// owned D until 2026-02-01 (the writer stores team-old), team-new owns it since. The report runs 2026-03-01.
func seedTestopsTestOwnership(e *pooledEnv) testopsTestSeed {
	s := testopsTestSeed{org: e.newOrg(),
		repoA: "14106000-0000-4000-8000-0000000000a1", repoB: "14106000-0000-4000-8000-0000000000b1",
		repoC: "14106000-0000-4000-8000-0000000000c1", repoD: "14106000-0000-4000-8000-0000000000d1",
		rowA: testopsTestRow{8, 2, 1, 0, 2, 0.1, 0.2, 0.3},
		rowB: testopsTestRow{4, 6, 2, 1, 3, 0.3, 0.4, 0.5},
		rowC: testopsTestRow{5, 5, 0, 2, 1, 0.5, 0.6, 0.7},
		rowD: testopsTestRow{7, 3, 4, 0, 5, 0.7, 0.8, 0.9},
	}
	e.own(s.org, "team-1", s.repoA)
	e.own(s.org, "team-1", s.repoB)
	e.ownAlso(s.org, "team-2", s.repoB, "")
	e.exec(`INSERT INTO repos (id, repo, provider, org_id, created_at, last_synced) VALUES (?, 'acme/0000d1', 'github', ?, now(), now())`, s.repoD, s.org)
	e.exec(`INSERT INTO team_repo_ownership (org_id, provider, team_id, repo_id, repo_full_name, match_type, source, is_primary, specificity, priority, valid_from, valid_to, updated_at) VALUES (?, 'github', 'team-old', ?, 'acme/0000d1', 'exact', 'inferred', 1, 0, 0, '2025-01-01 00:00:00', '2026-02-01 00:00:00', '2025-01-01 00:00:00')`, s.org, s.repoD)
	e.exec(`INSERT INTO team_repo_ownership (org_id, provider, team_id, repo_id, repo_full_name, match_type, source, is_primary, specificity, priority, valid_from, valid_to, updated_at) VALUES (?, 'github', 'team-new', ?, 'acme/0000d1', 'exact', 'inferred', 1, 0, 0, '2026-02-01 00:00:00', NULL, '2026-02-01 00:00:00')`, s.org, s.repoD)
	e.insertTestopsTestRow(s.org, s.repoA, "team-1", s.rowA)
	e.insertTestopsTestRow(s.org, s.repoB, "team-1", s.rowB)
	e.insertTestopsTestRow(s.org, s.repoC, "", s.rowC)
	e.insertTestopsTestRow(s.org, s.repoD, "team-old", s.rowD)
	return s
}

// A team of a TestOps test chart is the owner of the repository as of the report run, never the stored team_id: a team
// filter keeps the filtered teams' repositories (a shared repository once), a team group is one bucket per owner team with a
// co-owned repository whole in each, an unowned repository is in no team bucket, a team that owned a repository only before
// the run has no row, and the organization chart is unchanged.
func TestRegistryTestopsTestTeamIsRepositoryOwnership(t *testing.T) {
	e := startPooledEnv(t)
	s := seedTestopsTestOwnership(e)
	of := func(rows ...testopsTestRow) []testopsTestRow { return rows }
	const start, end = "2026-01-01", "2026-01-06"
	for _, metric := range testopsTestStoredKeys {
		chart := func(groupBy, chartType string, teams ...string) map[string]float64 {
			return e.testopsTestChart(s.org, metric, groupBy, chartType, start, end, teams...)
		}
		for _, chartType := range []string{"scorecard", "bar"} {
			mustEqualTestopsBuckets(t, chartType+" organization", metric, chart("", chartType), map[string][]testopsTestRow{"total": of(s.rowA, s.rowB, s.rowC, s.rowD)}, chartType)
			mustEqualTestopsBuckets(t, chartType+" team-1", metric, chart("", chartType, "team-1"), map[string][]testopsTestRow{"total": of(s.rowA, s.rowB)}, chartType)
			mustEqualTestopsBuckets(t, chartType+" co-owner team-2", metric, chart("", chartType, "team-2"), map[string][]testopsTestRow{"total": of(s.rowB)}, chartType)
			mustEqualTestopsBuckets(t, chartType+" team-1 and team-2, B once", metric, chart("", chartType, "team-1", "team-2"), map[string][]testopsTestRow{"total": of(s.rowA, s.rowB)}, chartType)
			mustEqualTestopsBuckets(t, chartType+" owner at the run team-new", metric, chart("", chartType, "team-new"), map[string][]testopsTestRow{"total": of(s.rowD)}, chartType)
			mustEqualTestopsBuckets(t, chartType+" team-old owns no repository at the run", metric, chart("", chartType, "team-old"), map[string][]testopsTestRow{"total": {}}, chartType)
		}
		mustEqualTestopsBuckets(t, "team group", metric, chart("team", "bar"),
			map[string][]testopsTestRow{"team-1": of(s.rowA, s.rowB), "team-2": of(s.rowB), "team-new": of(s.rowD)}, "bar")
		mustEqualTestopsBuckets(t, "team group with filter team-2", metric, chart("team", "bar", "team-2"),
			map[string][]testopsTestRow{"team-2": of(s.rowB)}, "bar")
		// The service group is unchanged: the stored service (NULL here, one "" bucket).
		mustEqualTestopsBuckets(t, "service group", metric, chart("service", "bar"),
			map[string][]testopsTestRow{"": of(s.rowA, s.rowB, s.rowC, s.rowD)}, "bar")
	}
}

// A team that owns no repository: the TestOps test charts give a count of 0 on a scorecard and no point for a rate, never a
// stored row of another team. Its own cell: ticket 14106 follows the CI/CD and pipeline charts here.
func TestRegistryTestopsTestTeamWithNoRepository(t *testing.T) {
	e := startPooledEnv(t)
	s := seedTestopsTestOwnership(e)
	for _, metric := range testopsTestStoredKeys {
		got := e.testopsTestChart(s.org, metric, "", "scorecard", "2026-01-01", "2026-01-06", "team-none")
		if want, ok := testopsTestWant(metric, nil); ok {
			mustEqual(t, metric+" team with no repository", got, "total", want)
		} else {
			mustHaveNoPoint(t, fmt.Sprintf("%s team with no repository", metric), got)
		}
	}
}

// An organization chart over a window with no row: counts 0, every rate no point, never NaN. The ratio columns are Nullable
// (internal/chmigrate/baseline/head.json), so a mean of no row is NULL; with rows every value is the stored one.
func TestRegistryTestopsTestEmptyWindowIsNoPoint(t *testing.T) {
	e := startPooledEnv(t)
	s := seedTestopsTestOwnership(e)
	for _, metric := range testopsTestStoredKeys {
		for _, chartType := range []string{"scorecard", "trend_delta", "table"} {
			mustEqualTestopsBuckets(t, chartType+" organization, window with no row", metric,
				e.testopsTestChart(s.org, metric, "", chartType, "2026-02-01", "2026-02-06"), map[string][]testopsTestRow{"total": {}}, "scorecard")
			mustEqualTestopsBuckets(t, chartType+" organization with no row at all", metric,
				e.testopsTestChart(e.newOrg(), metric, "", chartType, "2026-01-01", "2026-01-06"), map[string][]testopsTestRow{"total": {}}, "scorecard")
		}
	}
}
