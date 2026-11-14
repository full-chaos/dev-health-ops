//go:build integration

// The TestOps rate measures are one rule: the rate of a group is its summed numerators over its summed denominators,
// never the mean of the stored per-repository-day rates (CHAOS-13775). Each measure runs against a real ClickHouse
// engine with two repositories of unequal weight under one team, where the two answers differ.
package analytics

import (
	"context"
	"fmt"
	"math"
	"testing"
	"time"

	stdclickhouse "github.com/ClickHouse/clickhouse-go/v2"
	dhclickhouse "github.com/full-chaos/dev-health-go/clickhouse"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/graph/model"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

const weightedRateDDL = `
CREATE TABLE testops_pipeline_metrics_daily (
    repo_id UUID,
    day Date,
    pipelines_count UInt32,
    success_count UInt32,
    failure_count UInt32,
    success_rate Float64,
    failure_rate Float64,
    rerun_rate Float64,
    team_id Nullable(String),
    org_id LowCardinality(String) DEFAULT '',
    computed_at DateTime('UTC')
) ENGINE MergeTree ORDER BY (repo_id, day);

CREATE TABLE testops_coverage_metrics_daily (
    repo_id UUID,
    day Date,
    line_coverage_pct Nullable(Float64),
    lines_total Nullable(UInt32),
    lines_covered Nullable(UInt32),
    team_id Nullable(String),
    org_id LowCardinality(String) DEFAULT '',
    computed_at DateTime('UTC')
) ENGINE MergeTree ORDER BY (repo_id, day);

CREATE TABLE testops_test_metrics_daily (
    repo_id UUID,
    day Date,
    total_cases UInt32,
    passed_count UInt32,
    failed_count UInt32,
    skipped_count UInt32,
    pass_rate Float64,
    failure_rate Float64,
    flake_rate Float64,
    flake_cases Nullable(UInt32),
    distinct_cases Nullable(UInt32),
    team_id Nullable(String),
    org_id LowCardinality(String) DEFAULT '',
    computed_at DateTime('UTC')
) ENGINE MergeTree ORDER BY (repo_id, day);
`

func TestTestOpsRateMeasures_AreWeightedNotAMeanOfRates_RealClickHouse(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()
	inst, err := containers.StartClickHouse(ctx)
	if err != nil {
		t.Fatalf("start ClickHouse test dependency: %v", err)
	}
	defer func() { _ = inst.Close(context.Background()) }()
	opts, err := stdclickhouse.ParseDSN(inst.URI)
	if err != nil {
		t.Fatal(err)
	}
	conn, err := stdclickhouse.Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close() }()
	for _, stmt := range splitSQLStatements(weightedRateDDL) {
		if err := conn.Exec(ctx, stmt); err != nil {
			t.Fatalf("exec DDL: %v", err)
		}
	}
	exec := func(q string) {
		t.Helper()
		if err := conn.Exec(ctx, q); err != nil {
			t.Fatalf("seed %q: %v", q, err)
		}
	}

	const orgID = "org-13775"
	const big, small, empty, noTeam = "11111111-0000-0000-0000-000000000001", "11111111-0000-0000-0000-000000000002", "11111111-0000-0000-0000-000000000003", "11111111-0000-0000-0000-000000000004"
	const cFull, cPart = "11111111-0000-0000-0000-000000000005", "11111111-0000-0000-0000-000000000006"

	// Pipelines of team-a: a repository of 100 runs (90 succeeded, 10 failed, 20 reruns) and one of 10 runs (all
	// succeeded, no rerun). Mean of the stored rates: success 95, failure 5, rerun 10. Weighted: 100/110, 10/110, 20/110.
	// team-b holds a repository with no run: nothing to divide by.
	for _, r := range []struct {
		repo, team            string
		runs, ok, failed, rer int
	}{
		{big, "'team-a'", 100, 90, 10, 20},
		{small, "'team-a'", 10, 10, 0, 0},
		{empty, "'team-b'", 0, 0, 0, 0},
	} {
		rate := func(n int) float64 {
			if r.runs == 0 {
				return 0
			}
			return float64(n) / float64(r.runs)
		}
		exec(fmt.Sprintf("INSERT INTO testops_pipeline_metrics_daily (repo_id, day, pipelines_count, success_count, failure_count, success_rate, failure_rate, rerun_rate, team_id, org_id, computed_at) VALUES ('%s', '2026-01-02', %d, %d, %d, %v, %v, %v, %s, '%s', now())",
			r.repo, r.runs, r.ok, r.failed, rate(r.ok), rate(r.failed), rate(r.rer), r.team, orgID))
	}

	// Test cases of team-a: 999,000 passed + 1,000 failed + 50,000 skipped, and 90 passed + 10 failed. Mean of the stored
	// pass rates (over total_cases) is far from the weighted rate, which leaves skipped cases out of the denominator.
	// Flake: 50 of 1,000 and 1 of 2 case names. team-b holds only skipped cases. team-c has one row without the flake
	// counts. A repository with no team has 10 passed and 10 failed.
	for _, r := range []struct {
		repo, team              string
		passed, failed, skipped int
		flake, distinct         string
	}{
		{big, "'team-a'", 999000, 1000, 50000, "50", "1000"},
		{small, "'team-a'", 90, 10, 0, "1", "2"},
		{empty, "'team-b'", 0, 0, 9, "0", "0"},
		{cFull, "'team-c'", 5, 0, 0, "1", "10"},
		{cPart, "'team-c'", 5, 0, 0, "NULL", "NULL"},
		{noTeam, "NULL", 10, 10, 0, "NULL", "NULL"},
	} {
		total := r.passed + r.failed + r.skipped
		div := func(n int) float64 {
			if total == 0 {
				return 0
			}
			return float64(n) / float64(total)
		}
		exec(fmt.Sprintf("INSERT INTO testops_test_metrics_daily (repo_id, day, total_cases, passed_count, failed_count, skipped_count, pass_rate, failure_rate, flake_rate, flake_cases, distinct_cases, team_id, org_id, computed_at) VALUES ('%s', '2026-01-02', %d, %d, %d, %d, %v, %v, 0.5, %s, %s, %s, '%s', now())",
			r.repo, total, r.passed, r.failed, r.skipped, div(r.passed), div(r.failed), r.flake, r.distinct, r.team, orgID))
	}

	// Line coverage of team-a: 9,000 of 10,000 lines and 10 of 100 lines; a row with a percent and no counts adds to
	// neither side. Mean of the percents 50; weighted 9,010 / 10,100. team-b: a repository with no line.
	exec(fmt.Sprintf("INSERT INTO testops_coverage_metrics_daily (repo_id, day, line_coverage_pct, lines_total, lines_covered, team_id, org_id, computed_at) VALUES ('%s', '2026-01-02', 90, 10000, 9000, 'team-a', '%s', now()), ('%s', '2026-01-02', 10, 100, 10, 'team-a', '%s', now()), ('%s', '2026-01-02', 99, NULL, 500, 'team-a', '%s', now()), ('%s', '2026-01-02', NULL, 0, 0, 'team-b', '%s', now())",
		big, orgID, small, orgID, noTeam, orgID, empty, orgID))

	client, err := dhclickhouse.NewClickHouseQueryClientWithOptions(dhclickhouse.Options{DSN: inst.URI})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = client.Close() }()

	series := func(measure model.MeasureInput) (map[string]*float64, *float64) {
		t.Helper()
		got, err := resolveOneTimeseries(ctx, client, orgID, tsInput(model.DimensionInputTeam, measure), false, nil, true)
		if err != nil {
			t.Fatalf("%s: %v", measure, err)
		}
		teams := map[string]*float64{}
		var org *float64
		for _, s := range got {
			if len(s.Buckets) != 1 {
				t.Fatalf("%s %s: %d buckets, want 1", measure, s.DimensionValue, len(s.Buckets))
			}
			if s.Scope == model.TimeseriesScopeOrg {
				org = s.Buckets[0].Value
			} else {
				teams[s.DimensionValue] = s.Buckets[0].Value
			}
		}
		return teams, org
	}
	near := func(name string, got *float64, want float64) {
		t.Helper()
		if got == nil || math.Abs(*got-want) > 1e-9 {
			t.Errorf("%s = %v, want %v", name, deref(got), want)
		}
	}
	isNull := func(name string, got *float64) {
		t.Helper()
		if got != nil {
			t.Errorf("%s = %v, want NULL (nothing to divide by is not a rate of 0)", name, *got)
		}
	}

	for _, tc := range []struct {
		measure model.MeasureInput
		want    float64
	}{
		{model.MeasureInputPipelineSuccessRate, 100.0 / 110 * 100},
		{model.MeasureInputPipelineFailureRate, 10.0 / 110 * 100},
		// Known integer reruns: 20 of 110 runs. The stored rate is reruns / runs, so the product form is the count.
		{model.MeasureInputPipelineRerunRate, 20.0 / 110 * 100},
	} {
		teams, _ := series(tc.measure)
		near(string(tc.measure)+" team-a", teams["team-a"], tc.want)
		isNull(string(tc.measure)+" team-b", teams["team-b"])
	}

	passed, failed := 999000.0+90, 1000.0+10
	pass, passOrg := series(model.MeasureInputTestPassRate)
	near("TEST_PASS_RATE team-a", pass["team-a"], passed/(passed+failed)*100)
	isNull("TEST_PASS_RATE team-b (only skipped cases)", pass["team-b"])
	near("TEST_PASS_RATE ORG (includes the repository with no team)", passOrg, (passed+10+10)/(passed+10+10+failed+10)*100)
	fail, failOrg := series(model.MeasureInputTestFailureRate)
	near("TEST_FAILURE_RATE team-a", fail["team-a"], failed/(passed+failed)*100)
	near("TEST_FAILURE_RATE ORG", failOrg, (failed+10)/(passed+10+10+failed+10)*100)
	if pass["team-a"] != nil && fail["team-a"] != nil && math.Abs(*pass["team-a"]+*fail["team-a"]-100) > 1e-9 {
		t.Errorf("pass %v + failure %v != 100 (skipped cases are in neither side)", *pass["team-a"], *fail["team-a"])
	}

	flake, _ := series(model.MeasureInputTestFlakeRate)
	near("TEST_FLAKE_RATE team-a", flake["team-a"], 51.0/1002*100)
	isNull("TEST_FLAKE_RATE team-b (no distinct case)", flake["team-b"])
	isNull("TEST_FLAKE_RATE team-c (one row without the counts)", flake["team-c"])

	cov, _ := series(model.MeasureInputCoverageLinePct)
	near("COVERAGE_LINE_PCT team-a", cov["team-a"], 9010.0/10100*100)
	isNull("COVERAGE_LINE_PCT team-b (no line)", cov["team-b"])

	// The breakdown and the Sankey readers share the expression: one smoke that it executes there too.
	q, err := CompileBreakdown(BreakdownRequest{Dimension: DimensionTeam, Measure: MeasureTestPassRate, StartDate: mustDate(t, "2026-01-01"), EndDate: mustDate(t, "2026-01-07"), TopN: 10}, orgID, 120, false, nil)
	if err != nil {
		t.Fatal(err)
	}
	bd, err := ExecuteBreakdown(ctx, client, orgID, q, "team", "test_pass_rate")
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, item := range bd.Items {
		if item.Key == "team-a" {
			found = true
			near("breakdown TEST_PASS_RATE team-a", item.Value, passed/(passed+failed)*100)
		}
	}
	if !found {
		t.Errorf("breakdown has no team-a item: %+v", bd.Items)
	}
}

func deref(p *float64) any {
	if p == nil {
		return nil
	}
	return *p
}
