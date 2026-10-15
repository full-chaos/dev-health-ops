//go:build integration

// Runs the org-series SQL against a real ClickHouse engine: the dedup source, the count sums, the null-team
// repository and the sumIf guard, none of which a fake row scanner can execute (CHAOS-9736).
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

const orgSeriesDDL = `
CREATE TABLE testops_pipeline_metrics_daily (
    repo_id UUID,
    day Date,
    pipelines_count UInt32,
    success_count UInt32,
    success_rate Float64,
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
`

func TestResolveOneTimeseries_OrgSeries_SeededRealClickHouse(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()

	inst, err := containers.StartClickHouse(ctx)
	if err != nil {
		t.Fatalf("start ClickHouse test dependency: %v", err)
	}
	defer func() { _ = inst.Close(context.Background()) }()
	opts, err := stdclickhouse.ParseDSN(inst.URI)
	if err != nil {
		t.Fatalf("parse ClickHouse DSN: %v", err)
	}
	conn, err := stdclickhouse.Open(opts)
	if err != nil {
		t.Fatalf("open raw ClickHouse connection: %v", err)
	}
	defer func() { _ = conn.Close() }()
	for _, stmt := range splitSQLStatements(orgSeriesDDL) {
		if err := conn.Exec(ctx, stmt); err != nil {
			t.Fatalf("exec DDL: %v", err)
		}
	}

	const orgID = "org-9736"
	const otherOrg = "org-9736-other"
	const repoA = "11111111-1111-1111-1111-111111111111"
	const repoB = "22222222-2222-2222-2222-222222222222"
	const repoNoTeam = "33333333-3333-3333-3333-333333333333"
	const repoForeign = "44444444-4444-4444-4444-444444444444"
	exec := func(q string) {
		t.Helper()
		if err := conn.Exec(ctx, q); err != nil {
			t.Fatalf("seed %q: %v", q, err)
		}
	}
	// Pipelines: team-a 1/1 (100%), team-b 1/9 (11.1%), a repository with no team 0/10, a foreign org 5/5.
	// Team mean = 55.6; the org rate = 2/20 = 10.
	for _, r := range []struct {
		repo, team, org string
		total, ok       int
	}{
		{repoA, "'team-a'", orgID, 1, 1},
		{repoB, "'team-b'", orgID, 9, 1},
		{repoNoTeam, "NULL", orgID, 10, 0},
		{repoForeign, "'team-x'", otherOrg, 5, 5},
	} {
		exec(fmt.Sprintf("INSERT INTO testops_pipeline_metrics_daily (repo_id, day, pipelines_count, success_count, success_rate, team_id, org_id, computed_at) VALUES ('%s', '2026-01-02', %d, %d, %f, %s, '%s', now())",
			r.repo, r.total, r.ok, float64(r.ok)/float64(r.total), r.team, r.org))
	}
	// A stale rewrite of repoA's day: the newest computed_at wins, so it must not count twice.
	exec(fmt.Sprintf("INSERT INTO testops_pipeline_metrics_daily (repo_id, day, pipelines_count, success_count, success_rate, team_id, org_id, computed_at) VALUES ('%s', '2026-01-02', 7, 0, 0, 'team-a', '%s', now() - INTERVAL 1 DAY)", repoA, orgID))
	// Coverage: team-a 50/50 (100%), team-b 30/150 (20%), a row with a percent but no counts: the org rate = 80/200 = 40.
	exec(fmt.Sprintf("INSERT INTO testops_coverage_metrics_daily (repo_id, day, line_coverage_pct, lines_total, lines_covered, team_id, org_id, computed_at) VALUES ('%s', '2026-01-02', 100, 50, 50, 'team-a', '%s', now()), ('%s', '2026-01-02', 20, 150, 30, 'team-b', '%s', now()), ('%s', '2026-01-02', 90, NULL, NULL, NULL, '%s', now())",
		repoA, orgID, repoB, orgID, repoNoTeam, orgID))

	client, err := dhclickhouse.NewClickHouseQueryClientWithOptions(dhclickhouse.Options{DSN: inst.URI})
	if err != nil {
		t.Fatalf("construct ClickHouse query client: %v", err)
	}
	defer func() { _ = client.Close() }()

	for _, tc := range []struct {
		measure model.MeasureInput
		want    float64
	}{
		{model.MeasureInputPipelineSuccessRate, 10},
		{model.MeasureInputCoverageLinePct, 40},
	} {
		got, err := resolveOneTimeseries(ctx, client, orgID, tsInput(model.DimensionInputTeam, tc.measure), false, nil, true)
		if err != nil {
			t.Fatalf("%s: %v", tc.measure, err)
		}
		var org *model.TimeseriesResult
		for i := range got {
			if got[i].Scope == model.TimeseriesScopeOrg {
				org = &got[i]
			}
		}
		if org == nil || len(org.Buckets) != 1 || org.Buckets[0].Value == nil {
			t.Fatalf("%s: no ORG series with one value in %+v", tc.measure, got)
		}
		if v := *org.Buckets[0].Value; math.Abs(v-tc.want) > 1e-9 {
			t.Errorf("%s: org value = %v, want %v (the counts, not the mean of the team rates)", tc.measure, v, tc.want)
		}
	}
}
