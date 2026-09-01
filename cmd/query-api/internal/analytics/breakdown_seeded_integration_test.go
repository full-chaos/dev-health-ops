//go:build integration

// CHAOS-4730 red-first proof, against a REAL ClickHouse engine (never the
// routingFakeClient double every other test in this package uses). Every
// query CompileBreakdown/CompileTimeseries/CompileSankey/CompileFlowMatrix/
// CompileInvestmentMembershipScope generate ends with
// `SETTINGS max_execution_time = {timeout:UInt64}` -- a bound native
// ClickHouse parameter substituted INSIDE a SETTINGS clause. That fails to
// PARSE (ClickHouse Code: 62, "Expected substitution type (identifier)")
// on 26.6.1.1193, the exact digest-pinned image
// internal/testsupport/containers.StartClickHouse uses for every
// Testcontainers integration test in this repo -- while parsing fine on
// 26.7.5.10 (dev-stack/prod). No existing test in this package ever
// executed real SQL against a real engine, so the gap was invisible until
// CHAOS-4723's parked seeded test hit it first (see that ticket + this
// one's body for the three independent repros).
//
// This file proves the SAME defect against breakdown.go specifically --
// the simplest of the 13 affected sites -- rather than depending on
// investmentquality.go (CHAOS-4723, not yet on main when this file was
// written): CompileBreakdown's non-investment path, dimension=THEME
// (investment_area, a non-nullable LowCardinality(String) column -- picked
// over TEAM/REPO specifically to avoid an unrelated Nullable-scan type
// question muddying this ticket's single claim), measure=COUNT
// (SUM(work_items_completed)).
//
// SCHEMA: investment_metrics_daily, the exact production DDL from
// src/dev_health_ops/migrations/clickhouse/007_complexity_investment_issues.sql:73-90
// plus 024_add_org_id.sql:51's org_id column -- the only table
// CompileBreakdown's non-investment/default-measure path reads
// (nonInvestmentSourceAndDateFilter falls through to
// investmentMetricsDailyDedupSource for MeasureCount, whose
// measureSourceTable is "").
package analytics

import (
	"context"
	"fmt"
	"testing"
	"time"

	stdclickhouse "github.com/ClickHouse/clickhouse-go/v2"
	dhclickhouse "github.com/full-chaos/dev-health-go/clickhouse"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

// investmentMetricsDailyDDL mirrors production exactly: 007's base table
// plus 024's org_id ALTER, folded into one CREATE for a fresh test
// instance (same discipline hotspots_argmax_tiebreak_integration_test.go's
// fileHotspotDailyDDL and the CHAOS-4723 parked test's
// seededQualitySchemaDDL both use).
const investmentMetricsDailyDDL = `
CREATE TABLE investment_metrics_daily
(
    repo_id Nullable(UUID),
    day Date,
    team_id LowCardinality(Nullable(String)),
    investment_area LowCardinality(String),
    project_stream LowCardinality(String),
    delivery_units UInt32,
    work_items_completed UInt32,
    prs_merged UInt32,
    churn_loc UInt64,
    cycle_p50_hours Float64,
    computed_at DateTime DEFAULT now(),
    org_id String DEFAULT 'default'
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(day)
ORDER BY (day, team_id, investment_area, project_stream)
SETTINGS allow_nullable_key = 1;
`

// investmentMetricsDailySeedRow is one seeded investment_metrics_daily
// row -- only the columns CompileBreakdown's THEME/COUNT combination
// actually reads (investment_area, work_items_completed) vary
// meaningfully across rows; every other NOT NULL column gets a fixed but
// valid filler.
type investmentMetricsDailySeedRow struct {
	repoID         string
	teamID         string
	investmentArea string
	projectStream  string
	workItemsDone  uint32
}

func seedInvestmentMetricsDailyRows(t *testing.T, ctx context.Context, conn stdclickhouse.Conn, orgID, day string, rows []investmentMetricsDailySeedRow) {
	t.Helper()
	values := ""
	for i, r := range rows {
		if i > 0 {
			values += ", "
		}
		values += fmt.Sprintf(
			"(toUUID('%s'), toDate('%s'), '%s', '%s', '%s', 1, %d, 1, 10, 2.0, now(), '%s')",
			r.repoID, day, r.teamID, r.investmentArea, r.projectStream, r.workItemsDone, orgID,
		)
	}
	insert := fmt.Sprintf(
		"INSERT INTO investment_metrics_daily (repo_id, day, team_id, investment_area, project_stream, delivery_units, work_items_completed, prs_merged, churn_loc, cycle_p50_hours, computed_at, org_id) VALUES %s",
		values,
	)
	if err := conn.Exec(ctx, insert); err != nil {
		t.Fatalf("seed investment_metrics_daily: %v", err)
	}
}

// TestCompileBreakdown_ThemeCount_SeededRealClickHouse_ExactAggregate is
// CHAOS-4730's red-first proof: this compiles and RUNS CompileBreakdown's
// generated SQL against a real ClickHouse engine (containers.StartClickHouse
// -- the exact Testcontainers pin CI's integration shards use), not a
// fake row-scanner double.
//
// RED on origin/main (512c4e77b) and on any commit still emitting
// `SETTINGS max_execution_time = {timeout:UInt64}`: the query fails to
// PARSE with ClickHouse Code: 62, "Expected substitution type
// (identifier)" -- see this ticket's repros 1/2. GREEN once the timeout
// is rendered as a literal integer in the SQL text.
func TestCompileBreakdown_ThemeCount_SeededRealClickHouse_ExactAggregate(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
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

	if err := conn.Exec(ctx, investmentMetricsDailyDDL); err != nil {
		t.Fatalf("create investment_metrics_daily: %v", err)
	}

	client, err := dhclickhouse.NewClickHouseQueryClientWithOptions(dhclickhouse.Options{DSN: inst.URI})
	if err != nil {
		t.Fatalf("construct ClickHouse query client: %v", err)
	}
	defer func() { _ = client.Close() }()

	const orgID = "chaos-4730-seeded-breakdown"
	const otherOrgID = "chaos-4730-seeded-breakdown-OTHER-ORG"
	const day = "2026-01-05"

	// In-window, in-org rows: two "feature" (different repo/team, so the
	// dedup subquery's GROUP BY (org_id, day, repo_id, team_id,
	// investment_area, project_stream) keeps them as two distinct groups
	// BEFORE the outer breakdown GROUP BY investment_area combines them --
	// this exercises the real two-level aggregation, not just a
	// pass-through) and one "bug".
	seedInvestmentMetricsDailyRows(t, ctx, conn, orgID, day, []investmentMetricsDailySeedRow{
		{repoID: "11111111-1111-1111-1111-111111111111", teamID: "t1", investmentArea: "feature", projectStream: "ps1", workItemsDone: 5},
		{repoID: "22222222-2222-2222-2222-222222222222", teamID: "t2", investmentArea: "feature", projectStream: "ps2", workItemsDone: 7},
		{repoID: "33333333-3333-3333-3333-333333333333", teamID: "t3", investmentArea: "bug", projectStream: "ps3", workItemsDone: 3},
	})
	// Out-of-org row: same day/theme, must NOT be counted -- proves the
	// {org_id:String} predicate still filters correctly after the fix.
	seedInvestmentMetricsDailyRows(t, ctx, conn, otherOrgID, day, []investmentMetricsDailySeedRow{
		{repoID: "44444444-4444-4444-4444-444444444444", teamID: "t4", investmentArea: "feature", projectStream: "ps4", workItemsDone: 1000},
	})
	// Out-of-window row: same org/theme, a day outside the query's
	// [start_date,end_date] range -- must NOT be counted, proving the
	// date predicate still filters correctly after the fix.
	seedInvestmentMetricsDailyRows(t, ctx, conn, orgID, "2025-06-01", []investmentMetricsDailySeedRow{
		{repoID: "55555555-5555-5555-5555-555555555555", teamID: "t5", investmentArea: "feature", projectStream: "ps5", workItemsDone: 2000},
	})

	req := BreakdownRequest{
		Dimension: DimensionTheme,
		Measure:   MeasureCount,
		StartDate: mustGraphQLDate("2026-01-01"),
		EndDate:   mustGraphQLDate("2026-01-08"),
		TopN:      10,
	}
	q, err := CompileBreakdown(req, orgID, queryTimeoutSecs, false, nil)
	if err != nil {
		t.Fatalf("CompileBreakdown: %v", err)
	}

	result, err := ExecuteBreakdown(ctx, client, q, "theme", "count")
	if err != nil {
		t.Fatalf("ExecuteBreakdown: %v (this is the CHAOS-4730 defect if the message contains "+
			"\"Syntax error\" / \"code: 62\" / \"Expected substitution type\")", err)
	}

	if len(result.Items) != 2 {
		t.Fatalf("len(Items) = %d, want 2 (feature, bug) -- got %+v", len(result.Items), result.Items)
	}

	// ORDER BY value DESC, dimension_value ASC: feature (12) then bug (3).
	first, second := result.Items[0], result.Items[1]
	if first.Key != "feature" {
		t.Errorf("Items[0].Key = %q, want %q", first.Key, "feature")
	}
	if first.Value == nil || *first.Value != 12 {
		t.Errorf("Items[0].Value = %v, want 12 (5+7, out-of-org/out-of-window rows excluded)", first.Value)
	}
	if second.Key != "bug" {
		t.Errorf("Items[1].Key = %q, want %q", second.Key, "bug")
	}
	if second.Value == nil || *second.Value != 3 {
		t.Errorf("Items[1].Value = %v, want 3", second.Value)
	}
}
