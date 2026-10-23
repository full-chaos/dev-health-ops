//go:build integration

// CHAOS-4730 red-first proof, against a REAL ClickHouse engine (never the
// routingFakeClient double every other test in this package uses). Every
// query CompileBreakdown/CompileTimeseries/CompileSankey/CompileFlowMatrix/
// CompileInvestmentMembershipScope generate ends with
// `SETTINGS max_execution_time = <bound timeout parameter>` -- a bound native
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
// written): CompileBreakdown's non-investment path, dimension=TEAM
// (team_id, a non-nullable LowCardinality(String) column -- picked over REPO
// specifically to avoid an unrelated Nullable-scan type question muddying
// this ticket's single claim), measure=COUNT (SUM(work_items_completed)).
//
// SCHEMA: work_item_completion_daily, the exact production DDL from
// internal/chmigrate/sql/119_work_item_completion_daily.sql -- the only table
// CompileBreakdown's non-investment/default-measure path reads
// (nonInvestmentSourceAndDateFilter falls through to
// workItemCompletionDedupSource for MeasureCount, whose
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

// workItemCompletionDailyDDL mirrors production: migration 119's table, folded into one CREATE for a fresh test
// instance (same discipline hotspots_argmax_tiebreak_integration_test.go's fileHotspotDailyDDL and
// seededQualitySchemaDDL both use).
const workItemCompletionDailyDDL = `
CREATE TABLE work_item_completion_daily
(
    org_id String,
    repo_id Nullable(UUID),
    day Date,
    team_id LowCardinality(String),
    delivery_units UInt32,
    work_items_completed UInt32,
    prs_merged UInt32,
    churn_loc UInt64,
    cycle_p50_hours Float64,
    computed_at DateTime DEFAULT now()
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(day)
ORDER BY (org_id, day, team_id)
SETTINGS allow_nullable_key = 1;
`

// workItemCompletionSeedRow is one seeded work_item_completion_daily row -- only the columns the breakdown reads
// (team, work_items_completed) vary; every other NOT NULL column gets a fixed but valid filler.
type workItemCompletionSeedRow struct {
	repoID        string
	teamID        string
	workItemsDone uint32
}

func seedWorkItemCompletionRows(t *testing.T, ctx context.Context, conn stdclickhouse.Conn, orgID, day string, rows []workItemCompletionSeedRow) {
	t.Helper()
	values := ""
	for i, r := range rows {
		if i > 0 {
			values += ", "
		}
		values += fmt.Sprintf(
			"(toUUID('%s'), toDate('%s'), '%s', 1, %d, 1, 10, 2.0, now(), '%s')",
			r.repoID, day, r.teamID, r.workItemsDone, orgID,
		)
	}
	insert := fmt.Sprintf(
		"INSERT INTO work_item_completion_daily (repo_id, day, team_id, delivery_units, work_items_completed, prs_merged, churn_loc, cycle_p50_hours, computed_at, org_id) VALUES %s",
		values,
	)
	if err := conn.Exec(ctx, insert); err != nil {
		t.Fatalf("seed work_item_completion_daily: %v", err)
	}
}

// TestCompileBreakdown_ThemeCount_SeededRealClickHouse_ExactAggregate is
// CHAOS-4730's red-first proof: this compiles and RUNS CompileBreakdown's
// generated SQL against a real ClickHouse engine (containers.StartClickHouse
// -- the exact Testcontainers pin CI's integration shards use), not a
// fake row-scanner double.
//
// RED on origin/main (512c4e77b) and on any commit still emitting
// `SETTINGS max_execution_time = <bound timeout parameter>`: the query fails to
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

	if err := conn.Exec(ctx, workItemCompletionDailyDDL); err != nil {
		t.Fatalf("create work_item_completion_daily: %v", err)
	}

	client, err := dhclickhouse.NewClickHouseQueryClientWithOptions(dhclickhouse.Options{DSN: inst.URI})
	if err != nil {
		t.Fatalf("construct ClickHouse query client: %v", err)
	}
	defer func() { _ = client.Close() }()

	const orgID = "chaos-4730-seeded-breakdown"
	const otherOrgID = "chaos-4730-seeded-breakdown-OTHER-ORG"
	const day = "2026-01-05"

	// In-window, in-org rows: two rows of team "t1" (different repositories, so the dedup subquery's GROUP BY
	// (org_id, day, repo_id, team_id) keeps them as two distinct groups BEFORE the outer breakdown GROUP BY team_id
	// combines them -- this exercises the real two-level aggregation, not just a pass-through) and one of "t3".
	seedWorkItemCompletionRows(t, ctx, conn, orgID, day, []workItemCompletionSeedRow{
		{repoID: "11111111-1111-1111-1111-111111111111", teamID: "t1", workItemsDone: 5},
		{repoID: "22222222-2222-2222-2222-222222222222", teamID: "t1", workItemsDone: 7},
		{repoID: "33333333-3333-3333-3333-333333333333", teamID: "t3", workItemsDone: 3},
	})
	// Out-of-org row: same day/theme, must NOT be counted -- proves the
	// {org_id:String} predicate still filters correctly after the fix.
	seedWorkItemCompletionRows(t, ctx, conn, otherOrgID, day, []workItemCompletionSeedRow{
		{repoID: "44444444-4444-4444-4444-444444444444", teamID: "t1", workItemsDone: 1000},
	})
	// Out-of-window row: same org/theme, a day outside the query's
	// [start_date,end_date] range -- must NOT be counted, proving the
	// date predicate still filters correctly after the fix.
	seedWorkItemCompletionRows(t, ctx, conn, orgID, "2025-06-01", []workItemCompletionSeedRow{
		{repoID: "55555555-5555-5555-5555-555555555555", teamID: "t1", workItemsDone: 2000},
	})

	req := BreakdownRequest{
		Dimension: DimensionTeam,
		Measure:   MeasureCount,
		StartDate: mustGraphQLDate("2026-01-01"),
		EndDate:   mustGraphQLDate("2026-01-09"), // the exclusive end of a request built by hand: the last day read is 2026-01-08
		TopN:      10,
	}
	q, err := compileBreakdownOver(req, "team_id", orgID, queryTimeoutSecs, false, nil)
	if err != nil {
		t.Fatalf("CompileBreakdown: %v", err)
	}

	result, err := ExecuteBreakdown(ctx, client, "org-1", q, "team", "count")
	if err != nil {
		t.Fatalf("ExecuteBreakdown: %v (this is the CHAOS-4730 defect if the message contains "+
			"\"Syntax error\" / \"code: 62\" / \"Expected substitution type\")", err)
	}

	if len(result.Items) != 2 {
		t.Fatalf("len(Items) = %d, want 2 (t1, t3) -- got %+v", len(result.Items), result.Items)
	}

	// ORDER BY value DESC, dimension_value ASC: t1 (12) then t3 (3).
	first, second := result.Items[0], result.Items[1]
	if first.Key != "t1" {
		t.Errorf("Items[0].Key = %q, want %q", first.Key, "t1")
	}
	if first.Value == nil || *first.Value != 12 {
		t.Errorf("Items[0].Value = %v, want 12 (5+7, out-of-org/out-of-window rows excluded)", first.Value)
	}
	if second.Key != "t3" {
		t.Errorf("Items[1].Key = %q, want %q", second.Key, "t3")
	}
	if second.Value == nil || *second.Value != 3 {
		t.Errorf("Items[1].Value = %v, want 3", second.Value)
	}

	// CHAOS-8497: request the two known keys while topN is deliberately one.
	// The answer must retain both requested values: the key set defines this
	// response, so a measure's independent topN ranking cannot omit a join key.
	exactReq := req
	exactReq.TopN = 1
	exactReq.Keys = []string{"t1", "t3"}
	exactQuery, err := compileBreakdownOver(exactReq, "team_id", orgID, queryTimeoutSecs, false, nil)
	if err != nil {
		t.Fatalf("CompileBreakdown exact keys: %v", err)
	}
	exact, err := ExecuteBreakdown(ctx, client, "org-1", exactQuery, "team", "count")
	if err != nil {
		t.Fatalf("ExecuteBreakdown exact keys: %v", err)
	}
	if len(exact.Items) != 2 {
		t.Fatalf("exact-key len(Items) = %d, want 2 despite topN=1: %+v", len(exact.Items), exact.Items)
	}
	if exact.Items[0].Key != "t1" || exact.Items[0].Value == nil || *exact.Items[0].Value != 12 {
		t.Errorf("exact Items[0] = %+v, want t1=12", exact.Items[0])
	}
	if exact.Items[1].Key != "t3" || exact.Items[1].Value == nil || *exact.Items[1].Value != 3 {
		t.Errorf("exact Items[1] = %+v, want t3=3", exact.Items[1])
	}
}
