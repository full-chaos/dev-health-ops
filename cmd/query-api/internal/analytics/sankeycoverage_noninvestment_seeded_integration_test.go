//go:build integration

package analytics

// Seeded real-engine proof for resolveSankeyCoverage's useInvestment=false
// path (compileSankeyCoverage's default branch, investment_metrics_daily).
// Reuses investmentMetricsDailyDDL/seedInvestmentMetricsDailyRows from
// breakdown_seeded_integration_test.go -- the same schema and seeding
// helper, same package, same build tag.
//
// RED before the toFloat64(...) fix: count()/countIf() are UInt64 in
// ClickHouse; resolveSankeyCoverage's total/assignedTeam/repoTotal/
// assignedRepo destinations are plain (non-pointer) float64, matching the
// investment path's Float64 sum()/sumIf() columns. clickhouse-go's
// UInt64.ScanRow only recognises *uint64/**uint64 as a scan target, so the
// query in this test fails at the scan stage on a real engine and
// resolveSankeyCoverage returns nil -- this is the exact reproduction on
// this useInvestment=false path specifically. No
// pre-existing test in this file exercised this path against a real
// engine (every other seeded test here passes useInvestment=true), which
// is why the scan-type mismatch went undetected.
import (
	"context"
	"testing"
	"time"

	stdclickhouse "github.com/ClickHouse/clickhouse-go/v2"
	dhclickhouse "github.com/full-chaos/dev-health-go/clickhouse"

	"github.com/full-chaos/dev-health-ops/cmd/query-api/internal/graph/model"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

func TestResolveSankeyCoverage_NonInvestment_SeededRealClickHouse_ExactShares(t *testing.T) {
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

	if err := conn.Exec(ctx, investmentMetricsDailyDDL); err != nil {
		t.Fatalf("create investment_metrics_daily: %v", err)
	}

	client, err := dhclickhouse.NewClickHouseQueryClientWithOptions(dhclickhouse.Options{DSN: inst.URI})
	if err != nil {
		t.Fatalf("construct ClickHouse query client: %v", err)
	}
	defer func() { _ = client.Close() }()

	const orgID = "seeded-sankeycoverage-noninvestment"
	const otherOrgID = "seeded-sankeycoverage-noninvestment-OTHER-ORG"
	const day = "2026-01-05"

	// In-window, in-org rows: two team-assigned (t1, t3), one team-unassigned
	// (empty team_id -- assignedTeamExpr's nullIf(...,'')/ifNull(...,
	// 'unassigned') treats '' the same as a real NULL). All three carry a
	// repo_id, so repoTotal == assignedRepo == 3 and total == 3,
	// assignedTeam == 2 -- three distinct denominators/numerators, so a
	// column-order regression in the SELECT's four positions (rule: order is
	// part of the Scan contract, see compileSankeyCoverage's own comment)
	// would show up as a wrong ratio, not just a scan error.
	seedInvestmentMetricsDailyRows(t, ctx, conn, orgID, day, []investmentMetricsDailySeedRow{
		{repoID: "11111111-1111-1111-1111-111111111111", teamID: "t1", investmentArea: "feature", projectStream: "ps1", workItemsDone: 5},
		{repoID: "22222222-2222-2222-2222-222222222222", teamID: "", investmentArea: "feature", projectStream: "ps2", workItemsDone: 7},
		{repoID: "33333333-3333-3333-3333-333333333333", teamID: "t3", investmentArea: "bug", projectStream: "ps3", workItemsDone: 3},
	})
	// Out-of-org row: must NOT be counted -- proves org_id filtering still
	// works after the fix.
	seedInvestmentMetricsDailyRows(t, ctx, conn, otherOrgID, day, []investmentMetricsDailySeedRow{
		{repoID: "44444444-4444-4444-4444-444444444444", teamID: "t4", investmentArea: "feature", projectStream: "ps4", workItemsDone: 1000},
	})
	// Out-of-window row: must NOT be counted -- proves the date filter
	// still works after the fix.
	seedInvestmentMetricsDailyRows(t, ctx, conn, orgID, "2025-06-01", []investmentMetricsDailySeedRow{
		{repoID: "55555555-5555-5555-5555-555555555555", teamID: "t5", investmentArea: "feature", projectStream: "ps5", workItemsDone: 2000},
	})

	req, err := SankeyRequestFromInput(model.SankeyRequestInput{
		Path:    []model.DimensionInput{model.DimensionInputTeam, model.DimensionInputTheme},
		Measure: model.MeasureInputCount,
		DateRange: &model.DateRangeInput{
			StartDate: mustGraphQLDate("2026-01-01"),
			EndDate:   mustGraphQLDate("2026-01-08"),
		},
		MaxNodes: 16,
		MaxEdges: 100,
	})
	if err != nil {
		t.Fatalf("SankeyRequestFromInput: %v", err)
	}

	got := resolveSankeyCoverage(ctx, client, orgID, req, 60, false, nil)
	if got == nil {
		t.Fatal("resolveSankeyCoverage returned nil on a real ClickHouse -- the count()/countIf() UInt64-into-float64 scan mismatch")
	}
	const wantTeamCoverage = 2.0 / 3.0
	const wantRepoCoverage = 1.0
	if diff := got.TeamCoverage - wantTeamCoverage; diff > 1e-9 || diff < -1e-9 {
		t.Errorf("TeamCoverage = %v, want %v", got.TeamCoverage, wantTeamCoverage)
	}
	if diff := got.RepoCoverage - wantRepoCoverage; diff > 1e-9 || diff < -1e-9 {
		t.Errorf("RepoCoverage = %v, want %v", got.RepoCoverage, wantRepoCoverage)
	}
}
