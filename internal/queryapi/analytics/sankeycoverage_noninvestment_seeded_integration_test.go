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
	"fmt"
	"testing"
	"time"

	stdclickhouse "github.com/ClickHouse/clickhouse-go/v2"
	dhclickhouse "github.com/full-chaos/dev-health-go/clickhouse"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/graph/model"
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

// seedInvestmentMetricsDailyGeneration inserts one investment_metrics_daily
// row with an EXPLICIT computed_at, unlike seedInvestmentMetricsDailyRows
// (breakdown_seeded_integration_test.go), which always writes now() and so
// cannot produce two distinct physical rows for the same natural key
// (org_id, day, repo_id, team_id, investment_area, project_stream) with a
// deterministic newest generation. repoID == "" writes a NULL repo_id.
func seedInvestmentMetricsDailyGeneration(t *testing.T, ctx context.Context, conn stdclickhouse.Conn, orgID, day, repoID, teamID, investmentArea, projectStream, computedAt string, workItemsDone uint32) {
	t.Helper()
	repo := "NULL"
	if repoID != "" {
		repo = fmt.Sprintf("toUUID('%s')", repoID)
	}
	insert := fmt.Sprintf(
		"INSERT INTO investment_metrics_daily (repo_id, day, team_id, investment_area, project_stream, delivery_units, work_items_completed, prs_merged, churn_loc, cycle_p50_hours, computed_at, org_id) VALUES (%s, toDate('%s'), '%s', '%s', '%s', 1, %d, 1, 10, 2.0, toDateTime('%s'), '%s')",
		repo, day, teamID, investmentArea, projectStream, workItemsDone, computedAt, orgID,
	)
	if err := conn.Exec(ctx, insert); err != nil {
		t.Fatalf("seed investment_metrics_daily generation: %v", err)
	}
}

// TestResolveSankeyCoverage_NonInvestment_SeededRealClickHouse_DedupesDailyGenerations
// proves the non-investment coverage query reads investment_metrics_daily
// through its deduped source (investmentMetricsDailyDedupSource,
// timeseries.go), not the raw table: investment_metrics_daily is a plain
// MergeTree that does not self-merge duplicate (re)writes of the same
// natural key, so a row re-synced more than once must be
// counted ONCE, at its newest generation, not once per physical row.
//
// Fixture: two distinct natural keys, deliberately split so team and
// repo assignment disagree between them -- a fixture where both keys
// carry the same repo/team shape would leave TeamCoverage and
// RepoCoverage moving together, and a regression in only one of the two
// scan/count sites would pass. Key A (team-assigned "t1", repo NULL) is
// written as TWO physical rows sharing the same (day, repo_id, team_id,
// investment_area, project_stream) -- two generations of the same sync,
// an hour apart. Key B (team-unassigned, repo-assigned) is written once.
// A raw, undeduped read counts THREE rows (A's two generations both
// survive): assignedTeam=2 (A counted twice), total=3 -> TeamCoverage
// 0.667; assignedRepo=1 (only B), repoTotal=3 -> RepoCoverage 0.333.
// Both are wrong, and wrong in the specific way a generation re-sync
// inflates a coverage ratio, not a uniform scale a reader could shrug
// off. The deduped read collapses A to its one logical key: total=2,
// assignedTeam=1 -> TeamCoverage 0.5; repoTotal=2, assignedRepo=1 ->
// RepoCoverage 0.5.
func TestResolveSankeyCoverage_NonInvestment_SeededRealClickHouse_DedupesDailyGenerations(t *testing.T) {
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

	const orgID = "seeded-sankeycoverage-dedup"
	const day = "2026-01-05"
	const repoB = "77777777-7777-7777-7777-777777777777"

	// Key A, generation 1 (older): team-assigned, repo NULL.
	seedInvestmentMetricsDailyGeneration(t, ctx, conn, orgID, day, "", "t1", "feature", "ps1", "2026-01-05 01:00:00", 5)
	// Key A, generation 2 (newer, same natural key) -- the re-sync.
	seedInvestmentMetricsDailyGeneration(t, ctx, conn, orgID, day, "", "t1", "feature", "ps1", "2026-01-05 02:00:00", 6)
	// Key B, one generation: team-unassigned, repo-assigned.
	seedInvestmentMetricsDailyGeneration(t, ctx, conn, orgID, day, repoB, "", "feature", "ps2", "2026-01-05 01:00:00", 3)

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
		t.Fatal("resolveSankeyCoverage returned nil on a real ClickHouse")
	}
	const wantTeamCoverage = 0.5
	const wantRepoCoverage = 0.5
	if diff := got.TeamCoverage - wantTeamCoverage; diff > 1e-9 || diff < -1e-9 {
		t.Errorf("TeamCoverage = %v, want %v (a re-synced generation must be counted once, not once per physical row)", got.TeamCoverage, wantTeamCoverage)
	}
	if diff := got.RepoCoverage - wantRepoCoverage; diff > 1e-9 || diff < -1e-9 {
		t.Errorf("RepoCoverage = %v, want %v (a re-synced generation must be counted once, not once per physical row)", got.RepoCoverage, wantRepoCoverage)
	}
}
