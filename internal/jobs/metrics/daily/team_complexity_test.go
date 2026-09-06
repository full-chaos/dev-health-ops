package daily

import (
	"testing"
	"time"

	"github.com/google/uuid"
)

var (
	teamComplexityTestDay        = time.Date(2026, 8, 28, 0, 0, 0, 0, time.UTC)
	teamComplexityTestComputedAt = time.Date(2026, 8, 28, 12, 0, 0, 0, time.UTC)
)

// TestBuildTeamComplexityRowsAggregatesByOwnershipAndComputesRatio ports
// test_aggregates_by_ownership_and_computes_ratio.
func TestBuildTeamComplexityRowsAggregatesByOwnershipAndComputesRatio(t *testing.T) {
	repoID := uuid.New()
	rows := []repoComplexityInput{
		{RepoID: repoID, LOCTotal: 2000, CyclomaticTotal: 100, HighComplexityFunctions: 3, VeryHighComplexityFunctions: 1},
	}
	repoToTeam := map[string]string{repoID.String(): "gh:platform"}

	records := buildTeamComplexityRows("acme", teamComplexityTestDay, rows, repoToTeam, teamComplexityTestComputedAt)

	if len(records) != 1 {
		t.Fatalf("records=%d, want 1: %#v", len(records), records)
	}
	row := records[0]
	if row.TeamID != "gh:platform" {
		t.Fatalf("team_id=%q, want gh:platform", row.TeamID)
	}
	if row.OrganizationID != "acme" {
		t.Fatalf("org_id=%q, want acme", row.OrganizationID)
	}
	if !row.Day.Equal(teamComplexityTestDay) {
		t.Fatalf("day=%v, want %v", row.Day, teamComplexityTestDay)
	}
	if row.LOCTotal != 2000 {
		t.Fatalf("loc_total=%d, want 2000", row.LOCTotal)
	}
	if row.CyclomaticTotal != 100 {
		t.Fatalf("cyclomatic_total=%d, want 100", row.CyclomaticTotal)
	}
	if row.CyclomaticPerKLOC != 50.0 { // 100 / (2000/1000)
		t.Fatalf("cyclomatic_per_kloc=%v, want 50.0", row.CyclomaticPerKLOC)
	}
	if row.HighComplexityFunctions != 3 {
		t.Fatalf("high_complexity_functions=%d, want 3", row.HighComplexityFunctions)
	}
	if row.VeryHighComplexityFunctions != 1 {
		t.Fatalf("very_high_complexity_functions=%d, want 1", row.VeryHighComplexityFunctions)
	}
	if row.ContributingRepoCount != 1 {
		t.Fatalf("contributing_repo_count=%d, want 1", row.ContributingRepoCount)
	}
}

// TestBuildTeamComplexityRowsSumsAcrossEveryRepoAndRecomputesTheRatio ports
// test_sums_across_every_repo_the_team_owns_and_recomputes_the_ratio: the
// loc-weighted ratio (14.0) must differ from a naive per-repo average (30.0).
func TestBuildTeamComplexityRowsSumsAcrossEveryRepoAndRecomputesTheRatio(t *testing.T) {
	repoA, repoB := uuid.New(), uuid.New()
	rows := []repoComplexityInput{
		// repo_a: 1000 loc, 50 cc -> 50.0 cc/kloc alone.
		{RepoID: repoA, LOCTotal: 1000, CyclomaticTotal: 50, HighComplexityFunctions: 2, VeryHighComplexityFunctions: 0},
		// repo_b: 9000 loc, 90 cc -> 10.0 cc/kloc alone.
		{RepoID: repoB, LOCTotal: 9000, CyclomaticTotal: 90, HighComplexityFunctions: 1, VeryHighComplexityFunctions: 1},
	}
	repoToTeam := map[string]string{repoA.String(): "gh:platform", repoB.String(): "gh:platform"}

	records := buildTeamComplexityRows("acme", teamComplexityTestDay, rows, repoToTeam, teamComplexityTestComputedAt)

	if len(records) != 1 {
		t.Fatalf("records=%d, want 1: %#v", len(records), records)
	}
	row := records[0]
	if row.LOCTotal != 10000 {
		t.Fatalf("loc_total=%d, want 10000", row.LOCTotal)
	}
	if row.CyclomaticTotal != 140 {
		t.Fatalf("cyclomatic_total=%d, want 140", row.CyclomaticTotal)
	}
	if row.HighComplexityFunctions != 3 {
		t.Fatalf("high_complexity_functions=%d, want 3", row.HighComplexityFunctions)
	}
	if row.VeryHighComplexityFunctions != 1 {
		t.Fatalf("very_high_complexity_functions=%d, want 1", row.VeryHighComplexityFunctions)
	}
	if row.ContributingRepoCount != 2 {
		t.Fatalf("contributing_repo_count=%d, want 2", row.ContributingRepoCount)
	}
	// Loc-weighted: (50 + 90) / (10000 / 1000) = 14.0. A naive average of the
	// two repos' own ratios (50.0 and 10.0) would give 30.0 -- very
	// different, and wrong (repo_b's 9x larger codebase should dominate).
	if row.CyclomaticPerKLOC != 14.0 {
		t.Fatalf("cyclomatic_per_kloc=%v, want 14.0 (loc-weighted, not averaged)", row.CyclomaticPerKLOC)
	}
	if row.CyclomaticPerKLOC == 30.0 {
		t.Fatalf("cyclomatic_per_kloc=30.0 -- looks like a naive per-repo average, not loc-weighted")
	}
}

// TestBuildTeamComplexityRowsLOCTotalZeroYieldsZeroRatioNotADivisionError
// ports test_loc_total_zero_yields_zero_ratio_not_a_division_error.
func TestBuildTeamComplexityRowsLOCTotalZeroYieldsZeroRatioNotADivisionError(t *testing.T) {
	repoID := uuid.New()
	rows := []repoComplexityInput{
		{RepoID: repoID, LOCTotal: 0, CyclomaticTotal: 0},
	}
	repoToTeam := map[string]string{repoID.String(): "gh:platform"}

	records := buildTeamComplexityRows("acme", teamComplexityTestDay, rows, repoToTeam, teamComplexityTestComputedAt)

	if len(records) != 1 {
		t.Fatalf("records=%d, want 1: %#v", len(records), records)
	}
	if records[0].CyclomaticPerKLOC != 0.0 {
		t.Fatalf("cyclomatic_per_kloc=%v, want 0.0", records[0].CyclomaticPerKLOC)
	}
}

// TestBuildTeamComplexityRowsUnownedRepoContributesToNoTeam ports
// test_a_repo_with_no_ownership_entry_contributes_to_no_team.
func TestBuildTeamComplexityRowsUnownedRepoContributesToNoTeam(t *testing.T) {
	unownedRepo := uuid.New()
	rows := []repoComplexityInput{
		{RepoID: unownedRepo, LOCTotal: 1000, CyclomaticTotal: 10},
	}

	records := buildTeamComplexityRows("acme", teamComplexityTestDay, rows, map[string]string{}, teamComplexityTestComputedAt)

	if len(records) != 0 {
		t.Fatalf("records=%d, want 0 (unowned repo must contribute to no team): %#v", len(records), records)
	}
}

// TestBuildTeamComplexityRowsEmptyInputProducesNoRows is the aggregator-level
// analogue of ComputeFinalizeFamily's own `if len(repoRows) == 0 { return
// 0, nil }` early exit.
func TestBuildTeamComplexityRowsEmptyInputProducesNoRows(t *testing.T) {
	records := buildTeamComplexityRows("acme", teamComplexityTestDay, nil, map[string]string{}, teamComplexityTestComputedAt)
	if len(records) != 0 {
		t.Fatalf("records=%d, want 0", len(records))
	}
}
