package daily

import (
	"encoding/json"
	"math"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/google/uuid"
)

// TestTeamComplexityMatchesTheFrozenPythonGolden is CHAOS-5051's parity
// proof, mirroring CHAOS-4290's ic_finalize precedent (PR3,
// parity-ic-finalize-oracle / #2293): team_complexity.py's
// build_team_complexity_rows_for_day is deleted in this same PR (CHAOS-3092
// no-straddle: the native Go executor is the sole writer once this PR
// lands, so a still-present Python compute path would be dead weight, not a
// live fallback).
//
// This is a FROZEN-golden test, not a live dual-execution one: there is no
// live Python left to execute at test time once this PR merges. The golden
// file (testdata/team_complexity_parity_golden.json) was captured ONCE, via
// a throwaway script run against the still-live
// build_team_complexity_rows_for_day before its deletion in this same
// commit -- see the PR body for the exact capture recipe; the script itself
// was never committed.
//
// Unlike ic_finalize's own parity test, this one needs no ClickHouse
// container: both build_team_complexity_rows_for_day (Python) and
// buildTeamComplexityRows (Go) are PURE functions over already-loaded rows
// -- the ClickHouse read (loadRepoComplexityInputsForDay) and write
// (teamComplexityWriter) around them are separately covered by this
// package's existing non-golden tests and carry no team-keyed aggregation
// logic of their own to prove parity on.
//
// The corpus (four repos, two teams, one orphan) exercises every branch
// build_team_complexity_rows_for_day's own doc comment describes:
//   - team-a: TWO owned repos (repoAlpha, repoBeta) -- proves the SUM
//     (not average) across owned repos, and cyclomatic_per_kloc RECOMPUTED
//     from the summed totals rather than averaged per-repo.
//   - team-b: ONE owned repo (repoGamma) -- the trivial single-repo case,
//     checked alongside team-a so the test cannot pass by accident on a
//     single-bucket implementation.
//   - repoOrphan: present in the input rows but ABSENT from repo_to_team --
//     must contribute to NO team ("a repo with no entry contributes to no
//     team row (never guessed)"). Deliberately given large, distinctive
//     values (9999s) so any accidental fallback bucket would be
//     unmistakable in a failure diff.
func TestTeamComplexityMatchesTheFrozenPythonGolden(t *testing.T) {
	golden := loadTeamComplexityParityGolden(t)

	day := time.Date(2026, 9, 4, 0, 0, 0, 0, time.UTC)
	computedAt := time.Date(2026, 9, 5, 10, 0, 0, 0, time.UTC)
	const orgID = "00000000-0000-4000-8000-0000000ec101"

	repoAlpha := uuid.MustParse("11111111-1111-4111-8111-111111111111")
	repoBeta := uuid.MustParse("22222222-2222-4222-8222-222222222222")
	repoGamma := uuid.MustParse("33333333-3333-4333-8333-333333333333")
	repoOrphan := uuid.MustParse("44444444-4444-4444-8444-444444444444")

	repoRows := []repoComplexityInput{
		{RepoID: repoAlpha, LOCTotal: 1000, CyclomaticTotal: 150, HighComplexityFunctions: 5, VeryHighComplexityFunctions: 1},
		{RepoID: repoBeta, LOCTotal: 500, CyclomaticTotal: 100, HighComplexityFunctions: 3, VeryHighComplexityFunctions: 0},
		{RepoID: repoGamma, LOCTotal: 2000, CyclomaticTotal: 50, HighComplexityFunctions: 1, VeryHighComplexityFunctions: 0},
		// Orphan: deliberately NOT in repoToTeam below.
		{RepoID: repoOrphan, LOCTotal: 9999, CyclomaticTotal: 9999, HighComplexityFunctions: 9, VeryHighComplexityFunctions: 9},
	}
	repoToTeam := map[string]string{
		repoAlpha.String(): "team-a",
		repoBeta.String():  "team-a",
		repoGamma.String(): "team-b",
		// repoOrphan deliberately absent.
	}

	got := buildTeamComplexityRows(orgID, day, repoRows, repoToTeam, computedAt)

	if len(got) != len(golden.TeamComplexity) {
		t.Fatalf("row count: golden=%d got=%d (got=%+v)", len(golden.TeamComplexity), len(got), got)
	}
	byTeam := make(map[string]teamComplexityRow, len(got))
	for _, row := range got {
		byTeam[row.TeamID] = row
	}
	for _, want := range golden.TeamComplexity {
		row, ok := byTeam[want.TeamID]
		if !ok {
			t.Fatalf("%s: missing from Go's output entirely (got=%+v)", want.TeamID, got)
		}
		if row.LOCTotal != want.LOCTotal {
			t.Errorf("%s: LOCTotal = %d, want %d", want.TeamID, row.LOCTotal, want.LOCTotal)
		}
		if row.CyclomaticTotal != want.CyclomaticTotal {
			t.Errorf("%s: CyclomaticTotal = %d, want %d", want.TeamID, row.CyclomaticTotal, want.CyclomaticTotal)
		}
		if math.Abs(row.CyclomaticPerKLOC-want.CyclomaticPerKLOC) > 1e-9 {
			t.Errorf("%s: CyclomaticPerKLOC = %v, want %v", want.TeamID, row.CyclomaticPerKLOC, want.CyclomaticPerKLOC)
		}
		if row.HighComplexityFunctions != want.HighComplexityFunctions {
			t.Errorf("%s: HighComplexityFunctions = %d, want %d", want.TeamID, row.HighComplexityFunctions, want.HighComplexityFunctions)
		}
		if row.VeryHighComplexityFunctions != want.VeryHighComplexityFunctions {
			t.Errorf("%s: VeryHighComplexityFunctions = %d, want %d", want.TeamID, row.VeryHighComplexityFunctions, want.VeryHighComplexityFunctions)
		}
		if row.ContributingRepoCount != want.ContributingRepoCount {
			t.Errorf("%s: ContributingRepoCount = %d, want %d", want.TeamID, row.ContributingRepoCount, want.ContributingRepoCount)
		}
	}
	// repoOrphan's distinctive 9999 values must not appear on ANY row --
	// the class of bug this corpus exists to catch (an accidental
	// "unassigned" bucket instead of the documented "never guessed" skip).
	for _, row := range got {
		if row.LOCTotal == 9999 || row.CyclomaticTotal == 9999 {
			t.Errorf("%s: carries repoOrphan's values -- an unowned repo must contribute to no team", row.TeamID)
		}
	}
}

type teamComplexityParityGoldenRow struct {
	TeamID                      string  `json:"team_id"`
	LOCTotal                    int     `json:"loc_total"`
	CyclomaticTotal             int     `json:"cyclomatic_total"`
	CyclomaticPerKLOC           float64 `json:"cyclomatic_per_kloc"`
	HighComplexityFunctions     int     `json:"high_complexity_functions"`
	VeryHighComplexityFunctions int     `json:"very_high_complexity_functions"`
	ContributingRepoCount       int     `json:"contributing_repo_count"`
}

type teamComplexityParityGolden struct {
	TeamComplexity []teamComplexityParityGoldenRow `json:"team_complexity"`
}

func loadTeamComplexityParityGolden(t *testing.T) teamComplexityParityGolden {
	t.Helper()
	data, err := os.ReadFile(filepath.Join("testdata", "team_complexity_parity_golden.json"))
	if err != nil {
		t.Fatal(err)
	}
	var golden teamComplexityParityGolden
	if err := json.Unmarshal(data, &golden); err != nil {
		t.Fatal(err)
	}
	return golden
}
