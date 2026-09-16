package daily

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/google/uuid"
)

// TestTeamCognitiveLoadMatchesTheFrozenPythonGolden is the parity proof for
// buildTeamCognitiveLoadRows against build_team_cognitive_load_rows_for_day:
// the Python compute this function ports is deleted, so this is a
// FROZEN-golden test, not a live dual-execution one. The golden file
// (testdata/team_cognitive_load_parity_golden.json) was captured ONCE, via a
// throwaway, never-committed script run against the still-live Python
// function before its deletion; the script itself was never committed.
//
// Both build_team_cognitive_load_rows_for_day (Python) and
// buildTeamCognitiveLoadRows (Go) are PURE functions over already-loaded
// rows -- the ClickHouse reads and write around them carry no team-keyed
// aggregation logic of their own to prove parity on, so this test calls the
// aggregator directly with in-memory fixtures.
//
// The corpus exercises every branch the aggregator's own doc comment
// describes:
//   - team-a: TWO owned repos, one author active in both -- proves the SUM
//     of load counters across owned repos, the ratios RECOMPUTED from the
//     summed after-hours/weekend counts rather than averaged per-repo, the
//     distinct-author count (not double-counting the shared author), and
//     the distinct (author, repo) pair count backing context_spread_count.
//   - team-b: one owned repo whose contributing wellbeing row sums to zero
//     commits -- a MEASURED 0.0 ratio, not nil.
//   - team-c: one owned repo with only a user-metrics contribution and no
//     wellbeing row at all -- an UNMEASURED nil ratio, distinct from team-b's
//     measured zero.
//   - an orphan repo, present in the input rows but absent from repoToTeam,
//     carrying deliberately distinctive values so any accidental fallback
//     bucket would be unmistakable in a failure diff -- must contribute to
//     no team row at all.
func TestTeamCognitiveLoadMatchesTheFrozenPythonGolden(t *testing.T) {
	golden := loadTeamCognitiveLoadParityGolden(t)

	day := time.Date(2026, 9, 4, 0, 0, 0, 0, time.UTC)
	computedAt := time.Date(2026, 9, 5, 10, 0, 0, 0, time.UTC)
	const orgID = "00000000-0000-4000-8000-0000000ec101"

	repoAlpha := uuid.MustParse("11111111-1111-4111-8111-111111111111")
	repoBeta := uuid.MustParse("22222222-2222-4222-8222-222222222222")
	repoGamma := uuid.MustParse("33333333-3333-4333-8333-333333333333")
	repoOrphan := uuid.MustParse("44444444-4444-4444-8444-444444444444")
	repoEpsilon := uuid.MustParse("55555555-5555-4555-8555-555555555555")

	userRows := []userMetricsCognitiveLoadInput{
		{RepoID: repoAlpha, AuthorEmail: "alice@example.com", PRInterruptionLoad: 3, ReviewRequestLoad: 1},
		{RepoID: repoAlpha, AuthorEmail: "bob@example.com", PRInterruptionLoad: 2, ReviewRequestLoad: 0},
		{RepoID: repoBeta, AuthorEmail: "alice@example.com", PRInterruptionLoad: 4, ReviewRequestLoad: 2},
		// Orphan: deliberately NOT in repoToTeam below.
		{RepoID: repoOrphan, AuthorEmail: "eve@example.com", PRInterruptionLoad: 999, ReviewRequestLoad: 999},
		{RepoID: repoEpsilon, AuthorEmail: "carol@example.com", PRInterruptionLoad: 5, ReviewRequestLoad: 0},
	}
	teamRows := []teamMetricsCognitiveLoadInput{
		{RepoID: repoAlpha, AfterHoursCommitsCount: 3, WeekendCommitsCount: 2, CommitsCount: 20},
		{RepoID: repoBeta, AfterHoursCommitsCount: 1, WeekendCommitsCount: 1, CommitsCount: 10},
		{RepoID: repoGamma, AfterHoursCommitsCount: 0, WeekendCommitsCount: 0, CommitsCount: 0},
		{RepoID: repoOrphan, AfterHoursCommitsCount: 50, WeekendCommitsCount: 50, CommitsCount: 100},
	}
	repoToTeam := map[string]string{
		repoAlpha.String():   "team-a",
		repoBeta.String():    "team-a",
		repoGamma.String():   "team-b",
		repoEpsilon.String(): "team-c",
		// repoOrphan deliberately absent.
	}

	got := buildTeamCognitiveLoadRows(orgID, day, userRows, teamRows, repoToTeam, computedAt)

	if len(got) != len(golden.TeamCognitiveLoad) {
		t.Fatalf("row count: golden=%d got=%d (got=%+v)", len(golden.TeamCognitiveLoad), len(got), got)
	}
	byTeam := make(map[string]teamCognitiveLoadRow, len(got))
	for _, row := range got {
		byTeam[row.TeamID] = row
	}
	for _, want := range golden.TeamCognitiveLoad {
		row, ok := byTeam[want.TeamID]
		if !ok {
			t.Fatalf("%s: missing from Go's output entirely (got=%+v)", want.TeamID, got)
		}
		if row.PRInterruptionLoad != want.PRInterruptionLoad {
			t.Errorf("%s: PRInterruptionLoad = %v, want %v", want.TeamID, row.PRInterruptionLoad, want.PRInterruptionLoad)
		}
		if row.ContextSpreadCount != want.ContextSpreadCount {
			t.Errorf("%s: ContextSpreadCount = %v, want %v", want.TeamID, row.ContextSpreadCount, want.ContextSpreadCount)
		}
		if row.ReviewRequestLoad != want.ReviewRequestLoad {
			t.Errorf("%s: ReviewRequestLoad = %v, want %v", want.TeamID, row.ReviewRequestLoad, want.ReviewRequestLoad)
		}
		if !floatPtrEqual(row.AfterHoursCommitRatio, want.AfterHoursCommitRatio) {
			t.Errorf("%s: AfterHoursCommitRatio = %s, want %s",
				want.TeamID, floatPtrString(row.AfterHoursCommitRatio), floatPtrString(want.AfterHoursCommitRatio))
		}
		if !floatPtrEqual(row.WeekendCommitRatio, want.WeekendCommitRatio) {
			t.Errorf("%s: WeekendCommitRatio = %s, want %s",
				want.TeamID, floatPtrString(row.WeekendCommitRatio), floatPtrString(want.WeekendCommitRatio))
		}
		if row.ContributingRepoCount != want.ContributingRepoCount {
			t.Errorf("%s: ContributingRepoCount = %d, want %d", want.TeamID, row.ContributingRepoCount, want.ContributingRepoCount)
		}
		if row.SampleAuthorCount != want.SampleAuthorCount {
			t.Errorf("%s: SampleAuthorCount = %d, want %d", want.TeamID, row.SampleAuthorCount, want.SampleAuthorCount)
		}
	}
	// The orphan repo's distinctive 999/50 values must not appear on ANY
	// row -- the class of bug this corpus exists to catch (an accidental
	// "unassigned" bucket instead of the documented "never guessed" skip).
	for _, row := range got {
		if row.PRInterruptionLoad == 999 || row.ReviewRequestLoad == 999 || row.ContributingRepoCount > 2 {
			t.Errorf("%s: carries the orphan repo's values -- an unowned repo must contribute to no team", row.TeamID)
		}
	}
}

func floatPtrEqual(a, b *float64) bool {
	if a == nil || b == nil {
		return a == nil && b == nil
	}
	return *a == *b
}

func floatPtrString(v *float64) string {
	if v == nil {
		return "<nil>"
	}
	return fmt.Sprintf("%v", *v)
}

type teamCognitiveLoadParityGoldenRow struct {
	TeamID                string   `json:"team_id"`
	PRInterruptionLoad    float64  `json:"pr_interruption_load"`
	ContextSpreadCount    float64  `json:"context_spread_count"`
	ReviewRequestLoad     float64  `json:"review_request_load"`
	AfterHoursCommitRatio *float64 `json:"after_hours_commit_ratio"`
	WeekendCommitRatio    *float64 `json:"weekend_commit_ratio"`
	ContributingRepoCount int      `json:"contributing_repo_count"`
	SampleAuthorCount     int      `json:"sample_author_count"`
}

type teamCognitiveLoadParityGolden struct {
	TeamCognitiveLoad []teamCognitiveLoadParityGoldenRow `json:"team_cognitive_load"`
}

func loadTeamCognitiveLoadParityGolden(t *testing.T) teamCognitiveLoadParityGolden {
	t.Helper()
	data, err := os.ReadFile(filepath.Join("testdata", "team_cognitive_load_parity_golden.json"))
	if err != nil {
		t.Fatal(err)
	}
	var golden teamCognitiveLoadParityGolden
	if err := json.Unmarshal(data, &golden); err != nil {
		t.Fatal(err)
	}
	return golden
}
