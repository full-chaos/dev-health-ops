package daily

import (
	"math"
	"testing"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/teamownership"
)

// CHAOS-9410, D5988: the main owner and every lesser owner of a repository
// take its additive measures at their own linked share; the distinct counts
// stay whole; the ratios come from the share-weighted sums.
func TestTeamCognitiveLoadTakesTheMainOwnerAndEveryLesserOwnerAtItsShare(t *testing.T) {
	repoA, repoB := uuid.New(), uuid.New()
	owners := map[string][]teamownership.OwnerShare{
		repoA.String(): {
			{TeamID: "main", Main: true, Share: 0.6},
			{TeamID: "lesser-1", Share: 0.3},
			{TeamID: "lesser-2", Share: 0.1},
		},
		repoB.String(): {{TeamID: "lesser-1", Main: true, Share: 1}},
	}
	userRows := []userMetricsCognitiveLoadInput{
		{RepoID: repoA, AuthorEmail: "a@example.com", PRInterruptionLoad: 10, ReviewRequestLoad: 20},
		{RepoID: repoB, AuthorEmail: "b@example.com", PRInterruptionLoad: 4, ReviewRequestLoad: 8},
	}
	teamRows := []teamMetricsCognitiveLoadInput{
		{RepoID: repoA, AfterHoursCommitsCount: 10, WeekendCommitsCount: 0, CommitsCount: 100},
		{RepoID: repoB, AfterHoursCommitsCount: 10, WeekendCommitsCount: 10, CommitsCount: 10},
	}

	got := map[string]teamCognitiveLoadRow{}
	for _, row := range buildTeamCognitiveLoadRows("acme", cognitiveLoadTestDay, userRows, teamRows, owners, cognitiveLoadTestComputedAt) {
		got[row.TeamID] = row
	}
	if len(got) != 3 {
		t.Fatalf("rows for %d teams, want main, lesser-1 and lesser-2: %+v", len(got), got)
	}
	near := func(a, b float64) bool { return math.Abs(a-b) < 1e-9 }
	main, l1, l2 := got["main"], got["lesser-1"], got["lesser-2"]
	if !near(main.PRInterruptionLoad, 6) || !near(main.ReviewRequestLoad, 12) || !near(main.ContextSpreadCount, 0.6) {
		t.Errorf("main owner %+v, want 6 / 12 / 0.6 (60%% of repo A)", main)
	}
	if !near(l2.PRInterruptionLoad, 1) || !near(l2.ReviewRequestLoad, 2) {
		t.Errorf("lesser-2 %+v, want 1 / 2 (10%% of repo A)", l2)
	}
	if !near(l1.PRInterruptionLoad, 3+4) || !near(l1.ReviewRequestLoad, 6+8) || !near(l1.ContextSpreadCount, 0.3+1) {
		t.Errorf("lesser-1 %+v, want 7 / 14 / 1.3 (30%% of repo A and all of repo B)", l1)
	}
	// 3 of 33 after-hours commits (30% of repo A's 10, 100) plus all of repo B.
	if l1.AfterHoursCommitRatio == nil || !near(*l1.AfterHoursCommitRatio, (3+10)/(30+10.0)) {
		t.Errorf("lesser-1 after-hours ratio %v, want 13/40", l1.AfterHoursCommitRatio)
	}
	if l1.ContributingRepoCount != 2 || l1.SampleAuthorCount != 2 || l2.ContributingRepoCount != 1 {
		t.Errorf("distinct counts stay whole: lesser-1 %d repos %d authors, lesser-2 %d repos", l1.ContributingRepoCount, l1.SampleAuthorCount, l2.ContributingRepoCount)
	}
	for teamID, row := range got {
		if row.TeamID != teamID {
			t.Errorf("row of %s names team %s", teamID, row.TeamID)
		}
	}
}
