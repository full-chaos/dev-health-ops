//go:build integration

package remaining

import (
	"context"
	"math"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/jobs/metrics/prrework/prreworktest"
)

// The recommendations job reads a team's pull request rework ratio as the
// ratio of the window's summed counts over REVIEWED pull requests of the
// team's repositories, and has no value (not known) when the window holds no
// reviewed pull request. Rows computed and written by the daily job's own
// compute and writer on the migrated schema:
//
//	repo N  day 1: 3 merged, no review data
//	repo R  day 1: 4 merged: 2 reviewed (1 with changes requested), 2 with no review data
//	               (an older version of the day says 4 reviewed, 4 with changes requested)
//	        day 2: 8 merged: 3 reviewed (no changes requested), 5 with no review data
//	repo G (gitlab)  day 1: 3 merged with reviews: the provider has no changes-requested event
//	other organization, repo R's id, day 1: 10 reviewed, 10 with changes requested
func TestRecommendationsReworkRatioReadsReviewedPullRequestsOnly(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	// The store helper stops its container in a cleanup with this context:
	// cancel after it, not before.
	t.Cleanup(cancel)
	conn := membershipMigratedClickHouse(t, ctx)
	const org = "org-rework-recommendations"
	repoN, repoR, repoG := uuid.New(), uuid.New(), uuid.New()
	day1 := time.Date(2026, 8, 3, 0, 0, 0, 0, time.UTC)
	day2 := day1.AddDate(0, 0, 1)
	older := day1.Add(80 * time.Hour)
	newer := older.Add(time.Hour)
	// Two versions of a day are stored below. A background merge would keep
	// only the newest one and hide a reader that does not select it.
	if err := conn.Exec(ctx, "SYSTEM STOP MERGES repo_metrics_daily"); err != nil {
		t.Fatal(err)
	}
	unreviewed := prreworktest.PullRequest{}
	reviewed := prreworktest.PullRequest{Reviews: 2}
	rework := prreworktest.PullRequest{Reviews: 3, ChangesRequested: 1}
	repeat := func(pullRequest prreworktest.PullRequest, count int) []prreworktest.PullRequest {
		out := make([]prreworktest.PullRequest, count)
		for i := range out {
			out[i] = pullRequest
		}
		return out
	}
	prreworktest.WriteDay(ctx, t, conn, org, repoR, "github", day1, older, repeat(rework, 4))
	prreworktest.WriteDay(ctx, t, conn, org, repoN, "github", day1, newer, repeat(unreviewed, 3))
	prreworktest.WriteDay(ctx, t, conn, org, repoR, "github", day1, newer, append(append(repeat(reviewed, 1), repeat(rework, 1)...), repeat(unreviewed, 2)...))
	prreworktest.WriteDay(ctx, t, conn, org, repoR, "github", day2, newer, append(repeat(reviewed, 3), repeat(unreviewed, 5)...))
	prreworktest.WriteDay(ctx, t, conn, org, repoG, "gitlab", day1, newer, repeat(reviewed, 3))
	prreworktest.WriteDay(ctx, t, conn, "org-rework-recommendations-other", repoR, "github", day1, newer, repeat(rework, 10))

	loader, err := NewRecommendationsLoader(conn, org)
	if err != nil {
		t.Fatal(err)
	}
	windowStart, windowEnd := day1.AddDate(0, 0, -2), day2.AddDate(0, 0, 1)
	for _, testCase := range []struct {
		name  string
		repos []uuid.UUID
		want  float64
		known bool
	}{
		{"a team whose repository has no review data: not known, never a known 0", []uuid.UUID{repoN}, 0, false},
		{"a team whose repository is of a provider with no changes-requested event: not known", []uuid.UUID{repoG}, 0, false},
		// 1 of 5 reviewed pull requests of the two days.
		{"a team with reviewed pull requests: the ratio of the sums", []uuid.UUID{repoR}, 0.2, true},
		{"a team with both kinds of repository: the repository with no review data adds nothing", []uuid.UUID{repoN, repoR, repoG}, 0.2, true},
		{"a team that owns no repository: not known", nil, 0, false},
	} {
		got, known, err := loader.loadReworkRatio(ctx, "team-1", testCase.repos, windowStart, windowEnd)
		if err != nil {
			t.Fatalf("%s: %v", testCase.name, err)
		}
		if known != testCase.known || math.Abs(got-testCase.want) > 1e-12 {
			t.Errorf("%s: rework ratio %v (known %v), want %v (known %v)", testCase.name, got, known, testCase.want, testCase.known)
		}
	}
}
