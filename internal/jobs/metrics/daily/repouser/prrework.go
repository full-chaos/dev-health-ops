package repouser

import (
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/jobs/metrics/prrework"
)

// ApplyPRRework adds the day's rework counts to a Compute result: on each
// repository row, the merged pull requests of the day counted by review
// evidence (prrework.CountMerged) and the day's ratio over the reviewed ones.
//
// It is a step after Compute, not a part of it: Compute is the ported
// function and keeps PRReworkRatio as the reference computes it (rework / ALL
// merged pull requests, 0 with none). The counts take the same pull requests
// the same way: merged inside the day's window.
//
// hasReworkSignal says, for each repository, whether its provider can store a
// changes-requested review (the caller takes it from the provider layer's
// declaration). A repository that is not in it has no known provider and
// therefore no rework signal: its merged pull requests count as "no signal",
// never as reviewed.
//
// A repository row with no merged pull request gets zero counts: the day was
// counted and nothing merged.
func ApplyPRRework(result *Result, day time.Time, pullRequests []PullRequestRow, hasReworkSignal map[uuid.UUID]bool) {
	if result == nil {
		return
	}
	start, end := utcDayWindow(day)
	merged := map[uuid.UUID][]prrework.PullRequest{}
	for _, pullRequest := range pullRequests {
		if pullRequest.MergedAt == nil {
			continue
		}
		mergedAt := pullRequest.MergedAt.UTC()
		if mergedAt.Before(start) || !mergedAt.Before(end) {
			continue
		}
		merged[pullRequest.RepoID] = append(merged[pullRequest.RepoID], prrework.PullRequest{
			ReviewsCount: pullRequest.ReviewsCount, ChangesRequestedCount: pullRequest.ChangesRequestedCount,
		})
	}
	for i := range result.RepoMetrics {
		repoID := result.RepoMetrics[i].RepoID
		counts := prrework.CountMerged(merged[repoID], hasReworkSignal[repoID])
		result.RepoMetrics[i].PRRework = &counts
		// The one-day column holds the value only: NULL for every state that
		// is not measured. A reader that needs the state reads the counts.
		result.RepoMetrics[i].PRReworkRatioReviewed = prrework.Rate(counts).Value
	}
}
