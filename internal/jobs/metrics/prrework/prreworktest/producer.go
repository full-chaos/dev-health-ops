// Package prreworktest writes repo_metrics_daily rows for tests of the pull
// request rework ratio through the production path: the daily compute
// (repouser.Compute), the rework count step (repouser.ApplyPRRework) and the
// production writer. A reader test that seeds its rows with this package reads
// what the daily job stores, not a row shape a test author typed.
//
// To run a reader test on a tree that has no rework counts, this one file is
// replaced by a WriteDay that leaves the ApplyPRRework call out.
package prreworktest

import (
	"context"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/jobs/metrics/daily/repouser"
	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
)

// PullRequest is one pull request of a repository and day.
type PullRequest struct {
	// Reviews is the number of reviews the sync stored on the pull request.
	Reviews int
	// ChangesRequested is the number of changes-requested reviews.
	ChangesRequested int
	// Open is a pull request that was created on the day and not merged.
	Open bool
}

// WriteDay computes one repository's day from its pull requests and stores
// the repo_metrics_daily row (and the user rows of the same compute) as the
// daily job does. provider is the provider of the repository as the repos
// table names it; its rework signal is the provider layer's declaration, as
// in the daily job.
func WriteDay(
	ctx context.Context, t testing.TB, conn driver.Conn, organizationID string, repoID uuid.UUID, provider string,
	day, computedAt time.Time, pullRequests []PullRequest,
) {
	t.Helper()
	writer, err := repouser.NewWriter(conn)
	if err != nil {
		t.Fatalf("prreworktest: %v", err)
	}
	dayStart := time.Date(day.Year(), day.Month(), day.Day(), 0, 0, 0, 0, time.UTC)
	rows := make([]repouser.PullRequestRow, 0, len(pullRequests))
	for index, pullRequest := range pullRequests {
		row := repouser.PullRequestRow{
			RepoID: repoID, Number: index + 1, AuthorEmail: "dev@example.com", AuthorName: "Dev",
			CreatedAt: dayStart.Add(time.Hour), ReviewsCount: pullRequest.Reviews,
			ChangesRequestedCount: pullRequest.ChangesRequested, Additions: 10, Deletions: 2, ChangedFiles: 1,
		}
		if !pullRequest.Open {
			mergedAt := dayStart.Add(10 * time.Hour)
			row.MergedAt = &mergedAt
		}
		rows = append(rows, row)
	}
	result := repouser.Compute(dayStart, nil, rows, nil, computedAt, repouser.DefaultNormalizeIdentity, 1000, nil, nil, nil, nil, nil)
	repouser.ApplyPRRework(&result, dayStart, rows, map[uuid.UUID]bool{
		repoID: providerfoundation.EmitsPullRequestReviewState(provider, providerfoundation.ReviewStateChangesRequested),
	})
	if len(result.RepoMetrics) != 1 {
		t.Fatalf("prreworktest: the compute gave %d repository row(s) for one repository", len(result.RepoMetrics))
	}
	if _, _, _, err := writer.WriteResult(ctx, result, organizationID); err != nil {
		t.Fatalf("prreworktest: write the day: %v", err)
	}
}
