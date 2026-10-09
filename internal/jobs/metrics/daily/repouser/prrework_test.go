package repouser

import (
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/jobs/metrics/prrework"
)

// ApplyPRRework counts the same merged pull requests Compute counts (merged
// inside the day's window), by review evidence, and leaves the ported ratio
// as Compute made it.
func TestApplyPRReworkCountsTheDaysMergedPullRequestsByReviewEvidence(t *testing.T) {
	day := time.Date(2026, 8, 24, 0, 0, 0, 0, time.UTC)
	computedAt := day.Add(30 * time.Hour)
	github, gitlab, quiet := uuid.New(), uuid.New(), uuid.New()
	at := func(offset time.Duration) *time.Time {
		value := day.Add(offset)
		return &value
	}
	pullRequest := func(repo uuid.UUID, number int, mergedAt *time.Time, reviews, changesRequested int) PullRequestRow {
		return PullRequestRow{
			RepoID: repo, Number: number, AuthorEmail: "dev@example.com", CreatedAt: day.Add(time.Hour), MergedAt: mergedAt,
			ReviewsCount: reviews, ChangesRequestedCount: changesRequested, Additions: 5,
		}
	}
	rows := []PullRequestRow{
		pullRequest(github, 1, at(2*time.Hour), 0, 0),
		pullRequest(github, 2, at(3*time.Hour), 2, 0),
		pullRequest(github, 3, at(4*time.Hour), 1, 1),
		// Merged the day after and the day before: not of this day.
		pullRequest(github, 4, at(25*time.Hour), 3, 3),
		pullRequest(github, 5, at(-time.Hour), 3, 3),
		// Opened on the day, not merged.
		pullRequest(github, 6, nil, 3, 3),
		pullRequest(gitlab, 1, at(2*time.Hour), 2, 0),
		pullRequest(gitlab, 2, at(3*time.Hour), 2, 0),
		pullRequest(quiet, 1, nil, 0, 0),
	}
	result := Compute(day, nil, rows, nil, computedAt, DefaultNormalizeIdentity, 1000, nil, nil, nil, nil, nil)
	for _, row := range result.RepoMetrics {
		if row.PRRework != nil || row.PRReworkRatioReviewed != nil {
			t.Fatalf("Compute set the rework counts of %s: they are the step's, and nil means not counted", row.RepoID)
		}
	}
	before := map[uuid.UUID]float64{}
	for _, row := range result.RepoMetrics {
		before[row.RepoID] = row.PRReworkRatio
	}
	ApplyPRRework(&result, day, rows, map[uuid.UUID]string{github: "github", gitlab: "gitlab", quiet: "github"})

	want := map[uuid.UUID]prrework.Counts{
		github: {Merged: 3, Reviewed: 2, Rework: 1},
		gitlab: {Merged: 2, NoSignal: 2},
		quiet:  {},
	}
	if len(result.RepoMetrics) != len(want) {
		t.Fatalf("%d repository rows, want %d", len(result.RepoMetrics), len(want))
	}
	for _, row := range result.RepoMetrics {
		if row.PRRework == nil || *row.PRRework != want[row.RepoID] {
			t.Errorf("repository %s: counts %+v, want %+v", row.RepoID, row.PRRework, want[row.RepoID])
			continue
		}
		if int(row.PRRework.Merged) != row.PRsMerged {
			t.Errorf("repository %s: the step counts %d merged pull requests, Compute %d: one day, one count", row.RepoID, row.PRRework.Merged, row.PRsMerged)
		}
		if row.PRReworkRatio != before[row.RepoID] {
			t.Errorf("repository %s: the step changed the ported ratio from %v to %v", row.RepoID, before[row.RepoID], row.PRReworkRatio)
		}
	}
	for _, row := range result.RepoMetrics {
		switch row.RepoID {
		case github:
			// 1 of 2 reviewed; the ported ratio is 1 of 3 merged.
			if row.PRReworkRatioReviewed == nil || *row.PRReworkRatioReviewed != 0.5 || row.PRReworkRatio != 1.0/3.0 {
				t.Errorf("github: one-day ratio %v, ported ratio %v, want 0.5 and 1/3", row.PRReworkRatioReviewed, row.PRReworkRatio)
			}
		default:
			if row.PRReworkRatioReviewed != nil {
				t.Errorf("repository %s has no reviewed pull request and a one-day ratio of %v, want none", row.RepoID, *row.PRReworkRatioReviewed)
			}
		}
	}

	// A repository with no known provider has no signal.
	unknown := Compute(day, nil, rows[:3], nil, computedAt, DefaultNormalizeIdentity, 1000, nil, nil, nil, nil, nil)
	ApplyPRRework(&unknown, day, rows[:3], nil)
	if got := unknown.RepoMetrics[0].PRRework; got == nil || *got != (prrework.Counts{Merged: 3, NoSignal: 3}) {
		t.Errorf("no known provider: counts %+v, want 3 merged with no signal", got)
	}
	ApplyPRRework(nil, day, rows, nil) // a nil result is left alone
}
