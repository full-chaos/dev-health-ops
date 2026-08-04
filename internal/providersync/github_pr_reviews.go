package providersync

import (
	"encoding/json"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
)

// pullRequestReviewRow mirrors the complete row written by
// ClickHouseStore.insert_git_pull_request_reviews. It is deliberately kept
// separate from pullRequestRow: D16 preserves Python's three-way unit boundary
// even though the rows share a repository and pull-request identity.
type pullRequestReviewRow struct {
	RepoID      string    `json:"repo_id"`
	Number      int       `json:"number"`
	ReviewID    string    `json:"review_id"`
	Reviewer    string    `json:"reviewer"`
	State       string    `json:"state"`
	SubmittedAt time.Time `json:"submitted_at"`
	LastSynced  time.Time `json:"last_synced"`
	SourceID    *string   `json:"source_id"`
	OrgID       string    `json:"org_id"`
}

type gitHubReviewPayload struct {
	ID          any             `json:"id"`
	Reviewer    json.RawMessage `json:"author"`
	State       any             `json:"state"`
	SubmittedAt *string         `json:"submitted_at"`
}

func normalizeGitHubPullRequestReview(
	claim Claim,
	repoID string,
	number int,
	review gitHubReviewPayload,
	createdAt time.Time,
	normalizedAt time.Time,
) (pullRequestReviewRow, error) {
	reviewID := stringValue(review.ID)
	reviewer := gitHubPullUserLogin(review.Reviewer)
	if reviewer == "" {
		reviewer = "Unknown"
	}
	state := stringValue(review.State)
	submittedAt := parseGitHubPullTime(review.SubmittedAt)
	if submittedAt == nil {
		fallback := createdAt.UTC()
		submittedAt = &fallback
	}
	row := pullRequestReviewRow{
		RepoID: repoID, Number: number, ReviewID: reviewID,
		Reviewer: reviewer, State: state, SubmittedAt: submittedAt.UTC(),
		LastSynced: normalizedAt.UTC().Truncate(time.Millisecond), OrgID: claim.OrgID,
	}
	if err := row.validate(claim); err != nil {
		return pullRequestReviewRow{}, err
	}
	return row, nil
}

func (row pullRequestReviewRow) validate(claim Claim) error {
	if claim.Provider != "github" || claim.Dataset != "pr-reviews" ||
		row.OrgID == "" || row.OrgID != claim.OrgID || len(row.RepoID) != 36 ||
		row.Number < 1 || row.ReviewID == "" || row.Reviewer == "" ||
		row.State == "" || row.SubmittedAt.IsZero() || row.LastSynced.IsZero() ||
		row.SourceID != nil {
		return providerfoundation.ErrInvalidScope
	}
	return nil
}
