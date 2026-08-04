package providersync

import (
	"context"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
)

// GitHubPullRequestReviewRouteHandler is the production-constructible,
// deliberately-unregistered complete route for github/pr-reviews. Python runs
// PR collection, GraphQL review enrichment, and both writes in one
// _sync_github_prs_to_store_async execution. The Go unit keeps the
// pr-reviews claim for its own ledger/audit identity, but invokes the existing
// prs collector as an implementation detail and emits the one enriched,
// complete git_pull_requests row alongside git_pull_request_reviews.
//
// This is the only safe ownership boundary for a ReplacingMergeTree complete
// row: a separately scheduled review writer cannot patch just three columns,
// and a base PR writer cannot safely write their defaults after enrichment.
// The route remains unregistered until pr-comments joins this same unit and
// the matrix can switch the complete three-way Python boundary atomically.
type GitHubPullRequestReviewRouteHandler struct {
	PullRequests GitHubPullRequestRouteHandler
	Reviews      GitHubPullRequestReviewFetcher
}

func (handler GitHubPullRequestReviewRouteHandler) Collect(
	ctx context.Context,
	claim Claim,
	credential providerfoundation.Credential,
	client *providerfoundation.HTTPClient,
	normalizedAt time.Time,
) (CompleteRouteBatch, error) {
	if ctx == nil || claim.Validate() != nil || claim.Provider != "github" ||
		claim.Dataset != "pr-reviews" || normalizedAt.IsZero() {
		return CompleteRouteBatch{}, ErrInvalidConfiguration
	}

	// The REST collector is intentionally reused rather than re-listing PRs:
	// selecting a second independently-watermarked set would let review rows
	// describe PRs that the enclosing complete row effect does not contain.
	baseClaim := claim
	baseClaim.Dataset = "prs"
	base, err := handler.PullRequests.Collect(
		ctx, baseClaim, credential, client, normalizedAt,
	)
	if err != nil {
		return CompleteRouteBatch{}, err
	}
	if len(base.Effects) != 1 || base.Effects[0].Destination != "git_pull_requests" {
		return CompleteRouteBatch{}, ErrInvalidConfiguration
	}
	rows, err := decodeEffectRows[pullRequestRow](base.Effects[0])
	if err != nil {
		return CompleteRouteBatch{}, err
	}
	targets := make([]GitHubPullRequestReviewTarget, 0, len(rows))
	for _, row := range rows {
		if err := row.validate(claim); err != nil {
			return CompleteRouteBatch{}, err
		}
		targets = append(targets, GitHubPullRequestReviewTarget{
			Number: row.Number, CreatedAt: row.CreatedAt,
		})
	}

	repoID := ""
	if len(rows) > 0 {
		repoID = rows[0].RepoID
	}
	reviews, err := handler.Reviews.Fetch(
		ctx, claim, client, repoID, targets, normalizedAt,
	)
	if err != nil {
		return CompleteRouteBatch{}, err
	}
	// Non-rate review failures intentionally degrade to the original PR rows,
	// exactly as Python does. The typed marker makes an absent enrichment
	// observable without misreporting it as an empty review collection.
	if reviews.Complete() {
		if err := enrichPullRequestsWithReviews(rows, reviews.Rows); err != nil {
			return CompleteRouteBatch{}, err
		}
	}
	prEffect, err := effectBatchFromValues(
		"git_pull_requests", EffectReadbackRequired, rows,
	)
	if err != nil {
		return CompleteRouteBatch{}, err
	}
	reviewEffect, err := effectBatchFromValues(
		"git_pull_request_reviews", EffectReadbackRequired, reviews.Rows,
	)
	if err != nil {
		return CompleteRouteBatch{}, err
	}
	result := map[string]any{
		"prs_synced":        len(rows),
		"pr_reviews_synced": len(reviews.Rows),
		"repo":              base.Result["repo"],
	}
	if reviews.Incomplete != nil {
		result["pr_reviews_incomplete"] = reviews.Incomplete.Cause
	}
	return CompleteRouteBatch{
		Effects: []EffectBatch{prEffect, reviewEffect}, Result: result,
		Watermark: base.Watermark,
		Evidence: FetchEvidence{
			Provider: claim.Provider, Dataset: claim.Dataset,
			Requests:   base.Evidence.Requests + reviews.Evidence.Requests,
			Pages:      base.Evidence.Pages + reviews.Evidence.Pages,
			Records:    len(rows) + len(reviews.Rows),
			CapReached: base.Evidence.CapReached,
		},
	}, nil
}

// enrichPullRequestsWithReviews is the in-place equivalent of Python's
// _enrich_prs_with_reviews_batch. It only changes a PR when at least one
// review exists, preserving the base collector's zero values for a genuine
// no-review result and for the documented optional-enrichment degradation.
func enrichPullRequestsWithReviews(
	rows []pullRequestRow,
	reviews []pullRequestReviewRow,
) error {
	byNumber := make(map[int][]pullRequestReviewRow, len(rows))
	for _, review := range reviews {
		if review.Number < 1 || review.RepoID == "" || review.SubmittedAt.IsZero() {
			return providerfoundation.ErrNormalizationInvalid
		}
		byNumber[review.Number] = append(byNumber[review.Number], review)
	}
	for index := range rows {
		row := &rows[index]
		prReviews := byNumber[row.Number]
		if len(prReviews) == 0 {
			continue
		}
		var firstReviewAt *time.Time
		changesRequestedCount := 0
		for _, review := range prReviews {
			if review.RepoID != row.RepoID || review.OrgID != row.OrgID {
				return providerfoundation.ErrInvalidScope
			}
			at := review.SubmittedAt.UTC()
			if firstReviewAt == nil || at.Before(*firstReviewAt) {
				firstReviewAt = &at
			}
			if review.State == "CHANGES_REQUESTED" {
				changesRequestedCount++
			}
		}
		row.FirstReviewAt = firstReviewAt
		row.ReviewsCount = len(prReviews)
		row.ChangesRequestedCount = changesRequestedCount
	}
	return nil
}

var _ CompleteRouteHandler = GitHubPullRequestReviewRouteHandler{}
