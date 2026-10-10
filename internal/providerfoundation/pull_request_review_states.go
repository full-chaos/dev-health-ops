package providerfoundation

import "strings"

// ReviewStateChangesRequested is the state of a review that asks for changes.
// It is the rework signal of a pull request: a metric that counts rework
// (package prrework) can measure it only for a provider whose normalizer can
// store a review of this state.
const ReviewStateChangesRequested = "CHANGES_REQUESTED"

// pullRequestReviewStates declares, for each provider, the review states its
// normalizer can store on a pull request review row
// (git_pull_request_reviews.state). It is the one declaration of what a
// provider can say about a review; a consumer asks it and never tests a
// provider name.
//
//   - github: the normalizer stores the state of the provider's review as it
//     is (internal/providersync/github_pr_reviews.go).
//   - gitlab: GitLab has no review resource. The normalizer rebuilds reviews
//     from approvals, the "approved" and "unapproved" system notes and diff
//     notes (internal/providersync/gitlab_pr_reviews.go), so it stores no
//     review that asks for changes. When it maps GitLab's "requested changes"
//     review, the state is added here and GitLab has the signal.
//
// A test of the normalizers holds each list against what the normalizer
// stores. A provider that is not listed has no declared state.
var pullRequestReviewStates = map[string][]string{
	"github": {"APPROVED", ReviewStateChangesRequested, "COMMENTED", "DISMISSED", "PENDING"},
	"gitlab": {"APPROVED", "COMMENTED", "DISMISSED"},
}

// PullRequestReviewStates returns the review states the provider's normalizer
// can store, or nil for a provider with no declaration.
func PullRequestReviewStates(provider string) []string {
	return append([]string(nil), pullRequestReviewStates[normalizedProviderKey(provider)]...)
}

// EmitsPullRequestReviewState says that the provider's normalizer can store a
// review of the given state. A provider with no declaration emits none.
func EmitsPullRequestReviewState(provider, state string) bool {
	for _, declared := range pullRequestReviewStates[normalizedProviderKey(provider)] {
		if declared == state {
			return true
		}
	}
	return false
}

func normalizedProviderKey(provider string) string {
	return strings.ToLower(strings.TrimSpace(provider))
}
