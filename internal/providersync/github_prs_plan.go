package providersync

// The github/prs request plan assumes a range for every input its Collect
// does not bound. Each constant states one range; a run past a range
// under-reserves (see TestGitHubPRsOutsideStatedDomain*).
const (
	// githubPRsAssumedNewerThanWindow is the number of PRs updated AFTER the
	// window that precede the in-window PRs in the updated-descending list
	// and so consume list pages before the first in-window PR is reached.
	githubPRsAssumedNewerThanWindow = nativePerPage

	// githubPRsAssumedReviewsPerPR is the reviews one PR carries; every
	// gitHubPullRequestReviewPageSize reviews past the first page cost one
	// more GraphQL request.
	githubPRsAssumedReviewsPerPR = 2 * gitHubPullRequestReviewPageSize
)

// githubPRsAssumedInWindow is the number of PRs updated inside the window;
// each costs one detail request. Comments per PR need no assumption: the
// comment count arrives on the detail response, with no further request.
func githubPRsAssumedInWindow(spanDays int) int {
	return max(2, 2*spanDays)
}

// githubPRsRESTUnits is the REST_CORE reservation: the repository fetch,
// the list pages covering the newer-than-window PRs, the in-window PRs and
// the one older PR that ends the listing, and one detail fetch per
// in-window PR.
func githubPRsRESTUnits(spanDays int) int {
	inWindow := githubPRsAssumedInWindow(spanDays)
	listPages := max(1, (githubPRsAssumedNewerThanWindow+inWindow+1+nativePerPage-1)/nativePerPage)
	return 1 + listPages + inWindow
}
