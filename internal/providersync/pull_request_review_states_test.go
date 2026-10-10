package providersync

import (
	"encoding/json"
	"sort"
	"strconv"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
)

// The declared review states of a provider (providerfoundation) are what its
// normalizer stores. A consumer takes a provider's rework signal from the
// declaration, so a normalizer that starts to store a new state, or stops,
// must change the declaration in the same change.

func sortedStates(states map[string]struct{}) []string {
	out := make([]string, 0, len(states))
	for state := range states {
		out = append(out, state)
	}
	sort.Strings(out)
	return out
}

// GitLab: every kind of input the normalizer reads, and a "requested changes"
// system note, which GitLab writes when a reviewer asks for changes. The
// normalizer stores no review for that note today, so GitLab has no
// changes-requested state.
func TestGitLabReviewNormalizerStoresTheDeclaredStates(t *testing.T) {
	note := func(id int, system bool, kind, body, username string) json.RawMessage {
		raw, err := json.Marshal(map[string]any{
			"id": id, "system": system, "type": kind, "body": body, "created_at": "2026-08-24T10:00:00Z",
			"author": map[string]any{"username": username},
		})
		if err != nil {
			t.Fatal(err)
		}
		return raw
	}
	notes := []json.RawMessage{
		note(1, true, "", "approved this merge request", "reviewer-a"),
		note(2, true, "", "unapproved this merge request", "reviewer-a"),
		note(3, true, "", "requested changes", "reviewer-b"),
		note(4, false, "DiffNote", "please rename this", "reviewer-b"),
		note(5, false, "DiscussionNote", "a general remark", "reviewer-c"),
		note(6, false, "DiffNote", "my own note", "author"),
	}
	approvals := map[string]any{"approved_by": []any{map[string]any{"user": map[string]any{"username": "reviewer-d", "id": 7}}}}
	at := time.Date(2026, 8, 24, 12, 0, 0, 0, time.UTC)
	rows, _, changesRequested := mapGitLabPullRequestReviews(
		nativeTestClaim("gitlab", "prs"), "00000000-0000-4000-8000-000000000001", 1, approvals, notes, &at, at,
		map[string]any{"username": "author"},
	)
	stored := map[string]struct{}{}
	for _, row := range rows {
		stored[row.State] = struct{}{}
	}
	declared := providerfoundation.PullRequestReviewStates("gitlab")
	sort.Strings(declared)
	got := sortedStates(stored)
	if len(got) != len(declared) {
		t.Fatalf("the GitLab normalizer stored the states %v, the declaration holds %v", got, declared)
	}
	for index := range got {
		if got[index] != declared[index] {
			t.Fatalf("the GitLab normalizer stored the states %v, the declaration holds %v", got, declared)
		}
	}
	if changesRequested != 0 || providerfoundation.EmitsPullRequestReviewState("gitlab", providerfoundation.ReviewStateChangesRequested) {
		t.Errorf("GitLab: %d changes-requested review(s) counted and the declaration says %v: the normalizer maps no such review",
			changesRequested, providerfoundation.EmitsPullRequestReviewState("gitlab", providerfoundation.ReviewStateChangesRequested))
	}
}

// GitHub: the normalizer stores the provider's own state, so every declared
// state is stored as it is, and a changes-requested review is counted on the
// pull request.
func TestGitHubReviewNormalizerStoresTheDeclaredStates(t *testing.T) {
	declared := providerfoundation.PullRequestReviewStates("github")
	if !providerfoundation.EmitsPullRequestReviewState("github", providerfoundation.ReviewStateChangesRequested) {
		t.Fatalf("the declaration of github holds %v, with no %s", declared, providerfoundation.ReviewStateChangesRequested)
	}
	claim := oracleReviewClaim
	at := time.Date(2026, 8, 24, 12, 0, 0, 0, time.UTC)
	submitted := "2026-08-24T10:00:00Z"
	const repoID = "00000000-0000-4000-8000-000000000001"
	var reviews []pullRequestReviewRow
	for index, state := range declared {
		row, err := normalizeGitHubPullRequestReview(claim, repoID, 1, gitHubReviewPayload{
			ID: json.Number(strconv.Itoa(index + 1)), Reviewer: json.RawMessage(`{"login":"reviewer"}`), State: state, SubmittedAt: &submitted,
		}, at, at)
		if err != nil {
			t.Fatalf("normalize a %s review: %v", state, err)
		}
		if row.State != state {
			t.Errorf("a %s review is stored as %q", state, row.State)
		}
		reviews = append(reviews, row)
	}
	pullRequests := []pullRequestRow{{RepoID: repoID, Number: 1, OrgID: claim.OrgID}}
	if err := enrichPullRequestsWithReviews(pullRequests, reviews); err != nil {
		t.Fatal(err)
	}
	if pullRequests[0].ChangesRequestedCount != 1 || pullRequests[0].ReviewsCount != len(declared) {
		t.Errorf("the pull request holds %d changes-requested review(s) of %d, want 1 of %d",
			pullRequests[0].ChangesRequestedCount, pullRequests[0].ReviewsCount, len(declared))
	}
}

func TestAProviderWithNoDeclarationEmitsNoReviewState(t *testing.T) {
	for _, provider := range []string{"", "unknown", "local", "bitbucket", "jira", "linear"} {
		if states := providerfoundation.PullRequestReviewStates(provider); len(states) != 0 ||
			providerfoundation.EmitsPullRequestReviewState(provider, providerfoundation.ReviewStateChangesRequested) {
			t.Errorf("provider %q: declared states %v, want none", provider, states)
		}
	}
	if !providerfoundation.EmitsPullRequestReviewState(" GitHub ", providerfoundation.ReviewStateChangesRequested) {
		t.Errorf("the provider key is not read without regard to case and space")
	}
}
