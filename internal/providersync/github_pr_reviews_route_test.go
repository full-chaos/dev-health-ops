package providersync

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
)

type gitHubPullRequestReviewRouteDoer struct {
	t             *testing.T
	restBodies    map[string]string
	graphQLStatus int
	graphQLReply  string
	paths         []string
}

func (doer *gitHubPullRequestReviewRouteDoer) Do(request *http.Request) (*http.Response, error) {
	doer.t.Helper()
	doer.paths = append(doer.paths, request.URL.Path)
	if request.URL.Path == "/graphql" {
		_, _ = io.ReadAll(request.Body)
		status := doer.graphQLStatus
		if status == 0 {
			status = http.StatusOK
		}
		return gitHubPullRequestReviewResponse(request, status, doer.graphQLReply), nil
	}
	if body, ok := doer.restBodies[request.URL.Path]; ok {
		return gitHubPullRequestReviewResponse(request, http.StatusOK, body), nil
	}
	return gitHubPullRequestReviewResponse(request, http.StatusNotFound, `{}`), nil
}

func TestGitHubPullRequestReviewRouteComposesOneCompletePRRow(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 8, 4, 12, 0, 0, 123456000, time.UTC)
	doer := &gitHubPullRequestReviewRouteDoer{
		t: t, restBodies: defaultGitHubPullRequestFixtures(),
		graphQLReply: `{"data":{"repository":{"pr0":{"number":42,"reviews":{"nodes":[` +
			`{"databaseId":9007199254740993,"state":"APPROVED","submittedAt":"2026-07-11T10:30:00Z","author":{"login":"octocat"}},` +
			`{"id":"R_changes","state":"CHANGES_REQUESTED","submittedAt":"2026-07-10T11:00:00Z","author":{"login":"hubot"}}` +
			`],"pageInfo":{"hasNextPage":false,"endCursor":""}}}}}}`,
	}
	claim := nativeTestClaim("github", "pr-reviews")
	batch, err := (GitHubPullRequestReviewRouteHandler{}).Collect(
		context.Background(), claim, providerfoundation.Credential{},
		gitHubPullRequestClient(t, doer, "https://api.github.com"), now,
	)
	if err != nil {
		t.Fatal(err)
	}
	if batch.Watermark == nil || !batch.Watermark.Equal(*claim.BeforeAt) ||
		batch.Evidence.Provider != "github" || batch.Evidence.Dataset != "pr-reviews" ||
		batch.Evidence.Requests != 4 || batch.Evidence.Pages != 2 || batch.Evidence.Records != 3 {
		t.Fatalf("batch evidence=%+v watermark=%v", batch.Evidence, batch.Watermark)
	}
	if len(batch.Effects) != 2 || batch.Effects[0].Destination != "git_pull_requests" ||
		batch.Effects[1].Destination != "git_pull_request_reviews" ||
		batch.Effects[0].Recovery != EffectReadbackRequired || batch.Effects[1].Recovery != EffectReadbackRequired {
		t.Fatalf("effects=%+v", batch.Effects)
	}
	var pull pullRequestRow
	if err := json.Unmarshal(batch.Effects[0].Rows[0], &pull); err != nil {
		t.Fatal(err)
	}
	first := time.Date(2026, 7, 10, 11, 0, 0, 0, time.UTC)
	if pull.FirstReviewAt == nil || !pull.FirstReviewAt.Equal(first) ||
		pull.ReviewsCount != 2 || pull.ChangesRequestedCount != 1 ||
		pull.CommentsCount != 3 || pull.FirstCommentAt != nil {
		t.Fatalf("enriched pull=%+v", pull)
	}
	reviews, err := decodeEffectRows[pullRequestReviewRow](batch.Effects[1])
	if err != nil || len(reviews) != 2 {
		t.Fatalf("reviews=%+v err=%v", reviews, err)
	}
	if batch.Result["prs_synced"] != 1 || batch.Result["pr_reviews_synced"] != 2 ||
		batch.Result["pr_reviews_incomplete"] != nil {
		t.Fatalf("result=%+v", batch.Result)
	}
}

// TestGitHubPullRequestSocialRoutePreservesAliasIdentityAndParity pins the
// Python execution boundary: prs, pr-reviews, and pr-comments all invoke
// _sync_github_prs_to_store_async, so they must produce the same two durable
// effects while retaining their own claim/ledger/watermark identity. In
// particular, pr-comments is not a third raw-table fetch: comments_count is
// already supplied by the PR detail response.
func TestGitHubPullRequestSocialRoutePreservesAliasIdentityAndParity(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 8, 4, 12, 0, 0, 123456000, time.UTC)
	graphql := `{"data":{"repository":{"pr0":{"number":42,"reviews":{"nodes":[{"id":"review-1","state":"APPROVED","submittedAt":"2026-07-11T10:30:00Z","author":{"login":"octocat"}}],"pageInfo":{"hasNextPage":false,"endCursor":""}}}}}}`
	var baseline CompleteRouteBatch
	for _, dataset := range []string{"prs", "pr-reviews", "pr-comments"} {
		t.Run(dataset, func(t *testing.T) {
			doer := &gitHubPullRequestReviewRouteDoer{
				t: t, restBodies: defaultGitHubPullRequestFixtures(), graphQLReply: graphql,
			}
			claim := nativeTestClaim("github", dataset)
			batch, err := (GitHubPullRequestSocialRouteHandler{}).Collect(
				context.Background(), claim, providerfoundation.Credential{},
				gitHubPullRequestClient(t, doer, "https://api.github.com"), now,
			)
			if err != nil {
				t.Fatal(err)
			}
			if batch.Evidence.Dataset != dataset || batch.Watermark == nil ||
				!batch.Watermark.Equal(*claim.BeforeAt) {
				t.Fatalf("claim=%+v batch=%+v", claim, batch)
			}
			ledger, err := NewEffectLedgerState(claim, batch.Effects, now)
			if err != nil || ledger.Dataset != dataset || ledger.Generation != claim.GenerationKey() {
				t.Fatalf("claim=%+v ledger=%+v error=%v", claim, ledger, err)
			}
			if baseline.Effects == nil {
				baseline = batch
				return
			}
			for index := range batch.Effects {
				if batch.Effects[index].Destination != baseline.Effects[index].Destination ||
					batch.Effects[index].ContentDigest != baseline.Effects[index].ContentDigest {
					t.Fatalf("effect[%d]=%+v baseline=%+v", index, batch.Effects[index], baseline.Effects[index])
				}
			}
		})
	}
}

func TestGitHubPullRequestSocialEffectsAcceptEveryAliasClaim(t *testing.T) {
	t.Parallel()
	sink := GitHubPullRequestSocialClickHouseEffects{
		Lease: providerfoundation.LeaseGuardFunc(func(context.Context) error { return nil }),
	}
	pullEffect, err := effectBatchFromValues("git_pull_requests", EffectReadbackRequired, []pullRequestRow(nil))
	if err != nil {
		t.Fatal(err)
	}
	reviewEffect, err := effectBatchFromValues("git_pull_request_reviews", EffectReadbackRequired, []pullRequestReviewRow(nil))
	if err != nil {
		t.Fatal(err)
	}
	row := pullRequestReadbackFixture(time.Date(2026, 8, 4, 12, 0, 0, 0, time.UTC))
	nonEmptyReviewEffect, err := effectBatchFromValues(
		"git_pull_request_reviews", EffectReadbackRequired, []pullRequestReviewRow{{
			OrgID: "org-acme", RepoID: row.RepoID, Number: row.Number,
			ReviewID: "review-1", Reviewer: "octocat", State: "APPROVED",
			SubmittedAt: row.CreatedAt, LastSynced: row.LastSynced,
		}},
	)
	if err != nil {
		t.Fatal(err)
	}
	for _, dataset := range []string{"prs", "pr-reviews", "pr-comments"} {
		t.Run(dataset, func(t *testing.T) {
			claim := nativeTestClaim("github", dataset)
			if err := sink.WriteEffect(context.Background(), claim, pullEffect); err != nil {
				t.Fatalf("pull effect: %v", err)
			}
			if err := sink.WriteEffect(context.Background(), claim, reviewEffect); err != nil {
				t.Fatalf("review effect: %v", err)
			}
			// A nil Conn is the final expected failure: reaching it proves the
			// non-empty review row passed alias-aware scope validation first.
			if err := sink.WriteEffect(context.Background(), claim, nonEmptyReviewEffect); err != ErrInvalidConfiguration {
				t.Fatalf("non-empty review effect error=%v want ErrInvalidConfiguration", err)
			}
		})
	}
}

func TestGitHubPullRequestReviewRoutePreservesBaseRowOnOptionalReviewFailure(t *testing.T) {
	t.Parallel()
	doer := &gitHubPullRequestReviewRouteDoer{
		t: t, restBodies: defaultGitHubPullRequestFixtures(), graphQLStatus: http.StatusServiceUnavailable,
		graphQLReply: `{"message":"temporarily unavailable"}`,
	}
	claim := nativeTestClaim("github", "pr-reviews")
	batch, err := (GitHubPullRequestReviewRouteHandler{}).Collect(
		context.Background(), claim, providerfoundation.Credential{},
		gitHubPullRequestClient(t, doer, "https://api.github.com"), time.Date(2026, 8, 4, 12, 0, 0, 0, time.UTC),
	)
	if err != nil {
		t.Fatal(err)
	}
	var pull pullRequestRow
	if err := json.Unmarshal(batch.Effects[0].Rows[0], &pull); err != nil {
		t.Fatal(err)
	}
	if pull.FirstReviewAt != nil || pull.ReviewsCount != 0 || pull.ChangesRequestedCount != 0 ||
		len(batch.Effects[1].Rows) != 0 || batch.Result["pr_reviews_incomplete"] != "transient" {
		t.Fatalf("batch=%+v pull=%+v", batch, pull)
	}
}

func TestEnrichPullRequestsWithReviewsRejectsForeignReview(t *testing.T) {
	t.Parallel()
	row := pullRequestReadbackFixture(time.Date(2026, 8, 4, 12, 0, 0, 0, time.UTC))
	row.OrgID = "org-acme"
	err := enrichPullRequestsWithReviews([]pullRequestRow{row}, []pullRequestReviewRow{{
		OrgID: "other-org", RepoID: row.RepoID, Number: row.Number,
		ReviewID: "review-1", Reviewer: "octocat", State: "APPROVED",
		SubmittedAt: row.CreatedAt, LastSynced: row.LastSynced,
	}})
	if err != providerfoundation.ErrInvalidScope {
		t.Fatalf("error=%v want ErrInvalidScope", err)
	}
}

func TestEnrichPullRequestsWithReviewsLeavesNoReviewRowsUntouched(t *testing.T) {
	t.Parallel()
	row := pullRequestReadbackFixture(time.Date(2026, 8, 4, 12, 0, 0, 0, time.UTC))
	row.FirstReviewAt = nil
	row.ReviewsCount, row.ChangesRequestedCount = 0, 0
	rows := []pullRequestRow{row}
	if err := enrichPullRequestsWithReviews(rows, nil); err != nil {
		t.Fatal(err)
	}
	if rows[0].FirstReviewAt != nil || rows[0].ReviewsCount != 0 || rows[0].ChangesRequestedCount != 0 {
		t.Fatalf("row=%+v", rows[0])
	}
}

func TestGitHubPullRequestSocialRouteIsNotRegisteredUntilTheRegistryLayerLands(t *testing.T) {
	t.Parallel()
	for _, dataset := range []string{"prs", "pr-reviews", "pr-comments"} {
		descriptor, ok := (CompleteRouteSwitches{}).Descriptor("github", dataset)
		if !ok || descriptor.RouteReady {
			t.Fatalf("dataset=%s descriptor=%+v ok=%v", dataset, descriptor, ok)
		}
	}
}
