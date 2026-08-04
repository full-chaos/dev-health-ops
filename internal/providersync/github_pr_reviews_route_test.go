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

func TestGitHubPullRequestReviewRouteIsNotRegisteredUntilTheThreeWayUnitLands(t *testing.T) {
	t.Parallel()
	descriptor, ok := (CompleteRouteSwitches{}).Descriptor("github", "pr-reviews")
	if !ok || descriptor.RouteReady || len(descriptor.Destinations) != 0 {
		t.Fatalf("descriptor=%+v ok=%v", descriptor, ok)
	}
}
