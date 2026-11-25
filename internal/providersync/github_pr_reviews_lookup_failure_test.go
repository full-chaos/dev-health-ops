package providersync

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/fakehttp"
)

// prReviewsTransportDoer fails the GraphQL reviews read at the transport (no answer); REST goes to inner.
type prReviewsTransportDoer struct {
	inner *gitHubPullRequestReviewRouteDoer
}

func (doer prReviewsTransportDoer) Do(request *http.Request) (*http.Response, error) {
	if request.URL.Path == "/graphql" {
		return nil, errors.New("synthetic connection reset")
	}
	return doer.inner.Do(request)
}

const prReviewsAnsweredReply = `{"data":{"repository":{"pr0":{"number":42,"reviews":{"nodes":[` +
	`{"databaseId":7001,"state":"APPROVED","submittedAt":"2026-07-11T10:30:00Z","author":{"login":"octocat"}}` +
	`],"pageInfo":{"hasNextPage":false,"endCursor":""}}}}}}`

func collectPRReviews(t *testing.T, doer providerfoundation.HTTPDoer) (CompleteRouteBatch, error, Claim) {
	t.Helper()
	claim := nativeTestClaim("github", "pr-reviews")
	batch, err := (GitHubPullRequestReviewRouteHandler{}).Collect(
		context.Background(), claim, providerfoundation.Credential{},
		gitHubPullRequestClient(t, fakehttp.Client(doer), "https://api.github.com"), time.Date(2026, 8, 4, 12, 0, 0, 0, time.UTC),
	)
	return batch, err, claim
}

// A reviews read that FAILED (5xx after the client's retries, or a transport error) must not let the unit acknowledge
// its window: the PRs it was for are not listed again unless they are updated again, so their reviews would never be
// read. WANT the route's error and no watermark (classifyLookupFailure: lookupFails). RED at main 289d2716f6eb: the
// failure becomes Incomplete, the rows are flagged reviews_lookup_failed and the watermark advances.
func TestGitHubPullRequestReviewsLookupFailureHoldsTheWatermark(t *testing.T) {
	for name, doer := range map[string]providerfoundation.HTTPDoer{
		"503": &gitHubPullRequestReviewRouteDoer{t: t, restBodies: defaultGitHubPullRequestFixtures(),
			graphQLStatus: http.StatusServiceUnavailable, graphQLReply: `{"message":"temporarily unavailable"}`},
		"transport": prReviewsTransportDoer{inner: &gitHubPullRequestReviewRouteDoer{t: t, restBodies: defaultGitHubPullRequestFixtures()}},
	} {
		t.Run(name, func(t *testing.T) {
			batch, err, _ := collectPRReviews(t, doer)
			if err == nil || batch.Watermark != nil {
				t.Fatalf("err = %v, watermark = %v, result = %v: want the reviews read's error and no watermark", err, batch.Watermark, batch.Result)
			}
		})
	}
}

// An answered but unparseable reviews read (HTTP 200, a body that is not the GraphQL shape) keeps today's answer: the
// rows are flagged, the result names invalid_response, and the window is acknowledged. The provider answered, so a
// re-read would get the same body; this is not the failed-lookup class.
func TestGitHubPullRequestReviewsInvalidResponseKeepsTodaysAnswer(t *testing.T) {
	batch, err, claim := collectPRReviews(t, &gitHubPullRequestReviewRouteDoer{t: t, restBodies: defaultGitHubPullRequestFixtures(),
		graphQLReply: `{"data":{"repository":null}}`})
	if err != nil || batch.Watermark == nil || !batch.Watermark.Equal(*claim.BeforeAt) || batch.Result["pr_reviews_incomplete"] != "invalid_response" {
		t.Fatalf("err = %v, watermark = %v, result = %v: want today's invalid_response answer and the claim's end", err, batch.Watermark, batch.Result)
	}
}

// prReviewsAnsweredAtMain is the effect rows of an answered reviews read as ops main 289d2716f6eb writes them (recorded
// with this cell's fixtures).
const prReviewsAnsweredAtMain = `git_pull_requests=[{"repo_id":"c7198fbc-1945-3717-05d8-eb78866b4e79","number":42,"title":"Add widget support","body":"This PR adds widget support.","state":"merged","author_name":"octocat","author_email":null,"created_at":"2026-07-10T09:00:00Z","merged_at":"2026-07-21T15:30:00Z","closed_at":"2026-07-21T15:30:00Z","head_branch":"feature/widgets","base_branch":"main","additions":120,"deletions":30,"changed_files":5,"first_review_at":"2026-07-11T10:30:00Z","first_comment_at":null,"changes_requested_count":0,"reviews_count":1,"comments_count":3,"last_synced":"2026-08-04T12:00:00Z","source_id":null,"org_id":"org-acme"}]
git_pull_request_reviews=[{"repo_id":"c7198fbc-1945-3717-05d8-eb78866b4e79","number":42,"review_id":"7001","reviewer":"octocat","state":"APPROVED","submitted_at":"2026-07-11T10:30:00Z","last_synced":"2026-08-04T12:00:00Z","source_id":null,"org_id":"org-acme"}]
ai_attribution=[]`

// Control: an answered reviews read acknowledges the window and writes the rows byte-equal to main.
func TestGitHubPullRequestReviewsAnsweredReadAsMain(t *testing.T) {
	batch, err, claim := collectPRReviews(t, &gitHubPullRequestReviewRouteDoer{t: t, restBodies: defaultGitHubPullRequestFixtures(),
		graphQLReply: prReviewsAnsweredReply})
	if err != nil || batch.Watermark == nil || !batch.Watermark.Equal(*claim.BeforeAt) {
		t.Fatalf("err = %v, watermark = %v", err, batch.Watermark)
	}
	var effects []string
	for _, effect := range batch.Effects {
		var rows []string
		for _, row := range effect.Rows {
			rows = append(rows, string(row))
		}
		effects = append(effects, effect.Destination+"="+"["+strings.Join(rows, ",")+"]")
	}
	if got := strings.Join(effects, "\n"); got != prReviewsAnsweredAtMain {
		t.Errorf("effects =\n%s\nwant main's\n%s", got, prReviewsAnsweredAtMain)
	}
}
