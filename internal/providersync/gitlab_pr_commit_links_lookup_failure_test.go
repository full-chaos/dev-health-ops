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

func collectGitLabPRsWithCommits(t *testing.T, doer providerfoundation.HTTPDoer) (CompleteRouteBatch, error, Claim) {
	t.Helper()
	claim := nativeTestClaim("gitlab", "prs")
	batch, err := (GitLabPullRequestRouteHandler{}).Collect(
		context.Background(), claim, providerfoundation.Credential{},
		gitLabRepositoryClient(t, fakehttp.Client(doer), "https://gitlab.example"), time.Date(2026, 8, 9, 12, 0, 0, 0, time.UTC),
	)
	return batch, err, claim
}

// gitLabOneMergeRequestWithCommits is gitLabOneMergeRequestResponses followed by the commits listing's answer.
func gitLabOneMergeRequestWithCommits(commits gitLabPullRequestResponse) []gitLabPullRequestResponse {
	return append(gitLabOneMergeRequestResponses(), commits)
}

// A merge request commits listing that FAILED (5xx after the client's retries, or a transport error) must not let the
// unit acknowledge its window: the merge request is written with no commit links and is listed again only when it is
// updated again. WANT the route's error and no watermark (the CHAOS-14578 rule). RED at main 289d2716f6eb: the
// listing failure is read as "no links" and the watermark advances.
func TestGitLabCommitListingFailureHoldsTheWatermark(t *testing.T) {
	for name, doer := range map[string]providerfoundation.HTTPDoer{
		"503": &gitLabPullRequestDoer{t: t, responses: gitLabOneMergeRequestWithCommits(
			gitLabPullRequestResponse{status: http.StatusServiceUnavailable, body: `{"message":"temporarily unavailable"}`})},
		"transport": gitLabCommitListingTransportFailureDoer{inner: &gitLabPullRequestDoer{t: t, responses: gitLabOneMergeRequestResponses()},
			failure: errors.New("synthetic connection reset")},
	} {
		t.Run(name, func(t *testing.T) {
			batch, err, _ := collectGitLabPRsWithCommits(t, doer)
			if err == nil || batch.Watermark != nil {
				t.Fatalf("err = %v, watermark = %v, result = %v: want the commits listing's error and no watermark", err, batch.Watermark, batch.Result)
			}
		})
	}
}

// An answered listing with a malformed item (an answered read: a re-read gets the same body) keeps today's answer:
// the merge request is written without links and the window is acknowledged.
func TestGitLabCommitListingMalformedItemKeepsTodaysAnswer(t *testing.T) {
	batch, err, claim := collectGitLabPRsWithCommits(t, &gitLabPullRequestDoer{t: t, responses: gitLabOneMergeRequestWithCommits(
		gitLabPullRequestResponse{body: `[{"short_id":"abc"}]`})})
	if err != nil || batch.Watermark == nil || !batch.Watermark.Equal(*claim.BeforeAt) || len(batch.Effects) != 4 || len(batch.Effects[3].Rows) != 0 {
		t.Fatalf("err = %v, watermark = %v: want today's answer (no links, the claim's end)", err, batch.Watermark)
	}
}

// gitLabCommitListingAnsweredAtMain is the effect rows of an answered commits listing as ops main 289d2716f6eb writes
// them (recorded with this cell's fixtures).
const gitLabCommitListingAnsweredAtMain = `git_pull_requests=[{"repo_id":"c7198fbc-1945-3717-05d8-eb78866b4e79","number":7,"title":"Add API","body":null,"state":"open","author_name":"author","author_email":null,"created_at":"2026-07-15T10:00:00Z","merged_at":null,"closed_at":null,"head_branch":null,"base_branch":null,"additions":0,"deletions":0,"changed_files":0,"first_review_at":null,"first_comment_at":null,"changes_requested_count":0,"reviews_count":0,"comments_count":0,"last_synced":"2026-08-09T12:00:00Z","source_id":null,"org_id":"org-acme"}]
git_pull_request_reviews=[]
ai_attribution=[]
work_graph_pr_commit=[{"repo_id":"c7198fbc-1945-3717-05d8-eb78866b4e79","pr_number":7,"commit_hash":"0123456789abcdef0123456789abcdef01234567","confidence":1,"provenance":"native","evidence":"api_pr_commits","last_synced":"2026-08-09T12:00:00Z","org_id":"org-acme"}]`

// Control: an answered commits listing acknowledges the window and writes the rows byte-equal to main.
func TestGitLabCommitListingAnsweredAsMain(t *testing.T) {
	batch, err, claim := collectGitLabPRsWithCommits(t, &gitLabPullRequestDoer{t: t, responses: gitLabOneMergeRequestWithCommits(
		gitLabPullRequestResponse{body: `[{"id":"0123456789abcdef0123456789abcdef01234567"}]`})})
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
	if got := strings.Join(effects, "\n"); got != gitLabCommitListingAnsweredAtMain {
		t.Errorf("effects =\n%s\nwant main's\n%s", got, gitLabCommitListingAnsweredAtMain)
	}
}
