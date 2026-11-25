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

// gitLabApprovalsTransportFailureDoer fails the merge request approvals read at the transport (no answer).
type gitLabApprovalsTransportFailureDoer struct {
	inner *gitLabPullRequestDoer
}

func (doer gitLabApprovalsTransportFailureDoer) Do(request *http.Request) (*http.Response, error) {
	if strings.HasSuffix(request.URL.Path, "/approvals") {
		return nil, errors.New("synthetic connection reset")
	}
	return doer.inner.Do(request)
}

// gitLabOneMergeRequestWithApprovals is one in-window merge request whose approvals read gets approvals; its notes and
// commits listings answer empty.
func gitLabOneMergeRequestWithApprovals(approvals gitLabPullRequestResponse) []gitLabPullRequestResponse {
	responses := gitLabOneMergeRequestResponses()
	responses[2] = approvals
	return append(responses, gitLabPullRequestResponse{body: `[]`})
}

func collectGitLabPRsForApprovals(t *testing.T, doer providerfoundation.HTTPDoer) (CompleteRouteBatch, error, Claim) {
	t.Helper()
	claim := nativeTestClaim("gitlab", "pr-reviews")
	batch, err := (GitLabPullRequestRouteHandler{}).Collect(
		context.Background(), claim, providerfoundation.Credential{},
		gitLabRepositoryClient(t, fakehttp.Client(doer), "https://gitlab.example"), time.Date(2026, 8, 9, 12, 0, 0, 0, time.UTC),
	)
	return batch, err, claim
}

// A merge request approvals read that FAILED (5xx after the client's retries, or a transport error) must not let the
// unit acknowledge its window: the approvals that have no approval note are dropped, and the merge request is listed
// again only when it is updated again. WANT the route's error and no watermark (the CHAOS-14578 rule).
func TestGitLabApprovalsLookupFailureHoldsTheWatermark(t *testing.T) {
	transportResponses := gitLabOneMergeRequestWithApprovals(gitLabPullRequestResponse{})
	transportResponses = append(transportResponses[:2], transportResponses[3:]...)
	for name, doer := range map[string]providerfoundation.HTTPDoer{
		"503": &gitLabPullRequestDoer{t: t, responses: gitLabOneMergeRequestWithApprovals(
			gitLabPullRequestResponse{status: http.StatusServiceUnavailable, body: `{"message":"temporarily unavailable"}`})},
		"transport": gitLabApprovalsTransportFailureDoer{inner: &gitLabPullRequestDoer{t: t, responses: transportResponses}},
	} {
		t.Run(name, func(t *testing.T) {
			batch, err, _ := collectGitLabPRsForApprovals(t, doer)
			if err == nil || batch.Watermark != nil {
				t.Fatalf("err = %v, watermark = %v, result = %v: want the approvals read's error and no watermark", err, batch.Watermark, batch.Result)
			}
		})
	}
}

// An answered approvals read keeps today's answer: a 404 (approvals unavailable) and a 200 whose body is not an
// approvals object both write the merge request without approval rows and acknowledge the window.
func TestGitLabApprovalsAnsweredReadKeepsTodaysAnswer(t *testing.T) {
	for name, approvals := range map[string]gitLabPullRequestResponse{
		"404":         {status: http.StatusNotFound, body: `{"message":"404 Not found"}`},
		"unparseable": {body: `[]`},
	} {
		t.Run(name, func(t *testing.T) {
			batch, err, claim := collectGitLabPRsForApprovals(t, &gitLabPullRequestDoer{t: t, responses: gitLabOneMergeRequestWithApprovals(approvals)})
			if err != nil || batch.Watermark == nil || !batch.Watermark.Equal(*claim.BeforeAt) || len(batch.Effects) != 4 || len(batch.Effects[1].Rows) != 0 {
				t.Fatalf("err = %v, watermark = %v, result = %v: want today's answer (no review rows, the claim's end)", err, batch.Watermark, batch.Result)
			}
		})
	}
}
