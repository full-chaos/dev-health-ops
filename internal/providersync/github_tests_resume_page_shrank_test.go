package providersync

import (
	"context"
	"errors"
	"net/http"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
)

// githubTestsShrinkingRunsDoer serves a workflow-runs page whose SIZE the test
// controls, at a stable URL. It models the one thing GitHub's Actions listing
// does between two attempts on a busy repository: runs are served newest-first,
// so new runs push older ones off the page a resume cursor is anchored to, and
// the page the cursor named comes back holding different -- often fewer --
// items than when its index was recorded.
type githubTestsShrinkingRunsDoer struct {
	t     *testing.T
	items int
}

func (doer *githubTestsShrinkingRunsDoer) Do(request *http.Request) (*http.Response, error) {
	doer.t.Helper()
	header := http.Header{"Content-Type": {"application/json"}}
	path := request.URL.Path
	switch {
	case path == "/repos/acme/api":
		return githubTestsHTTPResponse(request, header, gitHubRepositoryFixture), nil
	case path == "/repos/acme/api/actions/runs":
		if doer.items == 0 {
			return githubTestsHTTPResponse(request, header, `{"workflow_runs":[]}`), nil
		}
		return githubTestsHTTPResponse(request, header, githubTestsWorkflowRunsFixture(1, doer.items)), nil
	case strings.HasSuffix(path, "/jobs"):
		return githubTestsHTTPResponse(request, header, `{"jobs":[]}`), nil
	case strings.HasSuffix(path, "/artifacts"):
		return githubTestsHTTPResponse(request, header, `{"artifacts":[]}`), nil
	default:
		doer.t.Fatalf("unexpected request %s", request.URL.String())
		return nil, nil
	}
}

func githubTestsRunsResumeCursor(index int) string {
	return `{"phase":"runs","next_url":"https://api.github.com/repos/acme/api/actions/runs?per_page=100",` +
		`"index":` + strconv.Itoa(index) + `,"run_pages":1,"artifact_pages":0,"repo":"acme/api"}`
}

func githubTestsResumeCollect(
	t *testing.T, doer providerfoundation.HTTPDoer, resume string,
) (emissions int, finals int, err error) {
	t.Helper()
	err = GitHubTestsRouteHandler{}.CollectChunks(
		context.Background(), nativeTestClaim("github", "cicd"),
		providerfoundation.Credential{}, githubTestsClient(t, doer),
		time.Date(2026, 8, 14, 12, 0, 0, 0, time.UTC), resume,
		func(emission ChunkRouteEmission) error {
			emissions++
			if emission.Final {
				finals++
			}
			return nil
		},
	)
	return emissions, finals, err
}

// CONTROL. A resume whose stored index still addresses an item on the page
// must complete. If this fails, the fixture or the cursor shape is wrong and
// the red test below proves nothing.
func TestGitHubTestsResumeWithinPageCompletes(t *testing.T) {
	doer := &githubTestsShrinkingRunsDoer{t: t, items: 5}

	_, finals, err := githubTestsResumeCollect(t, doer, githubTestsRunsResumeCursor(2))
	if err != nil {
		t.Fatalf("resume within the page returned err=%v, want completion", err)
	}
	if finals != 1 {
		t.Fatalf("finals=%d, want exactly 1", finals)
	}
}

// RED ON main (CHAOS-4177 H2).
//
// A resume cursor stores `index`, a position INSIDE the page named by
// `next_url`. On resume the route re-fetches that page and applies the stored
// index to whatever comes back:
//
//	start := 0
//	if cursor.NextURL == page.CursorBefore {
//	    start = cursor.Index
//	}
//	if start > len(page.Items) {
//	    return ErrChunkCheckpointConflict
//	}
//
// github_tests_chunked_route.go:360-366, and the artifacts-phase twin at
// :542-549.
//
// GitHub serves Actions runs newest-first. Between two attempts on a busy
// repository new runs appear, the contents of any given page shift, and the
// page the cursor named comes back shorter than the index recorded against it.
// The route then reports a checkpoint conflict -- but nothing is corrupt: the
// unit's durable state is fine and the provider simply moved. The conflict is
// classified as an ordinary retryable, so it costs ONE OF FIVE ATTEMPTS.
//
// That is why this failure tracks repository busyness rather than anything
// about the unit, and why it arrives in a burst after a gap: the longer since
// the last attempt, the more the pages have moved. Measured on local main, 21
// conflicts landed in a 15-second window when workers resumed units last
// touched 4.5 hours earlier, and zero landed across two restarts seconds
// apart.
//
// The outcome this pins: a page that moved under a resume is a normal provider
// condition, not a corrupt checkpoint. The unit must re-anchor and carry on
// WITHOUT spending an attempt. It deliberately does not prescribe how -- clamp
// the start, re-walk the page, or re-derive the anchor -- because that is the
// fix decision, not the evidence.
func TestGitHubTestsResumeAfterPageShrankDoesNotBurnAnAttempt(t *testing.T) {
	// The page held at least 5 runs when index 4 was recorded; by the time the
	// unit resumes only 2 remain addressable on it.
	doer := &githubTestsShrinkingRunsDoer{t: t, items: 2}

	_, finals, err := githubTestsResumeCollect(t, doer, githubTestsRunsResumeCursor(4))

	if errors.Is(err, ErrChunkCheckpointConflict) {
		t.Fatalf(
			"a page that moved under the resume was reported as a corrupt checkpoint: %v; "+
				"want the unit to re-anchor and continue without spending an attempt",
			err,
		)
	}
	if err != nil {
		t.Fatalf("resume after the page shrank returned err=%v, want completion", err)
	}
	if finals != 1 {
		t.Fatalf("finals=%d, want exactly 1", finals)
	}
}
