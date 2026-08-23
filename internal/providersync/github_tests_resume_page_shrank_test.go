package providersync

import (
	"bytes"
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
	// artifactPhaseURL records the URL the ARTIFACTS phase paginates, so a test
	// can build a resume cursor anchored to the real page instead of guessing
	// how the route composes that query.
	artifactPhaseURL string
}

func (doer *githubTestsShrinkingRunsDoer) Do(request *http.Request) (*http.Response, error) {
	doer.t.Helper()
	header := http.Header{"Content-Type": {"application/json"}}
	path := request.URL.Path
	switch {
	case path == "/repos/acme/api":
		return githubTestsHTTPResponse(request, header, gitHubRepositoryFixture), nil
	case path == "/repos/acme/api/actions/runs":
		if request.URL.Query().Get("branch") != "" {
			doer.artifactPhaseURL = request.URL.String()
		}
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
	t *testing.T, client *providerfoundation.HTTPClient, resume string,
) (emissions int, finals int, err error) {
	t.Helper()
	_, emissions, finals, err = githubTestsResumeCollectCursor(t, client, resume)
	return emissions, finals, err
}

// githubTestsResumeCollectCursor also returns the TERMINAL cursor. Completion
// and a metric line together still do not prove the walk processed anything:
// a route that recorded the re-anchor and then resumed from the stale index
// would satisfy both while skipping the page. The cursor's cumulative item
// counts are what show real work.
func githubTestsResumeCollectCursor(
	t *testing.T, client *providerfoundation.HTTPClient, resume string,
) (cursor githubTestsChunkCursor, emissions int, finals int, err error) {
	t.Helper()
	last := resume
	err = GitHubTestsRouteHandler{}.CollectChunks(
		context.Background(), nativeTestClaim("github", "cicd"),
		providerfoundation.Credential{}, client,
		time.Date(2026, 8, 14, 12, 0, 0, 0, time.UTC), resume,
		func(emission ChunkRouteEmission) error {
			emissions++
			last = emission.CursorAfter
			if emission.Final {
				finals++
			}
			return nil
		},
	)
	if decoded, decodeErr := decodeGitHubTestsChunkCursor(last); decodeErr == nil {
		cursor = decoded
	}
	return cursor, emissions, finals, err
}

// CONTROL. A resume whose stored index still addresses an item on the page
// must complete. If this fails, the fixture or the cursor shape is wrong and
// the red test below proves nothing.
func TestGitHubTestsResumeWithinPageCompletes(t *testing.T) {
	doer := &githubTestsShrinkingRunsDoer{t: t, items: 5}

	_, finals, err := githubTestsResumeCollect(t, githubTestsClient(t, doer), githubTestsRunsResumeCursor(2))
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

	_, finals, err := githubTestsResumeCollect(t, githubTestsClient(t, doer), githubTestsRunsResumeCursor(4))

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

// A re-anchor that nothing counts is invisible: the unit succeeds, the board
// says success, and nobody can tell whether pages are shifting faster than the
// walk consumes them. This is also the deploy-verification signal -- after the
// fix a deploy should show re-anchors where it used to show conflicts -- so it
// is asserted through the ROUTE rather than by calling the recorder.
func TestGitHubTestsResumeReanchorIsCounted(t *testing.T) {
	doer := &githubTestsShrinkingRunsDoer{t: t, items: 2}
	client := githubTestsClient(t, doer)
	// The executors attach Metrics before handing the client to a route
	// (chunked_stream_executor.go:147). githubTestsClient leaves it nil, so
	// attach one or the assertion below would pass against a nil recorder.
	client.Metrics = providerfoundation.NewMetrics()

	if _, _, err := githubTestsResumeCollect(t, client, githubTestsRunsResumeCursor(4)); err != nil {
		t.Fatalf("collect: %v", err)
	}

	var buffer bytes.Buffer
	if err := client.Metrics.WritePrometheus(&buffer); err != nil {
		t.Fatalf("WritePrometheus: %v", err)
	}
	want := `dev_health_provider_resume_reanchor_total{provider="github",dataset="cicd",phase="runs"} 1`
	if !strings.Contains(buffer.String(), want) {
		t.Fatalf("metrics did not carry the re-anchor:\nwant line: %s\ngot:\n%s", want, buffer.String())
	}
}

// A page that did NOT move must not be reported as a re-anchor, or the signal
// becomes noise and the deploy check above means nothing.
func TestGitHubTestsResumeWithinPageIsNotCountedAsReanchor(t *testing.T) {
	doer := &githubTestsShrinkingRunsDoer{t: t, items: 5}
	client := githubTestsClient(t, doer)
	client.Metrics = providerfoundation.NewMetrics()

	if _, _, err := githubTestsResumeCollect(t, client, githubTestsRunsResumeCursor(2)); err != nil {
		t.Fatalf("collect: %v", err)
	}

	var buffer bytes.Buffer
	if err := client.Metrics.WritePrometheus(&buffer); err != nil {
		t.Fatalf("WritePrometheus: %v", err)
	}
	if strings.Contains(buffer.String(), `dev_health_provider_resume_reanchor_total{provider="github",dataset="cicd",phase="runs"} 1`) {
		t.Fatal("a page that did not move was counted as a re-anchor")
	}
}

// The artifacts phase carries the identical positional-index resume and the
// identical failure. It has never fired in production only because no unit has
// survived the runs phase to reach it, so it is fixed and pinned together with
// its twin rather than left as a latent repeat.
//
// The resume cursor is built from the URL the route actually paginated in the
// artifacts phase, captured on a first pass, so the test cannot pass merely
// because a guessed URL failed to match the page.
func TestGitHubTestsArtifactsPhaseResumeAfterPageShrankDoesNotBurnAnAttempt(t *testing.T) {
	discover := &githubTestsShrinkingRunsDoer{t: t, items: 5}
	if _, _, err := githubTestsResumeCollect(t, githubTestsClient(t, discover), ""); err != nil {
		t.Fatalf("discovery pass: %v", err)
	}
	if discover.artifactPhaseURL == "" {
		t.Fatal("discovery pass never reached the artifacts phase; the cursor below would be meaningless")
	}

	resume := `{"phase":"artifacts","next_url":` + strconv.Quote(discover.artifactPhaseURL) +
		`,"index":4,"run_pages":1,"artifact_pages":1,"repo":"acme/api"}`

	doer := &githubTestsShrinkingRunsDoer{t: t, items: 2}
	client := githubTestsClient(t, doer)
	client.Metrics = providerfoundation.NewMetrics()

	_, finals, err := githubTestsResumeCollect(t, client, resume)
	if errors.Is(err, ErrChunkCheckpointConflict) {
		t.Fatalf(
			"artifacts phase reported a moved page as a corrupt checkpoint: %v; "+
				"want re-anchor and continue",
			err,
		)
	}
	if err != nil {
		t.Fatalf("artifacts resume returned err=%v, want completion", err)
	}
	if finals != 1 {
		t.Fatalf("finals=%d, want exactly 1", finals)
	}

	var buffer bytes.Buffer
	if err := client.Metrics.WritePrometheus(&buffer); err != nil {
		t.Fatalf("WritePrometheus: %v", err)
	}
	want := `dev_health_provider_resume_reanchor_total{provider="github",dataset="cicd",phase="artifacts"} 1`
	if !strings.Contains(buffer.String(), want) {
		t.Fatalf("artifacts re-anchor not counted:\nwant line: %s\ngot:\n%s", want, buffer.String())
	}
}

// RED. The re-anchor gate must fire when the stored index addresses NOTHING,
// not only when it points past the end.
//
// A cursor is never persisted with Index == len(page.Items): both item write
// sites in github_tests_chunked_route.go -- the runs loop and its artifacts
// twin -- assign index+1 and then normalise every >= len case to index 0 with
// CursorAfter, and both empty-page branches set index 0. So a persisted
// Index > 0 always addressed an item that existed. If a re-fetched
// page has shrunk to exactly that index, the index now addresses nothing --
// the walk starts at the end, processes zero items, advances to CursorAfter,
// and silently drops whatever that page still held. No error, no metric, no
// incomplete marker.
//
// That is the same class as the defect this branch fixes, and quieter: the
// original at least failed loudly. Detecting "points past the end" alone
// misses it, and on a busy repository pages tend to stay full, so this shape
// is the more common one.
func TestGitHubTestsResumeWhenPageShrankExactlyToIndexReAnchors(t *testing.T) {
	// Index 2 was persisted while the page held at least 3 items. It now holds
	// exactly 2, so index 2 addresses nothing.
	doer := &githubTestsShrinkingRunsDoer{t: t, items: 2}
	client := githubTestsClient(t, doer)
	client.Metrics = providerfoundation.NewMetrics()

	cursor, _, finals, err := githubTestsResumeCollectCursor(t, client, githubTestsRunsResumeCursor(2))
	if err != nil {
		t.Fatalf("err=%v, want the walk to re-anchor and complete", err)
	}
	if finals != 1 {
		t.Fatalf("finals=%d, want exactly 1", finals)
	}
	// The load-bearing assertion: re-anchoring must actually RE-WALK the page.
	// Recording the metric and then resuming from the stale index would still
	// skip both runs, and completion alone cannot tell the two apart.
	if cursor.Pipelines != 2 {
		t.Fatalf(
			"processed %d pipelines after re-anchoring a 2-item page, want 2; "+
				"the walk recorded the re-anchor but did not re-walk the page",
			cursor.Pipelines,
		)
	}

	var buffer bytes.Buffer
	if err := client.Metrics.WritePrometheus(&buffer); err != nil {
		t.Fatalf("WritePrometheus: %v", err)
	}
	want := `dev_health_provider_resume_reanchor_total{provider="github",dataset="cicd",phase="runs"} 1`
	if !strings.Contains(buffer.String(), want) {
		t.Fatalf(
			"a page that shrank exactly to the stored index was walked from that index, "+
				"processing nothing and recording nothing:\nwant line: %s\ngot:\n%s",
			want, buffer.String(),
		)
	}
}

// Index 0 is always a legitimate start, including on a page that came back
// EMPTY. Without an explicit index-0 clause the "addresses nothing" test reads
// 0 >= 0 as true and reports a re-anchor for an ordinary empty page, which
// would make the counter fire on healthy walks. A signal that cries wolf is
// worse than no signal, because this counter is what a deploy is verified with.
func TestGitHubTestsResumeAtIndexZeroOnEmptyPageIsNotAReanchor(t *testing.T) {
	doer := &githubTestsShrinkingRunsDoer{t: t, items: 0}
	client := githubTestsClient(t, doer)
	client.Metrics = providerfoundation.NewMetrics()

	if _, _, err := githubTestsResumeCollect(t, client, githubTestsRunsResumeCursor(0)); err != nil {
		t.Fatalf("err=%v, want an empty page to walk cleanly", err)
	}

	var buffer bytes.Buffer
	if err := client.Metrics.WritePrometheus(&buffer); err != nil {
		t.Fatalf("WritePrometheus: %v", err)
	}
	if strings.Contains(buffer.String(), `dev_health_provider_resume_reanchor_total{provider="github",dataset="cicd",phase="runs"} 1`) {
		t.Fatal("an empty page at index 0 was reported as a re-anchor")
	}
}
