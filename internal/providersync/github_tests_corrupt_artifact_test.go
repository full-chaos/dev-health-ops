package providersync

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
)

// githubTestsCorruptArtifactDoer serves ONE workflow run whose artifacts are
// individually either a real zip or a 200 response carrying a body that is not
// a zip at all. The non-zip body is what GitHub's artifact redirect actually
// yields when the blob host answers the unauthenticated follow-up with an error
// document instead of the archive: downloadGitHubTestsArtifact only converts
// 404/410 into a skip and >=400 into a ProviderError, so a 2xx error document
// reaches the parser as ordinary bytes.
type githubTestsCorruptArtifactDoer struct {
	t         *testing.T
	artifacts int
	// corrupt holds the 1-based artifact ids that answer with a non-zip body.
	corrupt map[int]bool
	// oversizedReports serves an archive that OPENS cleanly and then holds more
	// reports than githubTestsMaxReportsPerRun allows. That is a BLOCKING
	// recordSkipped, a different disposition from an unreadable container, and
	// it must still fail the batch closed.
	oversizedReports bool
	archiveRequests  int
}

const githubTestsBlobErrorDocument = `{"message":"Not Found","documentation_url":"https://docs.github.com/rest"}`

func (doer *githubTestsCorruptArtifactDoer) Do(request *http.Request) (*http.Response, error) {
	doer.t.Helper()
	header := http.Header{"Content-Type": {"application/json"}}
	path := request.URL.Path
	switch {
	case path == "/repos/acme/api":
		return githubTestsHTTPResponse(request, header, gitHubRepositoryFixture), nil
	case path == "/repos/acme/api/actions/runs":
		return githubTestsHTTPResponse(request, header, githubTestsWorkflowRunsFixture(1, 1)), nil
	case strings.HasSuffix(path, "/jobs"):
		return githubTestsHTTPResponse(request, header, `{"jobs":[]}`), nil
	case strings.HasSuffix(path, "/artifacts"):
		return githubTestsHTTPResponse(request, header, githubTestsArtifactsFixture(doer.artifacts)), nil
	case strings.HasPrefix(path, "/repos/acme/api/actions/artifacts/") && strings.HasSuffix(path, "/zip"):
		doer.archiveRequests++
		id := githubTestsArtifactIDFromPath(doer.t, path)
		if doer.corrupt[id] {
			// A 200 with a non-zip body. Not an HTTP error, so every status
			// guard in downloadGitHubTestsArtifact passes it through.
			return &http.Response{
				StatusCode: http.StatusOK,
				Header:     http.Header{"Content-Type": {"application/json"}},
				Body:       io.NopCloser(strings.NewReader(githubTestsBlobErrorDocument)),
				Request:    request,
			}, nil
		}
		members := map[string]string{"junit.xml": githubTestsMultiSuiteJUnit(1)}
		if doer.oversizedReports {
			members = make(map[string]string, githubTestsMaxReportsPerRun+1)
			for i := 0; i <= githubTestsMaxReportsPerRun; i++ {
				members["report-"+strconv.Itoa(i)+".xml"] = githubTestsMultiSuiteJUnit(1)
			}
		}
		archive := githubTestsZip(doer.t, members)
		return &http.Response{
			StatusCode: http.StatusOK,
			Header:     http.Header{"Content-Type": {"application/zip"}},
			Body:       io.NopCloser(bytes.NewReader(archive)),
			Request:    request,
		}, nil
	default:
		doer.t.Fatalf("unexpected request %s", request.URL.String())
		return nil, nil
	}
}

func githubTestsArtifactIDFromPath(t *testing.T, path string) int {
	t.Helper()
	segments := strings.Split(strings.Trim(path, "/"), "/")
	if len(segments) < 2 {
		t.Fatalf("cannot read artifact id from %q", path)
	}
	id, err := strconv.Atoi(segments[len(segments)-2])
	if err != nil {
		t.Fatalf("cannot read artifact id from %q: %v", path, err)
	}
	return id
}

// walkGitHubTestsChunksResult drives the chunked route to completion and
// RETURNS the route error instead of failing the test on it. walkGitHubTestsChunks
// calls t.Fatalf on any error that is not its continuation sentinel, which
// would report this defect as "want a continuation yield" and hide what
// actually happened. The subject here is precisely which error the route
// returns, so it has to be observable.
func walkGitHubTestsChunksResult(
	t *testing.T,
	client *providerfoundation.HTTPClient,
	maxChunks int,
) (githubTestsWalk, error) {
	t.Helper()
	claim := nativeTestClaim("github", "cicd")
	normalizedAt := time.Date(2026, 8, 14, 12, 0, 0, 0, time.UTC)
	walk := githubTestsWalk{}
	resume := ""
	for {
		walk.passes++
		if walk.passes > 500 {
			t.Fatal("continuation walk never reached a final emission")
		}
		emitted := 0
		last := resume
		finalSeen := false
		err := GitHubTestsRouteHandler{}.CollectChunks(
			context.Background(), claim, providerfoundation.Credential{}, client, normalizedAt, resume,
			func(emission ChunkRouteEmission) error {
				last = emission.CursorAfter
				if emission.Final {
					walk.final = emission.Batch
					finalSeen = true
					return nil
				}
				walk.chunks++
				emitted++
				if emitted >= maxChunks {
					return errGitHubTestsWalkContinuation
				}
				return nil
			},
		)
		if finalSeen {
			cursor, decodeErr := decodeGitHubTestsChunkCursor(last)
			if decodeErr != nil {
				t.Fatalf("decode terminal cursor: %v", decodeErr)
			}
			walk.cursor = cursor
			return walk, err
		}
		if err != nil && !isGitHubTestsWalkContinuation(err) {
			// The route gave up before any final emission. That is the outcome
			// under test; hand it back rather than deciding here.
			return walk, err
		}
		if err == nil {
			t.Fatal("pass returned no error and no final emission")
		}
		resume = last
	}
}

func isGitHubTestsWalkContinuation(err error) bool {
	return err == errGitHubTestsWalkContinuation
}

// CONTROL. Two healthy artifacts on the same fixture must walk to a final
// emission with both reports committed. If this fails, the fixture or the
// harness is wrong and the corrupt-artifact test below proves nothing.
func TestGitHubTestsHealthyArtifactsCompleteTheUnit(t *testing.T) {
	doer := &githubTestsCorruptArtifactDoer{t: t, artifacts: 2, corrupt: map[int]bool{}}

	walk, err := walkGitHubTestsChunksResult(t, githubTestsClient(t, doer), 4)
	if err != nil {
		t.Fatalf("healthy artifacts returned err=%v, want the unit to complete", err)
	}
	if doer.archiveRequests != 2 {
		t.Fatalf("downloaded %d archives, want 2; the fixture never exercised both artifacts", doer.archiveRequests)
	}
	if walk.cursor.Phase != "done" {
		t.Fatalf("terminal phase=%q, want done", walk.cursor.Phase)
	}
	if walk.cursor.Suites != 2 {
		t.Fatalf("committed %d suites, want 2 (one per healthy artifact)", walk.cursor.Suites)
	}
	if complete, ok := walk.final.Result["reports_complete"].(bool); !ok || !complete {
		t.Fatalf("reports_complete=%v, want true when nothing was lost", walk.final.Result["reports_complete"])
	}
}

// RED ON main (CHAOS-4177 / H1). One artifact whose body is not a zip must not
// sink the whole unit.
//
// Today parseGitHubTestsArtifact returns ErrGitHubTestsReportInvalid when
// zip.NewReader rejects the body (github_tests_reports.go:364-367) and the
// chunked route turns that into ErrGitHubTestsIncomplete and RETURNS
// (github_tests_chunked_route.go:611-614), abandoning the run, the artifacts
// after it, and the whole unit. Prod shows this as
// "github tests inventory incomplete: artifact parse failed: github tests
// report invalid: zip: not a valid zip file", retrying and then failing.
//
// The asymmetry is the defect: every comparable failure INSIDE the archive is
// already recorded and skipped -- recordSkipped("unreadable") at
// github_tests_reports.go:401, recordSkipped("malformed") at :414,
// "archive_bounds" at :389 -- and an expired or empty artifact is skipped
// outright a few lines above the parse call. Only the container itself failing
// to open is fatal, and one unreadable artifact is not a reason to lose the
// healthy ones.
//
// This test asserts the OUTCOME, not a mechanism: the unit finalizes, the
// healthy artifact's rows are committed, and coverage honestly reports that
// something was lost. It deliberately does not name a new cause constant --
// picking the closed-vocabulary label is part of the fix, not part of the
// evidence that the current behaviour is wrong.
func TestGitHubTestsCorruptArtifactDoesNotSinkTheUnit(t *testing.T) {
	doer := &githubTestsCorruptArtifactDoer{t: t, artifacts: 2, corrupt: map[int]bool{1: true}}

	client := githubTestsClient(t, doer)
	walk, err := walkGitHubTestsChunksResult(t, client, 4)
	if err != nil {
		t.Fatalf(
			"one unreadable artifact sank the unit: err=%v; want the artifact skipped and the unit finalized",
			err,
		)
	}
	// Anti-vacuity: the corrupt artifact must actually have been fetched, and
	// the route must have gone on to fetch the healthy one after it. Without
	// this, a route that stopped before any download would satisfy the
	// assertions above.
	if doer.archiveRequests != 2 {
		t.Fatalf(
			"downloaded %d archives, want 2; the route never reached the healthy artifact after the corrupt one",
			doer.archiveRequests,
		)
	}
	if walk.cursor.Phase != "done" {
		t.Fatalf("terminal phase=%q, want done", walk.cursor.Phase)
	}
	// The healthy artifact's report must still land. Losing it is the product
	// harm: a repository stops reporting tests because one artifact was bad.
	if walk.cursor.Suites != 1 {
		t.Fatalf("committed %d suites, want 1 from the healthy artifact", walk.cursor.Suites)
	}
	// Coverage honesty is a separate claim from surviving: data really was
	// lost, and the unit must not report complete coverage.
	if complete, ok := walk.final.Result["reports_complete"].(bool); !ok || complete {
		t.Fatalf(
			"reports_complete=%v, want false after an artifact was skipped",
			walk.final.Result["reports_complete"],
		)
	}
	observation := githubTestsSkipObservation(t, walk.cursor.Incomplete, githubTestsReportMemberComponent)
	if observation.Cause != githubTestsUnreadableArchiveCause || observation.Count != 1 {
		t.Fatalf(
			"durable observation=%+v, want cause=%s count=1",
			observation, githubTestsUnreadableArchiveCause,
		)
	}
	// The fail-closed gate the chunked executor runs before a completion
	// becomes durable must accept this shape too.
	if _, err := (ProductionContractComparator{}).CompareCompleteRoute(
		context.Background(), nativeTestClaim("github", "cicd"), walk.final,
	); err != nil {
		t.Fatalf("production comparator rejected a skipped-artifact completion: %v", err)
	}
}

// The non-chunked oracle route carries the same defect at
// github_tests_route.go:327 and must get the same disposition. It is not the
// path prod executes today, but it is the comparison implementation the
// production comparator is checked against, so letting the two disagree about
// an unreadable artifact would make the oracle reject a healthy chunked
// completion.
func TestGitHubTestsRouteCorruptArtifactDoesNotSinkTheUnit(t *testing.T) {
	doer := &githubTestsCorruptArtifactDoer{t: t, artifacts: 2, corrupt: map[int]bool{1: true}}
	claim := nativeTestClaim("github", "cicd")

	batch, err := (GitHubTestsRouteHandler{}).Collect(
		context.Background(), claim, providerfoundation.Credential{},
		githubTestsClient(t, doer), time.Date(2026, 8, 14, 12, 0, 0, 0, time.UTC),
	)
	if err != nil {
		t.Fatalf(
			"one unreadable artifact sank the non-chunked route: err=%v; want the artifact skipped",
			err,
		)
	}
	if doer.archiveRequests != 2 {
		t.Fatalf(
			"downloaded %d archives, want 2; the route never reached the healthy artifact",
			doer.archiveRequests,
		)
	}
	if complete, ok := batch.Result["reports_complete"].(bool); !ok || complete {
		t.Fatalf(
			"reports_complete=%v, want false after an artifact was skipped",
			batch.Result["reports_complete"],
		)
	}
	incomplete, ok := batch.Result["incomplete"].([]GitHubTestsIncomplete)
	if !ok {
		t.Fatalf("result carried no typed incomplete slice: %#v", batch.Result["incomplete"])
	}
	observation := githubTestsSkipObservation(t, incomplete, githubTestsReportMemberComponent)
	if observation.Cause != githubTestsUnreadableArchiveCause || observation.Count != 1 {
		t.Fatalf(
			"durable observation=%+v, want cause=%s count=1",
			observation, githubTestsUnreadableArchiveCause,
		)
	}
}

// A skipped artifact that nothing counts is a silent data loss: the unit
// finalizes, the board says success, and coverage quietly shrinks. The
// standing requirement is that new behaviour ships with its telemetry, so the
// counter is asserted through the ROUTE rather than by calling the recorder
// directly -- that is what proves the route is actually wired to it.
func TestGitHubTestsUnreadableArchiveIsCounted(t *testing.T) {
	doer := &githubTestsCorruptArtifactDoer{t: t, artifacts: 2, corrupt: map[int]bool{1: true}}
	client := githubTestsClient(t, doer)
	// The executors attach Metrics to the client before handing it to a route
	// (chunked_stream_executor.go:147, chunked_executor.go:92). githubTestsClient
	// leaves it nil, so attach one here or the assertion below would pass
	// vacuously against a nil recorder.
	client.Metrics = providerfoundation.NewMetrics()

	if _, err := walkGitHubTestsChunksResult(t, client, 4); err != nil {
		t.Fatalf("walk returned err=%v", err)
	}

	var buffer bytes.Buffer
	if err := client.Metrics.WritePrometheus(&buffer); err != nil {
		t.Fatalf("WritePrometheus: %v", err)
	}
	want := `dev_health_provider_artifact_skipped_total{provider="github",dataset="cicd",reason="unreadable_archive"} 1`
	if !strings.Contains(buffer.String(), want) {
		t.Fatalf("metrics did not carry the skip:\nwant line: %s\ngot:\n%s", want, buffer.String())
	}
}

// githubTestsSkipObservation returns the durable observation recorded for a
// component, or fails. Asserting the CAUSE matters: the durable observation is
// what the production comparator validates and what an operator reads, and the
// report_member vocabulary already contained "unreadable" for a member that
// could not be parsed. Without this, recording the pre-existing cause for a
// whole unreadable ARCHIVE would be indistinguishable from recording the new
// one, and the two conditions are not the same.
func githubTestsSkipObservation(
	t *testing.T, incomplete []GitHubTestsIncomplete, component string,
) GitHubTestsIncomplete {
	t.Helper()
	for _, observation := range incomplete {
		if observation.Component == component {
			return observation
		}
	}
	t.Fatalf("no %s observation in %+v", component, incomplete)
	return GitHubTestsIncomplete{}
}

// The chunked route is the one production executes. Its blocking-issue branch
// -- an archive that OPENS and then breaches a bound -- must still fail the
// batch closed, unchanged by the unreadable-container work. The retargeted
// oracle test covers only the non-chunked twin, so without this the production
// path's fail-closed behaviour is asserted nowhere.
func TestGitHubTestsChunkedUnsafeArchiveBoundsStillFailsClosed(t *testing.T) {
	doer := &githubTestsCorruptArtifactDoer{
		t: t, artifacts: 1, corrupt: map[int]bool{}, oversizedReports: true,
	}

	_, err := walkGitHubTestsChunksResult(t, githubTestsClient(t, doer), 4)
	if !errors.Is(err, ErrGitHubTestsIncomplete) {
		t.Fatalf("chunked route err=%v, want the batch to fail closed on a blocking issue", err)
	}
	if !strings.Contains(err.Error(), "unsafe archive bounds") {
		t.Fatalf("err=%v, want the unsafe-archive-bounds refusal", err)
	}
	// Anti-vacuity: the archive must actually have been downloaded and opened,
	// or this would pass on a route that failed earlier for another reason.
	if doer.archiveRequests != 1 {
		t.Fatalf("downloaded %d archives, want 1", doer.archiveRequests)
	}
}

// RED (CHAOS-4177 critical, codex round 1). Skipping ONE unreadable artifact
// is right: the rest of the walk is real data and the window is re-walked, so
// nothing is lost. Skipping EVERY artifact is a different fact about the
// world. A proxy or auth edge that returns an error document for every
// artifact produces a unit that completes successfully having ingested no test
// data at all, and re-walks the same window forever because the watermark is
// withheld. Nothing is corrupt and nothing is lost, but nothing progresses
// either, and the board shows success.
//
// Total unreadability is a systematic route condition, not item noise, and it
// does not heal by re-walking. It must fail loudly with its own cause so the
// unit carries a real reason an operator can act on. Attempts are spent only in
// the case where retrying was pointless anyway.
func TestGitHubTestsAllArtifactsUnreadableFailsTheUnit(t *testing.T) {
	doer := &githubTestsCorruptArtifactDoer{
		t: t, artifacts: 2, corrupt: map[int]bool{1: true, 2: true},
	}

	_, err := walkGitHubTestsChunksResult(t, githubTestsClient(t, doer), 4)

	if err == nil {
		t.Fatal("every artifact was unreadable and the unit completed successfully; " +
			"want a loud failure naming the systematic condition")
	}
	if !errors.Is(err, ErrGitHubTestsIncomplete) {
		t.Fatalf("err=%v, want it to wrap ErrGitHubTestsIncomplete", err)
	}
	if !strings.Contains(err.Error(), githubTestsAllArtifactsUnreadableCause) {
		t.Fatalf("err=%v, want it to name %s", err, githubTestsAllArtifactsUnreadableCause)
	}
	// Anti-vacuity: both artifacts must have been downloaded, or the failure
	// could be something that happened before totality could be observed.
	if doer.archiveRequests != 2 {
		t.Fatalf("downloaded %d archives, want 2", doer.archiveRequests)
	}
}

// The complement, and the line that must not move: one readable artifact among
// unreadable ones is NOT systematic. That walk still completes, still commits
// the readable report, and still counts the skip.
func TestGitHubTestsPartiallyUnreadableArtifactsStillComplete(t *testing.T) {
	doer := &githubTestsCorruptArtifactDoer{
		t: t, artifacts: 2, corrupt: map[int]bool{1: true},
	}

	walk, err := walkGitHubTestsChunksResult(t, githubTestsClient(t, doer), 4)
	if err != nil {
		t.Fatalf("a partially unreadable walk failed: %v; only TOTAL unreadability is systematic", err)
	}
	if walk.cursor.Suites != 1 {
		t.Fatalf("committed %d suites, want the readable artifact's 1", walk.cursor.Suites)
	}
	observation := githubTestsSkipObservation(t, walk.cursor.Incomplete, githubTestsReportMemberComponent)
	if observation.Cause != githubTestsUnreadableArchiveCause {
		t.Fatalf("cause=%s, want %s", observation.Cause, githubTestsUnreadableArchiveCause)
	}
}
