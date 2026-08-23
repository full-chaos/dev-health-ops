package providersync

import (
	"bytes"
	"context"
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
	corrupt         map[int]bool
	archiveRequests int
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
		archive := githubTestsZip(doer.t, map[string]string{
			"junit.xml": githubTestsMultiSuiteJUnit(1),
		})
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
	doer providerfoundation.HTTPDoer,
	maxChunks int,
) (githubTestsWalk, error) {
	t.Helper()
	claim := nativeTestClaim("github", "cicd")
	client := githubTestsClient(t, doer)
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

	walk, err := walkGitHubTestsChunksResult(t, doer, 4)
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

	walk, err := walkGitHubTestsChunksResult(t, doer, 4)
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
	// The fail-closed gate the chunked executor runs before a completion
	// becomes durable must accept this shape too.
	if _, err := (ProductionContractComparator{}).CompareCompleteRoute(
		context.Background(), nativeTestClaim("github", "cicd"), walk.final,
	); err != nil {
		t.Fatalf("production comparator rejected a skipped-artifact completion: %v", err)
	}
}
