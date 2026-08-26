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

// CHAOS-4191: prod cicd units failed with the BARE
// "github tests inventory incomplete" message -- no cause suffix, unlike the
// zip-abort class CHAOS-4177/#1882 fixed. downloadGitHubTestsArtifact
// (github_tests_route.go) has two raise sites that return the sentinel
// UNWRAPPED, with nothing appended: a 3xx artifact-download redirect with no
// Location header, and the read-body branch that collapses a real I/O error
// and "body exceeded max download size" into the same bare return. Both
// propagate to the route caller untouched (github_tests_chunked_route.go:696-698
// is the path production executes; github_tests_route.go:319-321 is its
// oracle twin), which is what makes the prod message suffix-less.
//
// githubTestsDownloadFailureDoer serves ONE workflow run whose artifacts hit
// these raise sites individually, so each can be pinned and fixed
// independently of the parse-time corrupt-artifact class already covered by
// github_tests_corrupt_artifact_test.go.
type githubTestsDownloadFailureDoer struct {
	t         *testing.T
	artifacts int
	// noLocation holds the 1-based artifact ids whose download redirects with
	// no Location header.
	noLocation map[int]bool
	// readError holds the 1-based artifact ids whose blob body errors mid-read.
	readError map[int]bool
	// oversized holds the 1-based artifact ids whose blob body exceeds
	// githubTestsMaxDownloadSize.
	oversized       map[int]bool
	archiveRequests int
}

func (doer *githubTestsDownloadFailureDoer) Do(request *http.Request) (*http.Response, error) {
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
		if doer.noLocation[id] {
			return &http.Response{
				StatusCode: http.StatusFound,
				Header:     http.Header{},
				Body:       io.NopCloser(strings.NewReader("")),
				Request:    request,
			}, nil
		}
		redirectHeader := http.Header{"Location": {"https://blob.example/" + strconv.Itoa(id) + ".zip"}}
		return &http.Response{
			StatusCode: http.StatusFound,
			Header:     redirectHeader,
			Body:       io.NopCloser(strings.NewReader("")),
			Request:    request,
		}, nil
	case request.URL.Host == "blob.example":
		id := githubTestsArtifactIDFromBlobPath(doer.t, path)
		if request.Header.Get("Authorization") != "" {
			doer.t.Fatal("provider Authorization leaked to artifact blob host")
		}
		blobHeader := http.Header{"Content-Type": {"application/zip"}}
		switch {
		case doer.readError[id]:
			return &http.Response{
				StatusCode: http.StatusOK, Header: blobHeader,
				Body: io.NopCloser(&githubTestsErrorReader{}), Request: request,
			}, nil
		case doer.oversized[id]:
			return &http.Response{
				StatusCode: http.StatusOK, Header: blobHeader,
				Body:    io.NopCloser(&githubTestsRepeatingReader{remaining: githubTestsMaxDownloadSize + 16}),
				Request: request,
			}, nil
		default:
			archive := githubTestsZip(doer.t, map[string]string{"junit.xml": githubTestsMultiSuiteJUnit(1)})
			return &http.Response{
				StatusCode: http.StatusOK, Header: blobHeader,
				Body: io.NopCloser(bytes.NewReader(archive)), Request: request,
			}, nil
		}
	default:
		doer.t.Fatalf("unexpected request %s", request.URL.String())
		return nil, nil
	}
}

func githubTestsArtifactIDFromBlobPath(t *testing.T, path string) int {
	t.Helper()
	trimmed := strings.TrimSuffix(strings.TrimPrefix(path, "/"), ".zip")
	id, err := strconv.Atoi(trimmed)
	if err != nil {
		t.Fatalf("cannot read artifact id from blob path %q: %v", path, err)
	}
	return id
}

// githubTestsErrorReader always fails mid-read, modelling a dropped
// connection while streaming the artifact blob.
type githubTestsErrorReader struct{}

func (*githubTestsErrorReader) Read([]byte) (int, error) {
	return 0, errors.New("connection reset by peer")
}

// githubTestsRepeatingReader synthesizes `remaining` bytes without holding
// them in one allocation, so a >100MB fixture does not double the memory
// io.ReadAll already has to buffer in the code under test.
type githubTestsRepeatingReader struct {
	remaining int
}

func (r *githubTestsRepeatingReader) Read(p []byte) (int, error) {
	if r.remaining <= 0 {
		return 0, io.EOF
	}
	n := len(p)
	if n > r.remaining {
		n = r.remaining
	}
	for i := 0; i < n; i++ {
		p[i] = 'a'
	}
	r.remaining -= n
	return n, nil
}

// RED on main (CHAOS-4191). GitHub answering the artifact-download redirect
// with no Location header is a property of ONE artifact, exactly like a
// non-zip body (CHAOS-4177) -- yet unlike that class it is not even recorded
// with a cause: github_tests_route.go:612 returns the bare
// ErrGitHubTestsIncomplete sentinel and the chunked route propagates it
// untouched, sinking the whole unit. This asserts the CHAOS-4177 disposition
// extends here: the artifact is skipped, the healthy one after it still
// lands, and the loss is recorded under a distinct cause (not conflated with
// unreadable_archive, which means something else already happened: the bytes
// as observations -- were obtained and the container just would not open).
func TestGitHubTestsArtifactDownloadMissingLocationDoesNotSinkTheUnit(t *testing.T) {
	doer := &githubTestsDownloadFailureDoer{t: t, artifacts: 2, noLocation: map[int]bool{1: true}}

	walk, err := walkGitHubTestsChunksResult(t, githubTestsClient(t, doer), 4)
	if err != nil {
		t.Fatalf(
			"artifact download with no Location header sank the unit: err=%v; want the artifact skipped and the unit finalized",
			err,
		)
	}
	if doer.archiveRequests != 2 {
		t.Fatalf(
			"downloaded %d archive redirects, want 2; the route never reached the healthy artifact after the unavailable one",
			doer.archiveRequests,
		)
	}
	if walk.cursor.Phase != "done" {
		t.Fatalf("terminal phase=%q, want done", walk.cursor.Phase)
	}
	if walk.cursor.Suites != 1 {
		t.Fatalf("committed %d suites, want 1 from the healthy artifact", walk.cursor.Suites)
	}
	if complete, ok := walk.final.Result["reports_complete"].(bool); !ok || complete {
		t.Fatalf("reports_complete=%v, want false after an artifact was skipped", walk.final.Result["reports_complete"])
	}
	observation := githubTestsSkipObservation(t, walk.cursor.Incomplete, githubTestsReportMemberComponent)
	if observation.Cause != githubTestsArtifactUnavailableCause || observation.Count != 1 {
		t.Fatalf(
			"durable observation=%+v, want cause=%s count=1",
			observation, githubTestsArtifactUnavailableCause,
		)
	}
	if _, err := (ProductionContractComparator{}).CompareCompleteRoute(
		context.Background(), nativeTestClaim("github", "cicd"), walk.final,
	); err != nil {
		t.Fatalf("production comparator rejected a skipped-artifact completion: %v", err)
	}
}

// The non-chunked oracle route must carry the same disposition, for the same
// reason github_tests_corrupt_artifact_test.go pins it for the parse-time
// class: it is the comparison implementation the production comparator is
// checked against.
func TestGitHubTestsRouteDownloadMissingLocationDoesNotSinkTheUnit(t *testing.T) {
	doer := &githubTestsDownloadFailureDoer{t: t, artifacts: 2, noLocation: map[int]bool{1: true}}
	claim := nativeTestClaim("github", "cicd")

	batch, err := (GitHubTestsRouteHandler{}).Collect(
		context.Background(), claim, providerfoundation.Credential{},
		githubTestsClient(t, doer), time.Date(2026, 8, 14, 12, 0, 0, 0, time.UTC),
	)
	if err != nil {
		t.Fatalf(
			"artifact download with no Location header sank the non-chunked route: err=%v; want the artifact skipped",
			err,
		)
	}
	if doer.archiveRequests != 2 {
		t.Fatalf("downloaded %d archive redirects, want 2", doer.archiveRequests)
	}
	incomplete, ok := batch.Result["incomplete"].([]GitHubTestsIncomplete)
	if !ok {
		t.Fatalf("result carried no typed incomplete slice: %#v", batch.Result["incomplete"])
	}
	observation := githubTestsSkipObservation(t, incomplete, githubTestsReportMemberComponent)
	if observation.Cause != githubTestsArtifactUnavailableCause || observation.Count != 1 {
		t.Fatalf("durable observation=%+v, want cause=%s count=1", observation, githubTestsArtifactUnavailableCause)
	}
}

// Telemetry ships with the behaviour it counts (standing requirement): a
// skipped-but-uncounted artifact is silent data loss.
func TestGitHubTestsArtifactUnavailableIsCounted(t *testing.T) {
	doer := &githubTestsDownloadFailureDoer{t: t, artifacts: 2, noLocation: map[int]bool{1: true}}
	client := githubTestsClient(t, doer)
	client.Metrics = providerfoundation.NewMetrics()

	if _, err := walkGitHubTestsChunksResult(t, client, 4); err != nil {
		t.Fatalf("walk returned err=%v", err)
	}

	var buffer bytes.Buffer
	if err := client.Metrics.WritePrometheus(&buffer); err != nil {
		t.Fatalf("WritePrometheus: %v", err)
	}
	want := `dev_health_provider_artifact_skipped_total{provider="github",dataset="cicd",reason="artifact_unavailable"} 1`
	if !strings.Contains(buffer.String(), want) {
		t.Fatalf("metrics did not carry the skip:\nwant line: %s\ngot:\n%s", want, buffer.String())
	}
}

// RED on main (CHAOS-4191). A genuine I/O failure reading the artifact blob
// (a dropped connection, not provider data) hits github_tests_route.go:628-629
// and returns the BARE ErrGitHubTestsIncomplete sentinel with the real
// io.ReadAll error discarded. Unlike the missing-Location case above, this is
// not treated as a per-artifact skip: a transient read failure can succeed on
// retry and silently dropping the artifact would risk losing data that a
// retry would have recovered in full. It stays terminal, but the cause must
// survive into the returned error so prod stops seeing the bare string.
func TestGitHubTestsArtifactDownloadReadFailureCarriesCause(t *testing.T) {
	doer := &githubTestsDownloadFailureDoer{t: t, artifacts: 1, readError: map[int]bool{1: true}}
	claim := nativeTestClaim("github", "cicd")

	_, err := (GitHubTestsRouteHandler{}).Collect(
		context.Background(), claim, providerfoundation.Credential{},
		githubTestsClient(t, doer), time.Date(2026, 8, 14, 12, 0, 0, 0, time.UTC),
	)
	if !errors.Is(err, ErrGitHubTestsIncomplete) {
		t.Fatalf("error=%v, want it to still satisfy ErrGitHubTestsIncomplete", err)
	}
	if errors.Is(err, ErrGitHubTestsArtifactOversized) {
		t.Fatalf("error=%v, a genuine read failure must not be misclassified as oversized", err)
	}
	if err.Error() == ErrGitHubTestsIncomplete.Error() {
		t.Fatalf("error=%q is the BARE sentinel with no cause attached (CHAOS-4191)", err.Error())
	}
	if !strings.Contains(err.Error(), "connection reset by peer") {
		t.Fatalf("error=%q does not carry the underlying read failure", err.Error())
	}
}

// RED on main (CHAOS-4191). An artifact body larger than
// githubTestsMaxDownloadSize hits the SAME bare return
// (github_tests_route.go:628-629) as the read-error case above, via the OTHER
// half of the collapsed `readErr != nil || len(body) > max` condition. It
// stays terminal for the same reason "archive_bounds" stays terminal in
// parseGitHubTestsArtifact (github_tests_reports.go:412-413): both are a
// property of the downloaded bytes exceeding a safety bound, not of the
// artifact being unreadable provider data, so silently skipping would hide a
// real, recurring loss rather than a one-off corrupt container.
func TestGitHubTestsArtifactDownloadOversizedCarriesCause(t *testing.T) {
	doer := &githubTestsDownloadFailureDoer{t: t, artifacts: 1, oversized: map[int]bool{1: true}}
	claim := nativeTestClaim("github", "cicd")

	_, err := (GitHubTestsRouteHandler{}).Collect(
		context.Background(), claim, providerfoundation.Credential{},
		githubTestsClient(t, doer), time.Date(2026, 8, 14, 12, 0, 0, 0, time.UTC),
	)
	if !errors.Is(err, ErrGitHubTestsIncomplete) {
		t.Fatalf("error=%v, want it to still satisfy ErrGitHubTestsIncomplete", err)
	}
	if !errors.Is(err, ErrGitHubTestsArtifactOversized) {
		t.Fatalf("error=%v, want it to satisfy ErrGitHubTestsArtifactOversized", err)
	}
	if err.Error() == ErrGitHubTestsIncomplete.Error() {
		t.Fatalf("error=%q is the BARE sentinel with no cause attached (CHAOS-4191)", err.Error())
	}
	if !strings.Contains(err.Error(), strconv.Itoa(githubTestsMaxDownloadSize)) {
		t.Fatalf("error=%q does not carry the max-download-size bound", err.Error())
	}
}

// The two terminal cases above only drove the non-chunked oracle. Production
// dispatch always executes CollectChunks (execution_registry.go marks github
// cicd/tests Chunked unconditionally), so without an equivalent here a
// regression in the chunked route's error propagation could ship while these
// oracle-only tests kept passing.
func TestGitHubTestsChunkedArtifactDownloadReadFailureCarriesCause(t *testing.T) {
	doer := &githubTestsDownloadFailureDoer{t: t, artifacts: 1, readError: map[int]bool{1: true}}

	_, err := walkGitHubTestsChunksResult(t, githubTestsClient(t, doer), 4)
	if !errors.Is(err, ErrGitHubTestsIncomplete) {
		t.Fatalf("error=%v, want it to still satisfy ErrGitHubTestsIncomplete", err)
	}
	if errors.Is(err, ErrGitHubTestsArtifactOversized) {
		t.Fatalf("error=%v, a genuine read failure must not be misclassified as oversized", err)
	}
	if err.Error() == ErrGitHubTestsIncomplete.Error() {
		t.Fatalf("error=%q is the BARE sentinel with no cause attached (CHAOS-4191)", err.Error())
	}
	if !strings.Contains(err.Error(), "connection reset by peer") {
		t.Fatalf("error=%q does not carry the underlying read failure", err.Error())
	}
}

// CHAOS-4315. RED on the pre-fix baseline: main terminalized the whole
// chunked-route unit the moment ONE of its N artifacts exceeded
// githubTestsMaxDownloadSize (the old ErrGitHubTestsChunkedArtifactDownloadOversizedCarriesCause
// asserted exactly that terminal error). Prod hit this every hour
// (github_tests_artifact_oversized, ~22 of the last 30 cicd units for org
// c6a38355) and sync_watermarks pinned forever, because the SAME artifact is
// the same size on every retry -- re-walking the window never clears it.
//
// The ruling (chris 08-23 on CHAOS-4142, reapplied here): partial-commit-
// and-continue. An oversized artifact is provider data, not a fault of the
// caller, exactly like ErrGitHubTestsArtifactUnavailable already is
// (CHAOS-4191) -- skip it, record a durable marker and counter, keep
// walking the OTHER N-1 artifacts, and let the unit finalize instead of
// sinking on this one artifact. This folds into the same top-level func the
// pre-fix assertion lived in (package census pin: 1084 top-level Test
// funcs) rather than adding a new one.
func TestGitHubTestsChunkedArtifactDownloadOversizedCarriesCause(t *testing.T) {
	t.Run("skips the oversized artifact and finalizes the unit", func(t *testing.T) {
		// Two artifacts per run: #1 oversized, #2 healthy -- proves the
		// oversized artifact costs only itself, not the healthy one after it
		// or the whole unit (mirrors
		// TestGitHubTestsArtifactDownloadMissingLocationDoesNotSinkTheUnit's
		// shape for ErrGitHubTestsArtifactUnavailable).
		doer := &githubTestsDownloadFailureDoer{t: t, artifacts: 2, oversized: map[int]bool{1: true}}

		walk, err := walkGitHubTestsChunksResult(t, githubTestsClient(t, doer), 4)
		if err != nil {
			t.Fatalf(
				"oversized artifact sank the unit: err=%v; want it skipped and the unit finalized",
				err,
			)
		}
		if doer.archiveRequests != 2 {
			t.Fatalf(
				"downloaded %d archive redirects, want 2; the route never reached the healthy artifact after the oversized one",
				doer.archiveRequests,
			)
		}
		if walk.cursor.Phase != "done" {
			t.Fatalf("terminal phase=%q, want done", walk.cursor.Phase)
		}
		if walk.cursor.Suites != 1 {
			t.Fatalf("committed %d suites, want 1 from the healthy artifact", walk.cursor.Suites)
		}
		if complete, ok := walk.final.Result["reports_complete"].(bool); !ok || complete {
			t.Fatalf("reports_complete=%v, want false after an artifact was skipped", walk.final.Result["reports_complete"])
		}
		observation := githubTestsSkipObservation(t, walk.cursor.Incomplete, githubTestsReportMemberComponent)
		if observation.Cause != githubTestsArtifactOversizedCause || observation.Count != 1 {
			t.Fatalf(
				"durable observation=%+v, want cause=%s count=1 -- and distinct from artifact_unavailable/unreadable_archive",
				observation, githubTestsArtifactOversizedCause,
			)
		}
		// The oversized branch must bump BOTH totality counters exactly like
		// the adjacent ErrGitHubTestsArtifactUnavailable branch does: SEEN
		// because a real download attempt was made, UNREADABLE because its
		// contents were never obtained. Asserting only the observation/cause
		// above would pass even if one of these two bumps were dropped,
		// silently miscounting the CHAOS-4185 total-unreadability floor.
		if walk.cursor.ArchivesSeen == nil || *walk.cursor.ArchivesSeen != 2 {
			t.Fatalf("ArchivesSeen=%v, want known 2 (1 oversized + 1 healthy)", intPtrString(walk.cursor.ArchivesSeen))
		}
		if walk.cursor.ArchivesUnreadable == nil || *walk.cursor.ArchivesUnreadable != 1 {
			t.Fatalf("ArchivesUnreadable=%v, want known 1 (the oversized artifact only)", intPtrString(walk.cursor.ArchivesUnreadable))
		}
		if _, err := (ProductionContractComparator{}).CompareCompleteRoute(
			context.Background(), nativeTestClaim("github", "cicd"), walk.final,
		); err != nil {
			t.Fatalf("production comparator rejected a skipped-artifact completion: %v", err)
		}
	})

	// A run whose bad artifacts are a MIX of causes (oversized + unavailable)
	// alongside one healthy artifact must classify totality the same way a
	// single-cause mix would: ArchivesSeen counts every attempt, both bad
	// causes land in Incomplete distinctly, and -- because one artifact WAS
	// readable -- githubTestsCheckAllArtifactsUnreadable's seen==unreadable
	// floor (CHAOS-4185) must NOT fire. This guards against the oversized
	// branch double-counting, undercounting, or being classified into the
	// wrong bucket relative to its unavailable sibling.
	t.Run("mixed oversized and unavailable artifacts classify totality correctly", func(t *testing.T) {
		doer := &githubTestsDownloadFailureDoer{
			t: t, artifacts: 3,
			oversized:  map[int]bool{1: true},
			noLocation: map[int]bool{2: true},
		}

		walk, err := walkGitHubTestsChunksResult(t, githubTestsClient(t, doer), 4)
		if err != nil {
			t.Fatalf("mixed oversized/unavailable artifacts sank the unit: err=%v; want both skipped and the unit finalized", err)
		}
		if walk.cursor.Phase != "done" {
			t.Fatalf("terminal phase=%q, want done", walk.cursor.Phase)
		}
		if walk.cursor.Suites != 1 {
			t.Fatalf("committed %d suites, want 1 from the healthy artifact", walk.cursor.Suites)
		}
		if walk.cursor.ArchivesSeen == nil || *walk.cursor.ArchivesSeen != 3 {
			t.Fatalf("ArchivesSeen=%v, want known 3", intPtrString(walk.cursor.ArchivesSeen))
		}
		if walk.cursor.ArchivesUnreadable == nil || *walk.cursor.ArchivesUnreadable != 2 {
			t.Fatalf("ArchivesUnreadable=%v, want known 2 (not 3 -- the healthy artifact must not count)", intPtrString(walk.cursor.ArchivesUnreadable))
		}
		var oversizedCount, unavailableCount int
		for _, observation := range walk.cursor.Incomplete {
			if observation.Component != githubTestsReportMemberComponent {
				continue
			}
			switch observation.Cause {
			case githubTestsArtifactOversizedCause:
				oversizedCount = observation.Count
			case githubTestsArtifactUnavailableCause:
				unavailableCount = observation.Count
			}
		}
		if oversizedCount != 1 {
			t.Fatalf("oversized report_member count=%d, want 1", oversizedCount)
		}
		if unavailableCount != 1 {
			t.Fatalf("unavailable report_member count=%d, want 1 -- must stay distinct from the oversized cause", unavailableCount)
		}
	})

	// Telemetry ships with the behaviour it counts (standing requirement): a
	// skipped-but-uncounted artifact is silent data loss. Reuses the
	// existing dev_health_provider_artifact_skipped_total series
	// (RecordArtifactSkipped) rather than inventing a new one -- CHAOS-4184's
	// ProviderArtifactSkipsSustained alert already consumes it by
	// provider/dataset, and its `reason` label vocabulary
	// (internal/providerfoundation/budget.go) now includes
	// "artifact_oversized" alongside "artifact_unavailable"/"unreadable_archive".
	t.Run("counts the skip on the existing artifact-skip counter", func(t *testing.T) {
		doer := &githubTestsDownloadFailureDoer{t: t, artifacts: 1, oversized: map[int]bool{1: true}}
		client := githubTestsClient(t, doer)
		client.Metrics = providerfoundation.NewMetrics()

		if _, err := walkGitHubTestsChunksResult(t, client, 4); err != nil {
			t.Fatalf("walk returned err=%v", err)
		}

		var buffer bytes.Buffer
		if err := client.Metrics.WritePrometheus(&buffer); err != nil {
			t.Fatalf("WritePrometheus: %v", err)
		}
		want := `dev_health_provider_artifact_skipped_total{provider="github",dataset="cicd",reason="artifact_oversized"} 1`
		if !strings.Contains(buffer.String(), want) {
			t.Fatalf("metrics did not carry the skip:\nwant line: %s\ngot:\n%s", want, buffer.String())
		}
	})
}
