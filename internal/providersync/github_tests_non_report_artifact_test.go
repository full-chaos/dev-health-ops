package providersync

import (
	"bytes"
	"compress/gzip"
	"io"
	"net/http"
	"strconv"
	"strings"
	"testing"
	"unicode/utf8"
)

// githubTestsNamedArtifactsFixture is githubTestsArtifactsFixture with
// caller-chosen names, needed because the shared fixture always names every
// artifact "test-results-N" and this file's whole point is the Name field.
func githubTestsNamedArtifactsFixture(names []string) string {
	var body strings.Builder
	body.WriteString(`{"artifacts":[`)
	for index, name := range names {
		if index > 0 {
			body.WriteByte(',')
		}
		body.WriteString(`{"id":`)
		body.WriteString(strconv.Itoa(index + 1))
		body.WriteString(`,"name":"`)
		body.WriteString(name)
		body.WriteString(`","expired":false}`)
	}
	body.WriteString(`]}`)
	return body.String()
}

// githubTestsDockerBuildGzipPayload is real GitHub artifact bytes: a probe
// against full-chaos/dev-health-ops run 32866726703's
// "full-chaos~dev-health-ops~BV20PU.dockerbuild" artifact
// (id 9570491681, `gh api /repos/.../actions/artifacts/9570491681/zip`,
// local, 2026-08-30) showed the download's first bytes are `1f 8b 08 00` --
// the gzip magic number, not `PK\x03\x04`. GitHub's classic
// archive_download_url does NOT zip-wrap a Docker Build Summary bundle the
// way it wraps an ordinary actions/upload-artifact artifact; the response IS
// the raw gzip stream. This fixture reproduces that exact shape (a real
// gzip member, not an arbitrary non-zip string) so the test exercises the
// same zip.NewReader rejection production hits, not a stand-in.
var githubTestsDockerBuildGzipPayload = func() []byte {
	var buffer bytes.Buffer
	writer := gzip.NewWriter(&buffer)
	_, _ = writer.Write([]byte(`{"buildx.build.ref":"probe/CHAOS-4588"}`))
	_ = writer.Close()
	return buffer.Bytes()
}()

// githubTestsNonReportArtifactDoer serves one workflow run mixing named
// ".dockerbuild" artifacts (real-shape raw gzip bytes on download) with one
// genuine report artifact (a real zip holding one JUnit suite). It records
// which artifact IDs were ever downloaded so a test can prove the
// dockerbuild ones were excluded BEFORE any request, not merely tolerated
// after a failed read.
type githubTestsNonReportArtifactDoer struct {
	t           *testing.T
	names       []string
	downloadIDs []string
}

func (doer *githubTestsNonReportArtifactDoer) Do(request *http.Request) (*http.Response, error) {
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
		return githubTestsHTTPResponse(request, header, githubTestsNamedArtifactsFixture(doer.names)), nil
	case strings.HasPrefix(path, "/repos/acme/api/actions/artifacts/") && strings.HasSuffix(path, "/zip"):
		id := githubTestsArtifactIDFromPath(doer.t, path)
		doer.downloadIDs = append(doer.downloadIDs, strconv.Itoa(id))
		name := doer.names[id-1]
		if selected, reason := githubTestsArtifactSelectionSeam(name); !selected {
			// The selection seam must run BEFORE any download -- an excluded
			// name reaching here at all is the regression this doer exists to
			// catch, whichever of the two closed reasons excluded it.
			doer.t.Fatalf("artifact %q (excluded: %s) was downloaded; selection must happen before download", name, reason)
			return nil, nil
		}
		archive := githubTestsZip(doer.t, map[string]string{"junit.xml": githubTestsMultiSuiteJUnit(1)})
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

// RED on main (CHAOS-4588). Two GitHub-generated ".dockerbuild" build-summary
// artifacts sit alongside one genuine JUnit report artifact in the same run.
// Before this fix, githubTestsArtifactPayload never decoded Name at all, so
// both dockerbuild artifacts were downloaded and fed to
// parseGitHubTestsArtifact, which correctly rejects their raw-gzip bytes as
// ErrGitHubTestsArchiveUnreadable -- recording two false report_member/
// unreadable_archive skips and two wasted downloads for artifacts that were
// never test reports to begin with. After the fix, the dockerbuild artifacts
// are excluded by name before any download, so no incomplete observation is
// recorded, only the real artifact is fetched, and the unit reports fully
// complete.
func TestGitHubTestsDockerBuildArtifactsExcludedBeforeDownload(t *testing.T) {
	doer := &githubTestsNonReportArtifactDoer{
		t: t,
		names: []string{
			"full-chaos~dev-health-ops~BV20PU.dockerbuild",
			"full-chaos~dev-health-ops~RPQHQ0.dockerbuild",
			"integration-junit",
		},
	}
	walk, err := walkGitHubTestsChunksResult(t, githubTestsClient(t, doer), 4)
	if err != nil {
		t.Fatalf("dockerbuild artifacts sank the unit: err=%v, want them excluded, not fatal", err)
	}
	if walk.cursor.Phase != "done" {
		t.Fatalf("terminal phase=%q, want done", walk.cursor.Phase)
	}
	if len(doer.downloadIDs) != 1 || doer.downloadIDs[0] != "3" {
		t.Fatalf(
			"downloaded artifact ids=%v, want exactly [3]: a .dockerbuild artifact must never be "+
				"requested at all, not merely tolerated after a failed read",
			doer.downloadIDs,
		)
	}
	if walk.cursor.Suites != 1 {
		t.Fatalf("committed %d suites, want 1 from the real report artifact", walk.cursor.Suites)
	}
	if len(walk.cursor.Incomplete) != 0 {
		t.Fatalf(
			"incomplete=%+v, want none: a .dockerbuild artifact is a routine provider artifact type, "+
				"not a read failure (excluded exactly like a routine 404, CHAOS-4185)",
			walk.cursor.Incomplete,
		)
	}
	if complete, ok := walk.final.Result["reports_complete"].(bool); !ok || !complete {
		t.Fatalf("reports_complete=%v, want true: nothing readable was actually lost", walk.final.Result["reports_complete"])
	}
}

// Exercises the seam's durable bookkeeping (CHAOS-4591 prep):
// ExcludedNonReportSuffix counts by reason, and ExcludedArtifactSample
// carries "name (reason)".
func TestGitHubTestsDockerBuildArtifactExclusionBookkeeping(t *testing.T) {
	doer := &githubTestsNonReportArtifactDoer{
		t: t,
		names: []string{
			"full-chaos~dev-health-ops~BV20PU.dockerbuild",
			"integration-junit",
		},
	}
	walk, err := walkGitHubTestsChunksResult(t, githubTestsClient(t, doer), 4)
	if err != nil {
		t.Fatalf("dockerbuild artifact sank the unit: err=%v, want it excluded, not fatal", err)
	}
	if len(doer.downloadIDs) != 1 || doer.downloadIDs[0] != "2" {
		t.Fatalf(
			"downloaded artifact ids=%v, want exactly [2]: a .dockerbuild artifact must never be requested",
			doer.downloadIDs,
		)
	}
	if walk.cursor.Suites != 1 {
		t.Fatalf("committed %d suites, want 1 from the real report artifact", walk.cursor.Suites)
	}
	if walk.cursor.ExcludedNonReportSuffix != 1 {
		t.Fatalf("ExcludedNonReportSuffix=%d, want 1", walk.cursor.ExcludedNonReportSuffix)
	}
	if walk.cursor.ExcludedNonReportPrefix != 0 {
		t.Fatalf("ExcludedNonReportPrefix=%d, want 0: the default prefix list is empty (codex round 1, P1)", walk.cursor.ExcludedNonReportPrefix)
	}
	want := []string{"full-chaos~dev-health-ops~BV20PU.dockerbuild (non_report_artifact_suffix)"}
	if len(walk.cursor.ExcludedArtifactSample) != 1 || walk.cursor.ExcludedArtifactSample[0] != want[0] {
		t.Fatalf("ExcludedArtifactSample=%v, want %v", walk.cursor.ExcludedArtifactSample, want)
	}
	// Nothing readable was lost, and no report_member skip occurred -- this
	// exclusion is not "incomplete" data, distinct from the durable
	// per-artifact result fields the CHAOS-4591 admin view will read.
	if len(walk.cursor.Incomplete) != 0 {
		t.Fatalf("incomplete=%+v, want none", walk.cursor.Incomplete)
	}
	if got := walk.final.Result["excluded_non_report_suffix"]; got != 1 {
		t.Errorf("final.Result[excluded_non_report_suffix]=%v, want 1", got)
	}
}

// Deliberate scoping decision as an executable spec (codex review round 1,
// P1): "digests-*" artifacts (this repo's own docker-images.yml uploads one
// per build target, e.g. "digests-api-3") are real, valid, tiny zips that
// parse cleanly and contribute zero report rows -- unlike ".dockerbuild",
// excluding them is a bandwidth optimization, not a correctness fix, and
// "digests-" is a plausible prefix a real report artifact could organically
// collide with on some other repository. githubTestsNonReportArtifactPrefixes
// is deliberately empty by default; this test pins that decision so a future
// change re-adding a global prefix default (rather than routing it through
// CHAOS-4591's config-driven predicate) fails loudly, not silently.
func TestGitHubTestsDigestArtifactsAreNotExcludedByDefault(t *testing.T) {
	if selected, reason := githubTestsArtifactSelectionSeam("digests-api-3"); !selected {
		t.Fatalf(
			"digests-api-3 was excluded (reason=%s), want it selected: the default prefix list "+
				"must stay empty (codex round 1, P1) -- digests-* exclusion belongs to CHAOS-4591's "+
				"config-driven predicate, not a global default",
			reason,
		)
	}
}

// githubTestsTruncateArtifactName unit tests (codex round 1, P1): a
// provider-supplied artifact name is unbounded (GitHub's own limit is 255
// bytes), and up to githubTestsMaxSkippedArtifactRecords of them live in the
// same maxChunkCursorBytes (4KiB) cursor budget as everything else. Untested,
// this bound is exactly the kind of thing a later refactor silently drops.
func TestGitHubTestsTruncateArtifactNameStaysWithinBound(t *testing.T) {
	short := "digests-api-3"
	if got := githubTestsTruncateArtifactName(short); got != short {
		t.Fatalf("truncated a name under the bound: got %q, want unchanged %q", got, short)
	}

	long := strings.Repeat("a", githubTestsMaxArtifactNameBytes+40)
	got := githubTestsTruncateArtifactName(long)
	if len(got) > githubTestsMaxArtifactNameBytes+len("…") {
		t.Fatalf("truncated name is %d bytes, want <= %d", len(got), githubTestsMaxArtifactNameBytes+len("…"))
	}
	if !strings.HasSuffix(got, "…") {
		t.Fatalf("truncated name %q does not end with the ellipsis marker", got)
	}
	if !utf8.ValidString(got) {
		t.Fatalf("truncated name %q is not valid UTF-8", got)
	}

	// A multi-byte codepoint sitting exactly on the cut boundary must not be
	// split -- the byte-safety loop must back off far enough to land on a
	// valid boundary, not just chop githubTestsMaxArtifactNameBytes bytes.
	multiByte := strings.Repeat("a", githubTestsMaxArtifactNameBytes-1) + "€€€"
	gotMultiByte := githubTestsTruncateArtifactName(multiByte)
	if !utf8.ValidString(gotMultiByte) {
		t.Fatalf("truncated multi-byte name %q is not valid UTF-8", gotMultiByte)
	}
}

// RED on the pre-truncation version of this fix (codex round 1, P1). A
// dockerbuild-suffixed name far longer than githubTestsMaxArtifactNameBytes
// must still be excluded (unchanged behavior) AND its ExcludedArtifactSample
// entry must be bounded, not the full unbounded provider name.
func TestGitHubTestsExcludedArtifactSampleNameIsBounded(t *testing.T) {
	longName := "full-chaos~a-repository-name-chosen-to-be-implausibly-long-for-this-test~ABCDEF.dockerbuild"
	doer := &githubTestsNonReportArtifactDoer{
		t:     t,
		names: []string{longName, "integration-junit"},
	}
	walk, err := walkGitHubTestsChunksResult(t, githubTestsClient(t, doer), 4)
	if err != nil {
		t.Fatalf("walk returned err=%v", err)
	}
	if len(walk.cursor.ExcludedArtifactSample) != 1 {
		t.Fatalf("ExcludedArtifactSample=%v, want exactly 1 entry", walk.cursor.ExcludedArtifactSample)
	}
	entry := walk.cursor.ExcludedArtifactSample[0]
	if len(entry) >= len(longName) {
		t.Fatalf("excluded-sample entry %q (%d bytes) was not bounded below the raw name %q (%d bytes)", entry, len(entry), longName, len(longName))
	}
	if !strings.Contains(entry, "…") {
		t.Fatalf("excluded-sample entry %q does not show truncation", entry)
	}
}

// RED on main (CHAOS-4588, CHAOS-4142 interaction). A run's artifact list
// mixes three ".dockerbuild" artifacts with one genuine report artifact,
// against a per-run cap of 2. Before this fix, the cap was applied to the
// UNFILTERED list: 4 > 2 truncates to the first 2 items, which -- ordered as
// GitHub returns them, dockerbuild-first in this fixture -- drops the real
// report artifact entirely and records a run_artifacts per_run_cap
// observation. That cause is WINDOW-BLOCKING
// (githubTestsWatermarkAdvancingPairs only lists report_member causes as
// advancing), so it withholds the watermark -- and because the SAME
// dockerbuild artifacts recur on every future encounter of this run, the
// unit re-walks and re-fails identically forever. This is the exact
// mechanism that pinned full-chaos/dev-health-ops's real "tests" watermark at
// 2026-08-08 (local CH: 0 rows in test_suite_results/test_case_results for
// that repo, confirmed 2026-08-30). After the fix, the dockerbuild artifacts
// are filtered out before the cap is applied, so only 1 candidate remains,
// well under the cap: no truncation, no per_run_cap observation, and the
// watermark advances.
func TestGitHubTestsDockerBuildArtifactsDoNotConsumePerRunArtifactCap(t *testing.T) {
	doer := &githubTestsNonReportArtifactDoer{
		t: t,
		names: []string{
			"full-chaos~dev-health-ops~BV20PU.dockerbuild",
			"full-chaos~dev-health-ops~RPQHQ0.dockerbuild",
			"full-chaos~dev-health-ops~DDVI2D.dockerbuild",
			"integration-junit",
		},
	}
	claim := nativeTestClaim("github", "cicd")
	client := githubTestsClient(t, doer)
	walk := walkGitHubTestsChunks(t, GitHubTestsRouteHandler{MaxArtifactsPerRun: 2}, claim, client, 4)

	if walk.cursor.Phase != "done" {
		t.Fatalf("terminal phase=%q, want done", walk.cursor.Phase)
	}
	if walk.cursor.Suites != 1 {
		t.Fatalf(
			"committed %d suites, want 1: the real report artifact must survive a cap sized only "+
				"for it, once dockerbuild noise is excluded first",
			walk.cursor.Suites,
		)
	}
	for _, observation := range walk.cursor.Incomplete {
		if observation.Component == githubTestsRunArtifactsComponent && observation.Cause == githubTestsPerRunCapCause {
			t.Fatalf(
				"run_artifacts per_run_cap fired (%+v): dockerbuild artifacts must not consume the "+
					"per-run cap ahead of the real report artifact -- this is the window-blocking "+
					"mechanism that pins a repo's tests watermark forever (CHAOS-4142)",
				observation,
			)
		}
	}
	if walk.final.Watermark == nil {
		t.Fatal("watermark withheld: dockerbuild-only cap pressure must not block coverage from advancing")
	}
}
