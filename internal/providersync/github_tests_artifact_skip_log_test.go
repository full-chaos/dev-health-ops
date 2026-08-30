package providersync

import (
	"bytes"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"testing"
)

// RED on main (CHAOS-4588). Before this fix, recordGitHubTestsSkippedArtifact
// emitted "provider artifact skipped; inventory continued" once PER SKIPPED
// ARTIFACT -- for full-chaos/dev-health-ops, ~14 times per unit attempt at
// ~300ms intervals (297 lines/30min locally). This test drives two genuinely
// unreadable artifacts (a 2xx non-zip body, the corrupt-artifact fixture
// shape) alongside one healthy one through the SAME unit, and asserts the
// unit logs exactly ONE record, carrying the total and the per-cause counts
// as structured attrs -- not two, and not a message string an operator would
// have to parse counts out of.
func TestGitHubTestsArtifactSkipsLogOncePerUnitWithCountsByCause(t *testing.T) {
	records := captureMembershipLogs(t)
	doer := &githubTestsCorruptArtifactDoer{t: t, artifacts: 3, corrupt: map[int]bool{1: true, 2: true}}

	walk, err := walkGitHubTestsChunksResult(t, githubTestsClient(t, doer), 4)
	if err != nil {
		t.Fatalf("walk returned err=%v, want the unit to finalize with two skips recorded", err)
	}
	if walk.cursor.Suites != 1 {
		t.Fatalf("premise failed: committed %d suites, want 1 (the one healthy artifact)", walk.cursor.Suites)
	}

	var summaries []slog.Record
	for _, record := range *records {
		if record.Message == "provider artifacts skipped this unit; inventory continued" {
			summaries = append(summaries, record)
		}
	}
	if len(summaries) != 1 {
		t.Fatalf(
			"got %d skip-summary log records, want exactly 1 for the whole unit (all records: %+v)",
			len(summaries), *records,
		)
	}
	record := summaries[0]
	if record.Level != slog.LevelWarn {
		t.Fatalf("level=%s, want WARN", record.Level)
	}
	attrs := membershipLogAttrs(record)
	if attrs["artifact_skip_total"] != int64(2) {
		t.Fatalf("artifact_skip_total=%v, want 2", attrs["artifact_skip_total"])
	}
	if attrs["incomplete_total"] != int64(2) {
		t.Fatalf("incomplete_total=%v, want 2", attrs["incomplete_total"])
	}
	if attrs["report_member_unreadable_archive"] != int64(2) {
		t.Fatalf("report_member_unreadable_archive=%v, want 2 (both corrupt artifacts, one cause, one counted line)", attrs["report_member_unreadable_archive"])
	}
	if attrs["repository"] != "Acme/API" {
		t.Fatalf("repository=%v, want Acme/API", attrs["repository"])
	}
}

// The other half: a fully healthy unit must log nothing about skips. A line
// on every sync regardless of content is noise operators learn to filter,
// which is the same reasoning githubProjectV2's membership_skips log follows
// (github_project_membership_log_test.go).
func TestGitHubTestsHealthyUnitLogsNoSkipSummary(t *testing.T) {
	records := captureMembershipLogs(t)
	doer := &githubTestsCorruptArtifactDoer{t: t, artifacts: 2, corrupt: map[int]bool{}}

	if _, err := walkGitHubTestsChunksResult(t, githubTestsClient(t, doer), 4); err != nil {
		t.Fatalf("healthy walk returned err=%v", err)
	}
	for _, record := range *records {
		if record.Message == "provider artifacts skipped this unit; inventory continued" {
			t.Fatalf("a fully healthy unit logged a skip summary: %+v", record)
		}
	}
}

// githubTestsMixedMemberArtifactDoer serves ONE artifact whose archive OPENS
// successfully and contains one valid JUnit member alongside one malformed
// one -- a member-level skip, not a whole-artifact one.
type githubTestsMixedMemberArtifactDoer struct{ t *testing.T }

func (doer *githubTestsMixedMemberArtifactDoer) Do(request *http.Request) (*http.Response, error) {
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
		return githubTestsHTTPResponse(request, header, githubTestsArtifactsFixture(1)), nil
	case strings.HasPrefix(path, "/repos/acme/api/actions/artifacts/") && strings.HasSuffix(path, "/zip"):
		archive := githubTestsZip(doer.t, map[string]string{
			"good.xml": githubTestsMultiSuiteJUnit(1),
			"bad.xml":  `<!DOCTYPE x [<!ENTITY x "boom">]><testsuite>&x;</testsuite>`,
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

// RED on the pre-fix version of this summary (codex round 2, P2). An
// artifact that opens fine but has one malformed MEMBER is a report_member
// skip in the closed vocabulary, but the artifact itself was not skipped --
// the healthy member's rows still land. artifact_skip_total (which an
// operator reads as "how many whole artifacts were skipped") must stay 0;
// only incomplete_total (the full closed-vocabulary count) reflects it.
func TestGitHubTestsMemberLevelSkipDoesNotCountAsAnArtifactSkip(t *testing.T) {
	records := captureMembershipLogs(t)
	doer := &githubTestsMixedMemberArtifactDoer{t: t}

	walk, err := walkGitHubTestsChunksResult(t, githubTestsClient(t, doer), 4)
	if err != nil {
		t.Fatalf("walk returned err=%v", err)
	}
	if walk.cursor.Suites != 1 {
		t.Fatalf("committed %d suites, want 1 from the healthy member", walk.cursor.Suites)
	}

	var summary slog.Record
	found := false
	for _, record := range *records {
		if record.Message == "provider artifacts skipped this unit; inventory continued" {
			summary, found = record, true
		}
	}
	if !found {
		t.Fatalf("no skip-summary log record, want one for the malformed member (all records: %+v)", *records)
	}
	attrs := membershipLogAttrs(summary)
	if attrs["artifact_skip_total"] != int64(0) {
		t.Fatalf("artifact_skip_total=%v, want 0: the artifact was not skipped, only one member inside it", attrs["artifact_skip_total"])
	}
	if attrs["incomplete_total"] != int64(1) {
		t.Fatalf("incomplete_total=%v, want 1 (the malformed member)", attrs["incomplete_total"])
	}
	if attrs["report_member_malformed"] != int64(1) {
		t.Fatalf("report_member_malformed=%v, want 1", attrs["report_member_malformed"])
	}
}

// RED before this fix (CHAOS-4592 codex review, on merged CHAOS-4588 code):
// githubTestsLogArtifactSkipSummary's gate was `len(incomplete) == 0`, but
// incomplete ALSO carries run-level page-budget truncations that never
// skipped a single artifact or report member. A unit whose run-listing hits
// its cumulative page budget -- with the artifacts endpoint returning
// nothing to skip at all -- must not log "provider artifacts skipped this
// unit": that line already fired, correctly, at the truncation site
// (recordGitHubTestsInventoryTruncation's "provider inventory page budget
// exhausted"), and the summary line repeating it with a misleading
// artifact_skip_total=0 right next to the claim is not a second useful
// signal, it is noise that teaches operators to distrust the message.
func TestGitHubTestsRunLevelTruncationDoesNotLogArtifactSkipSummary(t *testing.T) {
	records := captureMembershipLogs(t)
	doer := &githubTestsPagedDoer{t: t, pages: 3, perPage: 2}
	claim := nativeTestClaim("github", "tests")

	walk := walkGitHubTestsChunks(t, GitHubTestsRouteHandler{MaxRuns: 100}, claim, githubTestsClient(t, doer), 2)
	if !githubTestsBlocksWatermark(
		walk.cursor.Incomplete, walk.cursor.SkippedArtifacts,
		walk.cursor.SkippedArtifactsOverflow, walk.cursor.SkippedArtifactCauseOverflow,
	) {
		t.Fatalf("premise failed: want an inventory truncation, incomplete=%+v", walk.cursor.Incomplete)
	}
	observation := githubTestsSkipObservation(t, walk.cursor.Incomplete, githubTestsRunInventoryComponent)
	if observation.Cause != githubTestsPageBudgetCause {
		t.Fatalf("premise failed: cause=%s, want %s", observation.Cause, githubTestsPageBudgetCause)
	}

	for _, record := range *records {
		if record.Message == "provider artifacts skipped this unit; inventory continued" {
			t.Fatalf(
				"a unit with zero artifact/member skips logged the artifact-skip summary: %+v",
				record,
			)
		}
	}
}

// RED before this fix (CHAOS-4592 codex review, on merged CHAOS-4588 code):
// the oversized-artifact branch in CollectChunks kept its own direct
// slog.Warn("provider artifact skipped: oversized", ...) from before
// CHAOS-4588 collapsed every OTHER report_member skip cause onto one
// per-unit summary line. A unit with an oversized artifact therefore logged
// TWO records -- the per-artifact line here and the summary line at
// finalization -- violating the at-most-one-line-per-unit contract
// TestGitHubTestsArtifactSkipsLogOncePerUnitWithCountsByCause already pins
// for every other cause, and duplicating the summary's skipped_sample.
func TestGitHubTestsOversizedArtifactLogsExactlyOneLine(t *testing.T) {
	records := captureMembershipLogs(t)
	doer := &githubTestsDownloadFailureDoer{t: t, artifacts: 2, oversized: map[int]bool{1: true}}

	walk, err := walkGitHubTestsChunksResult(t, githubTestsClient(t, doer), 4)
	if err != nil {
		t.Fatalf("walk returned err=%v, want the unit to finalize with the oversized artifact skipped", err)
	}
	if walk.cursor.Suites != 1 {
		t.Fatalf("premise failed: committed %d suites, want 1 (the one healthy artifact)", walk.cursor.Suites)
	}

	for _, record := range *records {
		if record.Message == "provider artifact skipped: oversized" {
			t.Fatalf(
				"the per-artifact oversized line still fires; want it folded into the unit summary only: %+v",
				record,
			)
		}
	}
	var summaries []slog.Record
	for _, record := range *records {
		if record.Message == "provider artifacts skipped this unit; inventory continued" {
			summaries = append(summaries, record)
		}
	}
	if len(summaries) != 1 {
		t.Fatalf(
			"got %d skip-summary log records, want exactly 1 for the whole unit (all records: %+v)",
			len(summaries), *records,
		)
	}
	attrs := membershipLogAttrs(summaries[0])
	if attrs["artifact_skip_total"] != int64(1) {
		t.Fatalf("artifact_skip_total=%v, want 1", attrs["artifact_skip_total"])
	}
	if attrs["report_member_artifact_oversized"] != int64(1) {
		t.Fatalf("report_member_artifact_oversized=%v, want 1", attrs["report_member_artifact_oversized"])
	}
}
