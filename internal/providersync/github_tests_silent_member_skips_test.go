package providersync

import (
	"archive/zip"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/fakehttp"
)

// CHAOS-14030: a report member the archive parse drops at the compression-ratio
// guard or past the entry cap used to leave no cause and no count, so the run
// read reports_complete=true while a report was lost.

const silentSkipJUnit = `<testsuite name="s"><testcase classname="a" name="b"/></testsuite>`

type silentSkipMember struct{ name, body string }

func silentSkipZip(t *testing.T, members []silentSkipMember) []byte {
	t.Helper()
	var buffer bytes.Buffer
	writer := zip.NewWriter(&buffer)
	for _, m := range members {
		w, err := writer.Create(m.name)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := w.Write([]byte(m.body)); err != nil {
			t.Fatal(err)
		}
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	return buffer.Bytes()
}

func parseSilentSkipArchive(t *testing.T, members []silentSkipMember) githubTestsReportRows {
	t.Helper()
	rows, err := parseGitHubTestsArtifact(
		silentSkipZip(t, members), "artifact-1", "c7198fbc-1945-3717-05d8-eb78866b4e79", "9001", "org-a",
		nil, nil, githubTestsDoctypeNow,
	)
	if err != nil {
		t.Fatal(err)
	}
	return rows
}

func assertSilentSkipRecorded(t *testing.T, rows githubTestsReportRows, cause string, count int) {
	t.Helper()
	incomplete, optional := rows.optionalIncomplete()
	want := GitHubTestsIncomplete{Component: githubTestsReportMemberComponent, Cause: cause, Count: count}
	if !optional || len(incomplete) != 1 || incomplete[0] != want {
		t.Fatalf("incomplete=%+v optional=%t, want [%+v]", incomplete, optional, want)
	}
	if len(rows.SkippedArtifacts) != count {
		t.Fatalf("skipped artifact markers=%d, want %d", len(rows.SkippedArtifacts), count)
	}
	for _, marker := range rows.SkippedArtifacts {
		if marker.Cause != cause {
			t.Fatalf("marker cause=%q, want %q", marker.Cause, cause)
		}
	}
	if !githubTestsIncompleteInVocabulary(want) {
		t.Fatalf("%s missing from the incomplete vocabulary", cause)
	}
	if githubTestsBlocksWatermark(incomplete, rows.SkippedArtifacts, 0, nil, nil) {
		t.Fatalf("%s is deterministic and must advance the watermark", cause)
	}
}

func TestGitHubTestsArchiveRecordsReportOverCompressionRatio(t *testing.T) {
	bomb := silentSkipJUnit + strings.Repeat(" ", 256<<10)
	rows := parseSilentSkipArchive(t, []silentSkipMember{{"bomb.xml", bomb}, {"ok.xml", silentSkipJUnit}})
	if len(rows.Suites) != 1 {
		t.Fatalf("suites=%d, want the healthy report still parsed", len(rows.Suites))
	}
	assertSilentSkipRecorded(t, rows, githubTestsCompressionRatioCause, 1)
}

func TestGitHubTestsArchiveRecordsReportMembersCutByEntryCap(t *testing.T) {
	members := make([]silentSkipMember, 0, githubTestsMaxArchiveEntries+2)
	for i := 0; i < githubTestsMaxArchiveEntries; i++ {
		members = append(members, silentSkipMember{fmt.Sprintf("n%04d.txt", i), "x"})
	}
	members = append(members,
		silentSkipMember{"tail-a.xml", silentSkipJUnit},
		silentSkipMember{"tail-b.info", "SF:a.go\nDA:1,1\nend_of_record\n"},
		silentSkipMember{"tail-c.txt", "x"},
	)
	rows := parseSilentSkipArchive(t, members)
	assertSilentSkipRecorded(t, rows, githubTestsArchiveEntryCapCause, 2)
}

func TestGitHubTestsArchiveEntryCapCutOfNonReportsRecordsNothing(t *testing.T) {
	members := make([]silentSkipMember, 0, githubTestsMaxArchiveEntries+2)
	for i := 0; i < githubTestsMaxArchiveEntries; i++ {
		members = append(members, silentSkipMember{fmt.Sprintf("n%04d.txt", i), "x"})
	}
	members = append(members, silentSkipMember{"tail.txt", "x"}, silentSkipMember{"tail.json", "{}"}, silentSkipMember{"dir/", ""})
	rows := parseSilentSkipArchive(t, members)
	if incomplete, optional := rows.optionalIncomplete(); !optional || len(incomplete) != 0 || rows.Skipped != 0 || len(rows.SkippedArtifacts) != 0 {
		t.Fatalf("incomplete=%+v skipped=%d markers=%d, want none", incomplete, rows.Skipped, len(rows.SkippedArtifacts))
	}
}

// The chunked route is the production path: an over-ratio report member must reach
// the unit's incomplete[], flip reports_complete, and advance (not withhold) the watermark.
func TestGitHubTestsChunkedRouteRecordsOverRatioReportMember(t *testing.T) {
	doer := &githubTestsCorruptArtifactDoer{
		t: t, artifacts: 1, corrupt: map[int]bool{},
		extraMembers: map[string]string{"bomb.xml": silentSkipJUnit + strings.Repeat(" ", 256<<10)},
	}
	walk, err := walkGitHubTestsChunksResult(t, githubTestsClient(t, fakehttp.Client(doer)), 4)
	if err != nil {
		t.Fatal(err)
	}
	if doer.archiveRequests != 1 || walk.cursor.Phase != "done" || walk.cursor.Suites != 1 {
		t.Fatalf("archives=%d phase=%q suites=%d, want 1/done/1", doer.archiveRequests, walk.cursor.Phase, walk.cursor.Suites)
	}
	if complete, ok := walk.final.Result["reports_complete"].(bool); !ok || complete {
		t.Fatalf("reports_complete=%v, want false", walk.final.Result["reports_complete"])
	}
	observation := githubTestsSkipObservation(t, walk.cursor.Incomplete, githubTestsReportMemberComponent)
	if observation.Cause != githubTestsCompressionRatioCause || observation.Count != 1 {
		t.Fatalf("observation=%+v, want %s count=1", observation, githubTestsCompressionRatioCause)
	}
	if walk.final.Watermark == nil {
		t.Fatal("compression_ratio is deterministic and must advance the watermark")
	}
}

// silentSkipArchives are the two archives of the size-guard skips, each beside a healthy lcov report.
func silentSkipArchives(t *testing.T) map[string][]byte {
	t.Helper()
	cut := make([]silentSkipMember, 0, githubTestsMaxArchiveEntries+2)
	cut = append(cut, silentSkipMember{"lcov.info", githubTestsLCOVFixture})
	for i := 1; i < githubTestsMaxArchiveEntries; i++ {
		cut = append(cut, silentSkipMember{fmt.Sprintf("n%04d.txt", i), "x"})
	}
	cut = append(cut, silentSkipMember{"tail.xml", silentSkipJUnit})
	return map[string][]byte{
		githubTestsCompressionRatioCause: silentSkipZip(t, []silentSkipMember{
			{"lcov.info", githubTestsLCOVFixture}, {"bomb.xml", silentSkipJUnit + strings.Repeat(" ", 256<<10)},
		}),
		githubTestsArchiveEntryCapCause: silentSkipZip(t, cut),
	}
}

// CHAOS-14030 round 1, P1: GitLab shares the archive parse and treats any skip as fatal, so an archive it accepted before
// (a healthy coverage report beside a guard-skipped member) must still be accepted, with the loss recorded as GitLab's own
// report_member_dropped truncation, which advances.
func TestGitLabTestsAcceptsArchiveWithSizeGuardSkippedMember(t *testing.T) {
	for cause, archive := range silentSkipArchives(t) {
		t.Run(cause, func(t *testing.T) {
			claim := nativeTestClaim("gitlab", "tests")
			pipelines := gitLabAllRefPipelines()[1:2]
			pipelines[0].ArtifactJobs = []gitLabAllRefArtifactJob{{ID: "21"}}
			doer := &gitLabAllRefDoer{t: t, archive: archive, pipelines: pipelines}
			walk, _ := gitLabAllRefCollectChunks(t, doer, claim)
			if walk.err != nil {
				t.Fatalf("an archive main accepts failed the claim: %v", walk.err)
			}
			if !slices.Contains(doer.requests, "artifact 21") {
				t.Fatalf("requests %v: the artifact was not read", doer.requests)
			}
			if walk.final.Watermark == nil {
				t.Fatal("watermark withheld")
			}
			if walk.final.Result["coverage_complete"] != false || !slices.Equal(gitLabAllRefTruncated(walk.final), []string{gitLabReportMemberDroppedComponent}) {
				t.Fatalf("result = %v", walk.final.Result)
			}
			if !gitLabTestsTruncationKnown(gitLabReportMemberDroppedComponent) || gitLabTestsBlocksWatermark([]string{gitLabReportMemberDroppedComponent}) {
				t.Fatal("report_member_dropped must be known and advance")
			}
		})
	}
}

// Both GitHub routes carry the guard skips the same way: cause counted, reports_complete false, the markers durable, and the
// watermark advancing.
func TestGitHubTestsBothRoutesAdvanceOverSizeGuardSkips(t *testing.T) {
	for cause, archive := range silentSkipArchives(t) {
		t.Run(cause+"/direct", func(t *testing.T) {
			doer := &githubTestsRouteDoer{t: t, archive: archive}
			batch, err := (GitHubTestsRouteHandler{}).Collect(
				context.Background(), nativeTestClaim("github", "tests"), providerfoundation.Credential{},
				githubTestsClient(t, fakehttp.Client(doer)), githubTestsDoctypeNow,
			)
			if err != nil {
				t.Fatal(err)
			}
			if batch.Result["reports_complete"] != false {
				t.Fatalf("reports_complete=%v", batch.Result["reports_complete"])
			}
			incomplete, _ := batch.Result["incomplete"].([]GitHubTestsIncomplete)
			if len(incomplete) != 1 || incomplete[0].Cause != cause || incomplete[0].Count != 1 {
				t.Fatalf("incomplete=%+v, want %s x1", incomplete, cause)
			}
			markers, _ := batch.Result["skipped_artifacts"].([]GitHubTestsSkippedArtifact)
			if len(markers) != 1 || markers[0].Cause != cause {
				t.Fatalf("markers=%+v, want one %s marker", markers, cause)
			}
			if batch.Watermark == nil {
				t.Fatal("direct route withheld the watermark over a deterministic size-guard skip")
			}
		})
		t.Run(cause+"/chunked", func(t *testing.T) {
			doer := &githubTestsCorruptArtifactDoer{t: t, artifacts: 1, corrupt: map[int]bool{}, archive: archive}
			walk, err := walkGitHubTestsChunksResult(t, githubTestsClient(t, fakehttp.Client(doer)), 4)
			if err != nil {
				t.Fatal(err)
			}
			observation := githubTestsSkipObservation(t, walk.cursor.Incomplete, githubTestsReportMemberComponent)
			if observation.Cause != cause || observation.Count != 1 || len(walk.cursor.SkippedArtifacts) != 1 {
				t.Fatalf("observation=%+v markers=%+v, want %s x1 with one marker", observation, walk.cursor.SkippedArtifacts, cause)
			}
			if walk.final.Watermark == nil {
				t.Fatal("chunked route withheld the watermark")
			}
		})
	}
}

// CHAOS-14030 round 1, P2: a resumed walk that re-anchors reads the same artifact again; its guard skip must count once.
func TestGitHubTestsReanchorDoesNotCountSizeGuardSkipTwice(t *testing.T) {
	for cause, archive := range silentSkipArchives(t) {
		t.Run(cause, func(t *testing.T) {
			doer := &githubTestsCorruptArtifactDoer{t: t, artifacts: 1, corrupt: map[int]bool{}, archive: archive}
			client := githubTestsClient(t, fakehttp.Client(doer))
			claim := nativeTestClaim("github", "cicd")
			now := time.Date(2026, 8, 14, 12, 0, 0, 0, time.UTC)
			stop := errors.New("stop after the first skip checkpoint")
			var checkpoint githubTestsChunkCursor
			err := GitHubTestsRouteHandler{}.CollectChunks(context.Background(), claim, providerfoundation.Credential{}, client, now, "",
				func(emission ChunkRouteEmission) error {
					cursor, decodeErr := decodeGitHubTestsChunkCursor(emission.CursorAfter)
					if decodeErr == nil && len(cursor.SkippedArtifacts) > 0 {
						checkpoint = cursor
						return stop
					}
					return nil
				})
			if !errors.Is(err, stop) {
				t.Fatalf("err=%v, want the walk stopped at the checkpoint holding the skip", err)
			}
			// The listing moved under the resume: the walk re-anchors to the start of the page and reads the artifact again.
			checkpoint.Phase, checkpoint.Index = "artifacts", 0
			checkpoint.NextURL, checkpoint.Relist = "", 0
			encoded, encodeErr := encodeGitHubTestsChunkCursor(checkpoint)
			if encodeErr != nil {
				t.Fatal(encodeErr)
			}
			var final githubTestsChunkCursor
			resumeErr := GitHubTestsRouteHandler{}.CollectChunks(context.Background(), claim, providerfoundation.Credential{}, client, now, encoded,
				func(emission ChunkRouteEmission) error {
					if cursor, decodeErr := decodeGitHubTestsChunkCursor(emission.CursorAfter); decodeErr == nil {
						final = cursor
					}
					return nil
				})
			if resumeErr != nil {
				t.Fatal(resumeErr)
			}
			if doer.archiveRequests < 2 {
				t.Fatalf("archive downloads=%d: the artifact was not read again, so the cell proves nothing", doer.archiveRequests)
			}
			observation := githubTestsSkipObservation(t, final.Incomplete, githubTestsReportMemberComponent)
			if observation.Cause != cause || observation.Count != 1 || len(final.SkippedArtifacts) != 1 {
				t.Fatalf("observation=%+v markers=%d, want %s counted once", observation, len(final.SkippedArtifacts), cause)
			}
		})
	}
}

// CHAOS-14030 round 1, P2: a checkpoint written before the count ledger holds every marker; trimming the sample to the cap
// must keep the count those markers prove, or the unit withholds the watermark it advanced on before.
func TestGitHubTestsTrimmedPreLedgerSampleKeepsItsProvenCount(t *testing.T) {
	var cursor githubTestsChunkCursor
	for i := 0; i < githubTestsMaxSkippedArtifactRecords+1; i++ {
		cursor.SkippedArtifacts = append(cursor.SkippedArtifacts, GitHubTestsSkippedArtifact{
			RunID: "1", ArtifactID: fmt.Sprint(i), Cause: githubTestsArtifactOversizedCause,
		})
	}
	cursor.Incomplete = []GitHubTestsIncomplete{{
		Component: githubTestsReportMemberComponent, Cause: githubTestsArtifactOversizedCause, Count: len(cursor.SkippedArtifacts),
	}}
	trimmed := normalizeLegacyGitHubTestsSkippedArtifacts(cursor)
	if len(trimmed.SkippedArtifacts) != githubTestsMaxSkippedArtifactRecords {
		t.Fatalf("sample=%d, want trimmed to the cap", len(trimmed.SkippedArtifacts))
	}
	if githubTestsBlocksWatermark(trimmed.Incomplete, trimmed.SkippedArtifacts, trimmed.SkippedArtifactsOverflow, trimmed.SkippedArtifactCauseOverflow, trimmed.SkippedArtifactCauseCount) {
		t.Fatalf("trimming proof away withheld the watermark: overflow=%d count=%v", trimmed.SkippedArtifactsOverflow, trimmed.SkippedArtifactCauseCount)
	}
	// A ledger that already counts more is kept, not lowered.
	cursor.SkippedArtifactCauseCount = map[string]int{githubTestsArtifactOversizedCause: 40}
	if got := normalizeLegacyGitHubTestsSkippedArtifacts(cursor).SkippedArtifactCauseCount[githubTestsArtifactOversizedCause]; got != 40 {
		t.Fatalf("ledger=%d, want the greater existing 40", got)
	}
}

// CHAOS-14030 round 2, P1: a re-anchored walk reads the artifact again. The unit counts do not take the guard skip twice, but the
// run's verdict row must still carry its loss: the replay row must equal the first (a complete replay row with a higher version
// would outrank the partial one in TestOps).
func TestGitHubTestsReanchorKeepsRunVerdictLoss(t *testing.T) {
	type verdict struct {
		Complete        bool     `json:"complete"`
		Reasons         []string `json:"reasons"`
		ReportsExpected int64    `json:"reports_expected"`
		ReportsAdmitted int64    `json:"reports_admitted"`
	}
	verdicts := func(t *testing.T, batch CompleteRouteBatch) []verdict {
		t.Helper()
		var rows []verdict
		for _, effect := range batch.Effects {
			if effect.Destination != testOpsRunReportCompletenessDestination {
				continue
			}
			for _, raw := range effect.Rows {
				var row verdict
				if err := json.Unmarshal(raw, &row); err != nil {
					t.Fatal(err)
				}
				rows = append(rows, row)
			}
		}
		return rows
	}
	cells := []struct {
		name   string
		doer   func(t *testing.T) *githubTestsCorruptArtifactDoer
		reason string
	}{
		{"compression_ratio/healthy_sibling", func(t *testing.T) *githubTestsCorruptArtifactDoer {
			return &githubTestsCorruptArtifactDoer{t: t, artifacts: 1, corrupt: map[int]bool{},
				extraMembers: map[string]string{"bomb.xml": silentSkipJUnit + strings.Repeat(" ", 256<<10)}}
		}, githubTestsCompressionRatioCause},
		{"archive_entry_cap/healthy_sibling", func(t *testing.T) *githubTestsCorruptArtifactDoer {
			return &githubTestsCorruptArtifactDoer{t: t, artifacts: 1, corrupt: map[int]bool{}, archive: silentSkipArchives(t)[githubTestsArchiveEntryCapCause]}
		}, githubTestsArchiveEntryCapCause},
		{"compression_ratio/only_source", func(t *testing.T) *githubTestsCorruptArtifactDoer {
			return &githubTestsCorruptArtifactDoer{t: t, artifacts: 1, corrupt: map[int]bool{},
				archive: silentSkipZip(t, []silentSkipMember{{"bomb.xml", silentSkipJUnit + strings.Repeat(" ", 256<<10)}})}
		}, githubTestsCompressionRatioCause},
		{"archive_entry_cap/only_source", func(t *testing.T) *githubTestsCorruptArtifactDoer {
			members := make([]silentSkipMember, 0, githubTestsMaxArchiveEntries+1)
			for i := 0; i < githubTestsMaxArchiveEntries; i++ {
				members = append(members, silentSkipMember{fmt.Sprintf("n%04d.txt", i), "x"})
			}
			members = append(members, silentSkipMember{"tail.xml", silentSkipJUnit})
			return &githubTestsCorruptArtifactDoer{t: t, artifacts: 1, corrupt: map[int]bool{}, archive: silentSkipZip(t, members)}
		}, githubTestsArchiveEntryCapCause},
		{"control_no_guard_skip", func(t *testing.T) *githubTestsCorruptArtifactDoer {
			return &githubTestsCorruptArtifactDoer{t: t, artifacts: 1, corrupt: map[int]bool{}}
		}, ""},
	}
	for _, cell := range cells {
		t.Run(cell.name, func(t *testing.T) {
			doer := cell.doer(t)
			client := githubTestsClient(t, fakehttp.Client(doer))
			claim := nativeTestClaim("github", "cicd")
			now := time.Date(2026, 8, 14, 12, 0, 0, 0, time.UTC)
			stop := errors.New("stop after the run's verdict")
			var first []verdict
			var checkpoint githubTestsChunkCursor
			err := GitHubTestsRouteHandler{}.CollectChunks(context.Background(), claim, providerfoundation.Credential{}, client, now, "",
				func(emission ChunkRouteEmission) error {
					if rows := verdicts(t, emission.Batch); len(rows) > 0 {
						first = rows
						cursor, decodeErr := decodeGitHubTestsChunkCursor(emission.CursorAfter)
						if decodeErr != nil {
							t.Fatal(decodeErr)
						}
						checkpoint = cursor
						return stop
					}
					return nil
				})
			if !errors.Is(err, stop) || len(first) != 1 {
				t.Fatalf("err=%v first=%+v, want one verdict row from the first read", err, first)
			}
			if cell.reason != "" && (first[0].Complete || !slices.Equal(first[0].Reasons, []string{cell.reason})) {
				t.Fatalf("first verdict=%+v, want partial with %s", first[0], cell.reason)
			}
			checkpoint.Phase, checkpoint.Index = "artifacts", 0
			checkpoint.NextURL, checkpoint.Relist = "", 0
			encoded, encodeErr := encodeGitHubTestsChunkCursor(checkpoint)
			if encodeErr != nil {
				t.Fatal(encodeErr)
			}
			var replay []verdict
			resumeErr := GitHubTestsRouteHandler{}.CollectChunks(context.Background(), claim, providerfoundation.Credential{}, client, now, encoded,
				func(emission ChunkRouteEmission) error {
					replay = append(replay, verdicts(t, emission.Batch)...)
					return nil
				})
			if resumeErr != nil {
				t.Fatal(resumeErr)
			}
			if doer.archiveRequests < 2 {
				t.Fatalf("archive downloads=%d: the artifact was not read again", doer.archiveRequests)
			}
			if len(replay) != 1 || !reflect.DeepEqual(replay[0], first[0]) {
				t.Fatalf("replay verdict=%+v, want it equal to the first %+v", replay, first[0])
			}
		})
	}
}
