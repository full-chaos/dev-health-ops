package providersync

import (
	"archive/zip"
	"bytes"
	"fmt"
	"strings"
	"testing"

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
