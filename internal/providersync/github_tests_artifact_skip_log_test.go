package providersync

import (
	"log/slog"
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
