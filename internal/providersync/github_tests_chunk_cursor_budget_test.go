package providersync

import (
	"strings"
	"testing"
)

// TestGitHubTestsChunkCursorWorstCaseStaysWithinBudget pins the CHAOS-4592
// codex review finding (P1, on merged CHAOS-4588/CHAOS-4591 code): a record
// serializes far larger once GitHubTestsSkippedArtifact.Name exists than the
// estimate githubTestsMaxSkippedArtifactRecords was originally sized
// against, so githubTestsMaxSkippedArtifactRecords's OLD cap (20) combined
// with githubTestsMaxArtifactNameBytes's OLD cap (48) alone encoded to
// ~4.1KB -- already exceeding the WHOLE cursor's maxChunkCursorBytes (4KiB)
// budget before Repo/NextURL/Incomplete/ExcludedArtifactSample/everything
// else on githubTestsChunkCursor is added. A heavy-skip-volume unit's
// checkpoint write could fail ErrChunkCheckpointConflict outright --
// turning a documented, harmless truncation (records beyond the cap
// collapse into SkippedArtifactsOverflow) into an undocumented, harmful one
// (the WHOLE cursor becomes unencodable, and unrelated committed progress
// is lost).
//
// This constructs a cursor with every field at a realistic maximum --
// EVERY (component, cause) pair in the closed incomplete vocabulary,
// githubTestsMaxSkippedArtifactRecords records each carrying a
// githubTestsMaxArtifactNameBytes-length name and int64-max size/cap
// fields, githubTestsMaxExcludedArtifactSampleRecords excluded samples, and
// a long realistic paginated NextURL -- and asserts the encoded cursor both
// succeeds and leaves real margin under the budget, not just enough to pass
// today. RED on the pre-fix caps (20 records, 48-byte names): encoding
// fails ErrChunkCheckpointConflict outright.
func TestGitHubTestsChunkCursorWorstCaseStaysWithinBudget(t *testing.T) {
	seen, unreadable := 6219, 6219
	cursor := githubTestsChunkCursor{
		Phase: "artifacts",
		NextURL: "https://api.github.com/repos/full-chaos/dev-health-ops/actions/runs/" +
			"33301167231/artifacts?per_page=100&page=42&branch=main",
		Index: 42, Pipelines: 999999, Jobs: 999999, Acceptance: 999999,
		Suites: 999999, Cases: 999999, Coverage: 999999,
		Requests: 999999, Pages: 999999, RunPages: 999999, ArtifactPages: 999999,
		Repo:                     "full-chaos/dev-health-ops",
		ArchivesSeen:             &seen,
		ArchivesUnreadable:       &unreadable,
		SkippedArtifactsOverflow: 999999,
		ExcludedNonReportSuffix:  999999,
		ExcludedNonReportPrefix:  999999,
	}

	// Every (component, cause) pair the closed vocabulary allows -- this
	// test automatically covers a future vocabulary addition, which is the
	// whole point of a worst-case budget test.
	for component, causes := range githubTestsIncompleteVocabulary {
		for cause := range causes {
			cursor.Incomplete = append(cursor.Incomplete, GitHubTestsIncomplete{
				Component: component, Cause: cause, Count: 999999,
			})
		}
	}

	name := strings.Repeat("x", githubTestsMaxArtifactNameBytes)
	causeOverflow := map[string]bool{}
	for i := 0; i < githubTestsMaxSkippedArtifactRecords; i++ {
		cursor.SkippedArtifacts = append(cursor.SkippedArtifacts, GitHubTestsSkippedArtifact{
			RunID: "33301167231234567", ArtifactID: "9729013072123456", Name: name,
			Cause: githubTestsUnreadableArchiveCause,
			// int64 max, the widest possible decimal rendering.
			SizeBytes: 9223372036854775807, CapBytes: 9223372036854775807,
		})
	}
	for _, cause := range []string{
		githubTestsArtifactOversizedCause, githubTestsArtifactUnavailableCause,
		githubTestsUnreadableArchiveCause, githubTestsMalformedCause, githubTestsUnreadableCause,
	} {
		causeOverflow[cause] = true
	}
	cursor.SkippedArtifactCauseOverflow = causeOverflow

	for i := 0; i < githubTestsMaxExcludedArtifactSampleRecords; i++ {
		cursor.ExcludedArtifactSample = append(
			cursor.ExcludedArtifactSample, name+" (non_report_artifact_suffix)",
		)
	}

	encoded, err := encodeGitHubTestsChunkCursor(cursor)
	if err != nil {
		t.Fatalf(
			"worst-case cursor failed to encode: %v (a heavy-skip-volume unit's checkpoint write "+
				"would fail outright instead of degrading into SkippedArtifactsOverflow)",
			err,
		)
	}
	if len(encoded) > maxChunkCursorBytes {
		t.Fatalf("worst-case cursor is %d bytes, want <= %d (maxChunkCursorBytes)", len(encoded), maxChunkCursorBytes)
	}
	// A canary for margin erosion, not a hard correctness bound: a future
	// field addition that eats this margin should fail loudly here before it
	// ever gets close to the hard 4KiB limit in production.
	const wantMargin = 500
	if remaining := maxChunkCursorBytes - len(encoded); remaining < wantMargin {
		t.Fatalf(
			"worst-case cursor leaves only %d bytes of headroom under maxChunkCursorBytes, want >= %d -- "+
				"the byte budget this file's caps were sized against has eroded",
			remaining, wantMargin,
		)
	}
}
