package providersync

import (
	"encoding/json"
	"strconv"
	"strings"
	"testing"
)

// githubTestsRealisticCursorBase returns a githubTestsChunkCursor with every
// field EXCEPT SkippedArtifacts/SkippedArtifactCauseOverflow/
// SkippedArtifactCauseCount/ExcludedArtifactSample set to a realistic
// in-flight maximum: a long
// paginated NextURL, six-digit counters, and every (component, cause) pair
// the closed incomplete vocabulary allows -- automatically covering a future
// vocabulary addition, which is the whole point of a worst-case budget test.
// Shared by both tests in this file so their SkippedArtifacts shape is the
// only thing that differs between them.
func githubTestsRealisticCursorBase() githubTestsChunkCursor {
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
	for component, causes := range githubTestsIncompleteVocabulary {
		for cause := range causes {
			cursor.Incomplete = append(cursor.Incomplete, GitHubTestsIncomplete{
				Component: component, Cause: cause, Count: 999999,
			})
		}
	}
	return cursor
}

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
// githubTestsMaxSkippedArtifactRecords records each carrying a
// githubTestsMaxArtifactNameBytes-length name and int64-max size/cap
// fields, githubTestsMaxExcludedArtifactSampleRecords excluded samples, on
// top of githubTestsRealisticCursorBase -- and asserts the encoded cursor
// both succeeds and leaves real margin under the budget, not just enough to
// pass today. RED on the pre-fix caps (20 records, 48-byte names): encoding
// fails ErrChunkCheckpointConflict outright.
func TestGitHubTestsChunkCursorWorstCaseStaysWithinBudget(t *testing.T) {
	cursor := githubTestsRealisticCursorBase()

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

	causeCount := map[string]int{}
	for _, cause := range []string{
		githubTestsArtifactOversizedCause, githubTestsArtifactUnavailableCause,
		githubTestsUnreadableArchiveCause, githubTestsMalformedCause, githubTestsUnreadableCause,
	} {
		causeCount[cause] = 999999999
	}
	cursor.SkippedArtifactCauseCount = causeCount

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
	//
	// 500 -> 450 (codex review gate round 2, P1): SkippedArtifactCauseCount
	// (5 causes, int-max values) added ~10 bytes past the old threshold. The
	// worst case still leaves ~490 bytes (12%) of real headroom under
	// maxChunkCursorBytes -- comfortable, not merely passing.
	const wantMargin = 450
	if remaining := maxChunkCursorBytes - len(encoded); remaining < wantMargin {
		t.Fatalf(
			"worst-case cursor leaves only %d bytes of headroom under maxChunkCursorBytes, want >= %d -- "+
				"the byte budget this file's caps were sized against has eroded",
			remaining, wantMargin,
		)
	}
}

// TestGitHubTestsChunkCursorNormalizesLegacySkippedArtifactsOnDecode pins the
// CHAOS-4592 codex review finding (P1, round 5) that shrinking
// githubTestsMaxSkippedArtifactRecords/githubTestsMaxArtifactNameBytes only
// bounds a NEWLY appended record: a cursor a PRIOR binary version already
// wrote under the OLDER, larger caps decodes with its sample exactly as
// written -- up to 20 records with 48-byte names (and, for the
// artifact_oversized cause, size/cap fields) is legal JSON this binary's own
// json.Unmarshal has no reason to reject. Without normalizing that
// inherited sample down to the CURRENT bounded shape, an otherwise ordinary
// in-flight cursor (realistic NextURL, several incomplete observations --
// githubTestsRealisticCursorBase) can already sit over maxChunkCursorBytes
// on its own once the legacy sample is added back in, and the very next
// re-encode -- this attempt's own checkpoint write, unrelated to any new
// skip -- fails ErrChunkCheckpointConflict outright: during a rolling
// deploy, a unit resuming a pre-upgrade cursor loses its committed progress
// instead of degrading gracefully.
//
// Also carries the OLD githubTestsMaxExcludedArtifactSampleRecords (5)
// excluded-artifact-sample entries at the OLD 48-byte name bound (codex
// review round 5, P2, a third variant of the same class: "20 skipped-
// artifact markers and at least five excluded artifacts... about 4.45KB in
// a realistic case"). ExcludedArtifactSample's record COUNT never changed
// (still 5), and its entries are pre-formatted "name (reason)" strings
// decodeGitHubTestsChunkCursor does not re-truncate -- normalizing
// SkippedArtifacts alone must still leave enough margin to absorb these
// untouched, over-length entries too.
func TestGitHubTestsChunkCursorNormalizesLegacySkippedArtifactsOnDecode(t *testing.T) {
	const legacyRecords = 20           // the OLD githubTestsMaxSkippedArtifactRecords
	oldName := strings.Repeat("x", 48) // the OLD githubTestsMaxArtifactNameBytes
	legacyCursor := githubTestsRealisticCursorBase()
	for i := 0; i < legacyRecords; i++ {
		legacyCursor.SkippedArtifacts = append(legacyCursor.SkippedArtifacts, GitHubTestsSkippedArtifact{
			RunID: "33301167231234567", ArtifactID: strconv.Itoa(9729013072123456 + i),
			Name: oldName, Cause: githubTestsArtifactOversizedCause,
			SizeBytes: 9223372036854775807, CapBytes: 9223372036854775807,
		})
	}
	for i := 0; i < githubTestsMaxExcludedArtifactSampleRecords; i++ {
		legacyCursor.ExcludedArtifactSample = append(
			legacyCursor.ExcludedArtifactSample, oldName+" (non_report_artifact_suffix)",
		)
	}

	raw, err := json.Marshal(legacyCursor)
	if err != nil {
		t.Fatal(err)
	}
	// RED without normalization: exactly what a prior binary version could
	// legally have written, re-encoded verbatim, already exceeds budget --
	// this is the bug, not a contrived edge case.
	if len(raw) <= maxChunkCursorBytes {
		t.Fatalf(
			"premise failed: this legacy-shaped cursor must already exceed maxChunkCursorBytes on its "+
				"own (encoded=%d bytes) or this test proves nothing",
			len(raw),
		)
	}

	decoded, err := decodeGitHubTestsChunkCursor(string(raw))
	if err != nil {
		t.Fatalf("decode of a legacy-shaped cursor returned err=%v, want normalization not rejection", err)
	}

	if len(decoded.SkippedArtifacts) != githubTestsMaxSkippedArtifactRecords {
		t.Fatalf(
			"decoded sample has %d records, want trimmed to the current cap %d",
			len(decoded.SkippedArtifacts), githubTestsMaxSkippedArtifactRecords,
		)
	}
	for _, record := range decoded.SkippedArtifacts {
		if len(record.Name) > githubTestsMaxArtifactNameBytes+len("…") {
			t.Fatalf("decoded record name %q is %d bytes, want <= %d", record.Name, len(record.Name), githubTestsMaxArtifactNameBytes+len("…"))
		}
	}
	wantOverflow := 999999 + (legacyRecords - githubTestsMaxSkippedArtifactRecords)
	if decoded.SkippedArtifactsOverflow != wantOverflow {
		t.Fatalf(
			"SkippedArtifactsOverflow=%d, want %d -- trimmed records must be reflected, not silently dropped, "+
				"on top of whatever aggregate overflow the cursor already carried",
			decoded.SkippedArtifactsOverflow, wantOverflow,
		)
	}
	// The legacy sentinel must also fire: this cursor already had aggregate
	// overflow with no per-cause ledger before normalization ever ran.
	if !decoded.SkippedArtifactCauseOverflow[githubTestsLegacyReportOverflowSentinel] {
		t.Fatalf("legacy overflow did not stamp the legacy sentinel: %+v", decoded.SkippedArtifactCauseOverflow)
	}

	reencoded, err := encodeGitHubTestsChunkCursor(decoded)
	if err != nil {
		t.Fatalf(
			"normalized legacy cursor still fails to encode: %v (codex round 5, P2: the untouched "+
				"legacy-shaped ExcludedArtifactSample entries alone must not undo the SkippedArtifacts "+
				"normalization's margin)",
			err,
		)
	}
	if len(reencoded) > maxChunkCursorBytes {
		t.Fatalf("normalized legacy cursor is %d bytes, want <= %d (maxChunkCursorBytes)", len(reencoded), maxChunkCursorBytes)
	}
}

// TestGitHubTestsChunkCursorLegacyTrimDoesNotExcuseAnUnrelatedUnmarkedCause
// pins the CHAOS-4592 codex review finding (P1, round 6): a PRE-CHAOS-4394
// cursor can legally have exactly the old githubTestsMaxSkippedArtifactRecords
// (20) artifact_oversized markers with SkippedArtifactsOverflow==0 (it never
// exceeded the OLD cap) alongside an UNMARKED artifact_unavailable
// observation -- that binary predates CHAOS-4394 entirely, so it never wrote
// a marker for artifact_unavailable at all, overflow or not. Trimming the 20
// oversized markers down to the CURRENT cap creates SkippedArtifactsOverflow
// > 0 for the first time, purely as an artifact of THIS migration. That
// migration-only overflow must not be read as generic proof covering ALL
// three original whole-artifact causes -- it is precise evidence about
// artifact_oversized specifically (the cause of the records actually
// dropped), and must leave the unrelated, still-completely-unmarked
// artifact_unavailable observation blocking exactly as it always has.
func TestGitHubTestsChunkCursorLegacyTrimDoesNotExcuseAnUnrelatedUnmarkedCause(t *testing.T) {
	const legacyRecords = 20 // the OLD githubTestsMaxSkippedArtifactRecords
	oldName := strings.Repeat("x", 48)
	cursor := githubTestsChunkCursor{
		Phase: "done", Repo: "acme/api", Requests: 3, Pages: 2, NextURL: "x",
		Incomplete: []GitHubTestsIncomplete{
			{Component: githubTestsReportMemberComponent, Cause: githubTestsArtifactOversizedCause, Count: legacyRecords},
			{Component: githubTestsReportMemberComponent, Cause: githubTestsArtifactUnavailableCause, Count: 1},
		},
		// SkippedArtifactsOverflow is deliberately absent (0): a pre-CHAOS-4394
		// binary never overflowed its OWN cap (20 == 20, exactly at the old
		// bound) and never wrote a marker OR an overflow count for
		// artifact_unavailable at all.
	}
	for i := 0; i < legacyRecords; i++ {
		cursor.SkippedArtifacts = append(cursor.SkippedArtifacts, GitHubTestsSkippedArtifact{
			// Cause deliberately OMITTED (codex review gate round 3, P2): a
			// genuinely pre-CHAOS-4394 marker decodes with Cause=="" because
			// that field did not exist on the binary that wrote it -- SizeBytes
			// is the only era-appropriate signal, resolved through
			// githubTestsSkippedArtifactCause's fallback. A test that manufactures
			// these with Cause already set never exercises that fallback path at
			// all, so it cannot catch a regression in it (which is exactly what
			// happened: normalizeLegacyGitHubTestsSkippedArtifacts used
			// record.Cause directly instead of githubTestsSkippedArtifactCause(record),
			// attributing migration overflow to the empty string).
			RunID: "33301167231234567", ArtifactID: strconv.Itoa(9729013072123456 + i),
			Name:      oldName,
			SizeBytes: 9223372036854775807, CapBytes: 9223372036854775807,
		})
	}

	raw, err := json.Marshal(cursor)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := decodeGitHubTestsChunkCursor(string(raw))
	if err != nil {
		t.Fatalf("decode returned err=%v, want normalization not rejection", err)
	}
	if decoded.SkippedArtifactsOverflow <= 0 {
		t.Fatalf(
			"premise failed: normalizing 20 records down to the current cap must produce overflow>0 "+
				"(got %d) or this test proves nothing",
			decoded.SkippedArtifactsOverflow,
		)
	}
	// The migration-induced overflow must be attributed to the cause it
	// actually came from, never to the unrelated cause.
	if !decoded.SkippedArtifactCauseOverflow[githubTestsArtifactOversizedCause] {
		t.Fatalf("migration overflow was not attributed to artifact_oversized: %+v", decoded.SkippedArtifactCauseOverflow)
	}
	if decoded.SkippedArtifactCauseOverflow[githubTestsArtifactUnavailableCause] {
		t.Fatalf("migration overflow was wrongly attributed to the unrelated artifact_unavailable cause: %+v", decoded.SkippedArtifactCauseOverflow)
	}

	claim := nativeTestClaim("github", "cicd")
	batch, err := githubTestsFinalMetadataBatch(claim, decoded)
	if err != nil {
		t.Fatal(err)
	}
	if batch.Watermark != nil {
		t.Fatalf(
			"watermark=%v, want nil -- migration-induced overflow for artifact_oversized must not "+
				"excuse the completely unrelated, never-marked artifact_unavailable observation",
			batch.Watermark,
		)
	}
	mustCompareGitHubTestsCompletionOK(t, claim, batch)
}
