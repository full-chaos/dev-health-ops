package providersync

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"
)

// jsonRoundTripCompletionResult re-encodes a completion result the way every
// durable recovery plane does: through JSON. Typed Go values come back as the
// generic shapes json.Unmarshal produces — bool stays bool, int becomes
// float64, and []GitHubTestsIncomplete becomes []any of map[string]any.
//
// applyGitHubWorkItemsIncompletePolicy was retrofitted to accept exactly this
// shape (see its doc comment); these tests pin the same contract onto the
// production comparator so a decoded replay of a healthy github tests/cicd
// completion is never refused.
func jsonRoundTripCompletionResult(t *testing.T, result map[string]any) map[string]any {
	t.Helper()
	encoded, err := json.Marshal(result)
	if err != nil {
		t.Fatal(err)
	}
	var decoded map[string]any
	if err := json.Unmarshal(encoded, &decoded); err != nil {
		t.Fatal(err)
	}
	return decoded
}

func decodedGitHubTestsBatch(claim Claim, incomplete []GitHubTestsIncomplete) CompleteRouteBatch {
	var watermark *time.Time
	if len(incomplete) == 0 {
		watermark = claim.BeforeAt
	}
	return CompleteRouteBatch{
		Watermark: watermark,
		Result: map[string]any{
			"pipeline_runs_synced": 0, "job_runs_synced": 0,
			"acceptance_checks_synced": 0, "test_suites_synced": 0,
			"test_cases_synced": 0, "coverage_snapshots_synced": 0,
			"repo":             "acme/api",
			"reports_complete": len(incomplete) == 0,
			"reports_skipped":  githubTestsIncompleteCount(incomplete),
			"incomplete":       incomplete,
		},
	}
}

func TestProductionContractComparatorAcceptsDecodedGitHubTestsCompletion(t *testing.T) {
	claim := nativeTestClaim("github", "tests")
	batch := decodedGitHubTestsBatch(claim, make([]GitHubTestsIncomplete, 0))
	batch.Result = jsonRoundTripCompletionResult(t, batch.Result)
	comparison, err := (ProductionContractComparator{}).CompareCompleteRoute(
		context.Background(), claim, batch,
	)
	if err != nil || !comparison.Match {
		t.Fatalf("decoded complete result refused: comparison=%+v error=%v", comparison, err)
	}
}

func TestProductionContractComparatorAcceptsDecodedGitHubTestsIncomplete(t *testing.T) {
	claim := nativeTestClaim("github", "tests")
	batch := decodedGitHubTestsBatch(claim, []GitHubTestsIncomplete{{
		Component: "report_member", Cause: "malformed", Count: 2,
	}})
	batch.Result = jsonRoundTripCompletionResult(t, batch.Result)
	comparison, err := (ProductionContractComparator{}).CompareCompleteRoute(
		context.Background(), claim, batch,
	)
	if err != nil || !comparison.Match {
		t.Fatalf("decoded incomplete result refused: comparison=%+v error=%v", comparison, err)
	}
	// The completion invariants must still bind on the decoded shape: an
	// incomplete inventory advancing the watermark stays an error.
	held := batch
	held.Watermark = claim.BeforeAt
	if _, err := (ProductionContractComparator{}).CompareCompleteRoute(
		context.Background(), claim, held,
	); !errors.Is(err, ErrInvalidConfiguration) {
		t.Fatalf("decoded incomplete watermark accepted: error=%v", err)
	}
}

func TestProductionContractComparatorDecodedShapeStillFailsClosed(t *testing.T) {
	claim := nativeTestClaim("github", "tests")
	for name, mutate := range map[string]func(result map[string]any){
		"missing reports_complete": func(result map[string]any) {
			delete(result, "reports_complete")
		},
		"null reports_complete": func(result map[string]any) {
			result["reports_complete"] = nil
		},
		"missing reports_skipped": func(result map[string]any) {
			delete(result, "reports_skipped")
		},
		"fractional reports_skipped": func(result map[string]any) {
			result["reports_skipped"] = 0.5
		},
		"string reports_skipped": func(result map[string]any) {
			result["reports_skipped"] = "0"
		},
		"missing incomplete": func(result map[string]any) {
			delete(result, "incomplete")
		},
		"null incomplete": func(result map[string]any) {
			result["incomplete"] = nil
		},
		"unknown field in incomplete entry": func(result map[string]any) {
			result["incomplete"] = []any{map[string]any{
				"component": "report_member", "cause": "malformed",
				"count": 1, "member": "junit.xml",
			}}
			result["reports_complete"] = false
			result["reports_skipped"] = 1
		},
		"non-array incomplete": func(result map[string]any) {
			result["incomplete"] = map[string]any{}
		},
	} {
		batch := decodedGitHubTestsBatch(claim, make([]GitHubTestsIncomplete, 0))
		mutate(batch.Result)
		batch.Result = jsonRoundTripCompletionResult(t, batch.Result)
		if name == "unknown field in incomplete entry" {
			batch.Watermark = nil
		}
		if _, err := (ProductionContractComparator{}).CompareCompleteRoute(
			context.Background(), claim, batch,
		); !errors.Is(err, ErrInvalidConfiguration) {
			t.Fatalf("%s: decoded malformed result accepted: error=%v", name, err)
		}
	}
}

// A typed-nil slice marshals to JSON null, so the comparator refuses it the
// same way every durable optional-evidence reader has since CHAOS-3940. The
// contract is writer-side: every producer must emit a non-nil (possibly
// empty) slice, and the chunked-route test below holds the one producer that
// used to emit typed nil to that contract.
func TestProductionContractComparatorRejectsTypedNilIncomplete(t *testing.T) {
	claim := nativeTestClaim("github", "tests")
	batch := decodedGitHubTestsBatch(claim, make([]GitHubTestsIncomplete, 0))
	batch.Result["incomplete"] = []GitHubTestsIncomplete(nil)
	if _, err := (ProductionContractComparator{}).CompareCompleteRoute(
		context.Background(), claim, batch,
	); !errors.Is(err, ErrInvalidConfiguration) {
		t.Fatalf("typed-nil incomplete accepted: error=%v", err)
	}
}

// A clean chunked run never appends to the cursor's Incomplete slice, and a
// resumed cursor decodes it from JSON with omitempty, so the field reaches
// final metadata as typed nil on every healthy unit. The terminal batch must
// still pass the production comparator, and its durable form must be [] —
// never null.
func TestGitHubTestsChunkedFinalMetadataSurvivesComparator(t *testing.T) {
	claim := nativeTestClaim("github", "tests")
	batch, err := githubTestsFinalMetadataBatch(claim, githubTestsChunkCursor{
		Phase: "done", Repo: "acme/api", Requests: 3, Pages: 2,
	})
	if err != nil {
		t.Fatal(err)
	}
	comparison, err := (ProductionContractComparator{}).CompareCompleteRoute(
		context.Background(), claim, batch,
	)
	if err != nil || !comparison.Match {
		t.Fatalf("clean chunked completion refused: comparison=%+v error=%v", comparison, err)
	}
	encoded, err := json.Marshal(batch.Result["incomplete"])
	if err != nil {
		t.Fatal(err)
	}
	if string(encoded) != "[]" {
		t.Fatalf("durable incomplete form=%s, want []", encoded)
	}
	if batch.Watermark == nil || !batch.Watermark.Equal(*claim.BeforeAt) {
		t.Fatalf("clean chunked completion watermark=%v", batch.Watermark)
	}
}

func TestGitHubTestsChunkedFinalMetadataIncompleteRunSurvivesComparator(t *testing.T) {
	claim := nativeTestClaim("github", "tests")
	batch, err := githubTestsFinalMetadataBatch(claim, githubTestsChunkCursor{
		Phase: "done", Repo: "acme/api", Requests: 3, Pages: 2,
		Incomplete: []GitHubTestsIncomplete{{
			Component: "report_member", Cause: "unreadable", Count: 1,
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	comparison, err := (ProductionContractComparator{}).CompareCompleteRoute(
		context.Background(), claim, batch,
	)
	if err != nil || !comparison.Match {
		t.Fatalf("incomplete chunked completion refused: comparison=%+v error=%v", comparison, err)
	}
	if batch.Watermark != nil {
		t.Fatalf("incomplete chunked completion advanced watermark=%v", batch.Watermark)
	}
}

// TestGitHubTestsChunkedFinalMetadataWithholdsOnLegacyCursorWithoutMarkers
// pins the upgrade-boundary guard codex review round 1 (P1, CHAOS-4394)
// required: a cursor checkpointed by the PRE-CHAOS-4394 binary can carry an
// artifact_unavailable/unreadable_archive count on Incomplete with NO
// GitHubTestsSkippedArtifact marker, because the old binary only ever wrote
// markers for artifact_oversized. Resuming such a cursor here must still
// withhold the watermark -- advancing it would promise a backfill-targeting
// marker that was never recorded, permanently.
func TestGitHubTestsChunkedFinalMetadataWithholdsOnLegacyCursorWithoutMarkers(t *testing.T) {
	claim := nativeTestClaim("github", "cicd")
	legacy := githubTestsChunkCursor{
		Phase: "done", Repo: "acme/api", Requests: 3, Pages: 2,
		Incomplete: []GitHubTestsIncomplete{{
			Component: githubTestsReportMemberComponent,
			Cause:     githubTestsArtifactUnavailableCause, Count: 1,
		}},
		// No SkippedArtifacts, no SkippedArtifactsOverflow: exactly what a
		// pre-CHAOS-4394 cursor decodes as.
	}
	batch, err := githubTestsFinalMetadataBatch(claim, legacy)
	if err != nil {
		t.Fatal(err)
	}
	if batch.Watermark != nil {
		t.Fatalf(
			"a legacy cursor with a report_member skip but no durable marker advanced watermark=%v, want nil",
			batch.Watermark,
		)
	}
	// RED before codex review round 3, P1: validateGitHubTestsCompletion did
	// not know about the legacy-marker guard, so it independently re-derived
	// "should this block" from incomplete ALONE, disagreed with the Watermark
	// this closure just computed, and rejected the batch outright -- turning
	// an intended safe withhold into ErrInvalidConfiguration, which
	// jobruntime.Retryable would burn the unit's retry budget on repeatedly
	// rather than the harmless next-window retry withholding is supposed to
	// be.
	mustCompareGitHubTestsCompletionOK(t, claim, batch)

	// The identical incomplete evidence, but with a marker attached (as this
	// binary always writes going forward), must advance -- proving the guard
	// keys on marker presence, not merely on having seen a legacy-shaped
	// cursor.
	withMarker := legacy
	withMarker.SkippedArtifacts = []GitHubTestsSkippedArtifact{{
		RunID: "42", ArtifactID: "7", Cause: githubTestsArtifactUnavailableCause,
	}}
	batch, err = githubTestsFinalMetadataBatch(claim, withMarker)
	if err != nil {
		t.Fatal(err)
	}
	if batch.Watermark == nil || !batch.Watermark.Equal(*claim.BeforeAt) {
		t.Fatalf("watermark=%v, want %v once a durable marker backs the skip", batch.Watermark, claim.BeforeAt)
	}
	mustCompareGitHubTestsCompletionOK(t, claim, batch)

	// The overflow-only case: the marker sample was capped but the overflow
	// counter proves this binary's code path actually ran.
	withOverflow := legacy
	withOverflow.SkippedArtifactsOverflow = 1
	batch, err = githubTestsFinalMetadataBatch(claim, withOverflow)
	if err != nil {
		t.Fatal(err)
	}
	if batch.Watermark == nil || !batch.Watermark.Equal(*claim.BeforeAt) {
		t.Fatalf("watermark=%v, want %v once overflow proves this binary wrote the evidence", batch.Watermark, claim.BeforeAt)
	}
	mustCompareGitHubTestsCompletionOK(t, claim, batch)

	// RED before codex review round 2, P1: a legacy cursor mixing a MARKED
	// cause (artifact_oversized, which the old binary always marked) with an
	// UNMARKED one (artifact_unavailable, which it never did) must still
	// withhold. Round 1's guard checked only "does SkippedArtifacts have ANY
	// entry", which this case satisfies vacuously via the oversized marker
	// alone, advancing on the unmarked artifact_unavailable count anyway.
	mixedMarkerCoverage := githubTestsChunkCursor{
		Phase: "done", Repo: "acme/api", Requests: 3, Pages: 2,
		Incomplete: []GitHubTestsIncomplete{
			{Component: githubTestsReportMemberComponent, Cause: githubTestsArtifactOversizedCause, Count: 1},
			{Component: githubTestsReportMemberComponent, Cause: githubTestsArtifactUnavailableCause, Count: 1},
		},
		SkippedArtifacts: []GitHubTestsSkippedArtifact{{
			RunID: "42", ArtifactID: "7", Cause: githubTestsArtifactOversizedCause,
			SizeBytes: 200, CapBytes: 100,
		}},
	}
	batch, err = githubTestsFinalMetadataBatch(claim, mixedMarkerCoverage)
	if err != nil {
		t.Fatal(err)
	}
	if batch.Watermark != nil {
		t.Fatalf(
			"watermark=%v, want nil -- artifact_unavailable has no marker even though artifact_oversized does",
			batch.Watermark,
		)
	}
	mustCompareGitHubTestsCompletionOK(t, claim, batch)

	// The same mix, but BOTH causes marked, must advance.
	mixedFullyMarked := mixedMarkerCoverage
	mixedFullyMarked.SkippedArtifacts = append(
		append([]GitHubTestsSkippedArtifact{}, mixedMarkerCoverage.SkippedArtifacts...),
		GitHubTestsSkippedArtifact{RunID: "43", ArtifactID: "8", Cause: githubTestsArtifactUnavailableCause},
	)
	batch, err = githubTestsFinalMetadataBatch(claim, mixedFullyMarked)
	if err != nil {
		t.Fatal(err)
	}
	if batch.Watermark == nil || !batch.Watermark.Equal(*claim.BeforeAt) {
		t.Fatalf("watermark=%v, want %v once every cause present has its own marker", batch.Watermark, claim.BeforeAt)
	}
	mustCompareGitHubTestsCompletionOK(t, claim, batch)

	// codex review round 3, P2: a marker persisted BEFORE the Cause field
	// existed decodes with Cause == "" -- it must still be recognized as an
	// artifact_oversized marker via its SizeBytes, not treated as unmarked
	// and needlessly withheld.
	legacyOversizedMarker := githubTestsChunkCursor{
		Phase: "done", Repo: "acme/api", Requests: 3, Pages: 2,
		Incomplete: []GitHubTestsIncomplete{{
			Component: githubTestsReportMemberComponent,
			Cause:     githubTestsArtifactOversizedCause, Count: 1,
		}},
		SkippedArtifacts: []GitHubTestsSkippedArtifact{{
			RunID: "42", ArtifactID: "7", Cause: "", // pre-CHAOS-4394 shape
			SizeBytes: 200, CapBytes: 100,
		}},
	}
	batch, err = githubTestsFinalMetadataBatch(claim, legacyOversizedMarker)
	if err != nil {
		t.Fatal(err)
	}
	if batch.Watermark == nil || !batch.Watermark.Equal(*claim.BeforeAt) {
		t.Fatalf(
			"watermark=%v, want %v -- a pre-Cause-field oversized marker (SizeBytes>0, Cause=\"\") must still count",
			batch.Watermark, claim.BeforeAt,
		)
	}
	mustCompareGitHubTestsCompletionOK(t, claim, batch)

	// The negative control: a marker with neither Cause nor SizeBytes is
	// genuinely unidentifiable and must NOT be silently treated as covering
	// anything.
	unidentifiableMarker := legacyOversizedMarker
	unidentifiableMarker.SkippedArtifacts = []GitHubTestsSkippedArtifact{{RunID: "42", ArtifactID: "7"}}
	batch, err = githubTestsFinalMetadataBatch(claim, unidentifiableMarker)
	if err != nil {
		t.Fatal(err)
	}
	if batch.Watermark != nil {
		t.Fatalf("watermark=%v, want nil -- a marker with no Cause and no SizeBytes identifies nothing", batch.Watermark)
	}
	mustCompareGitHubTestsCompletionOK(t, claim, batch)
}

// TestGitHubTestsChunkedFinalMetadataOverflowShortcutExcludesReportParseCauses
// pins the CHAOS-4592 narrowing of the overflow shortcut (codex review round
// 1, P1): an INTERMEDIATE binary -- post-CHAOS-4394, pre-CHAOS-4592 -- could
// hit its overflow cap marking the three original whole-artifact causes
// (proving that marker-writing path ran) while never writing a marker for
// malformed/unreadable at all, because that binary predates those two causes
// existing. Resuming such a cursor here must still withhold on the unmarked
// malformed count -- letting the aggregate overflow prove marker-writing ran
// for a cause it never even knew about would advance over an unidentifiable
// parse skip with no backfill target, permanently. The identical overflow
// value must still excuse an unmarked ORIGINAL cause (artifact_unavailable),
// proving the narrowing is per-cause, not a wholesale removal of the
// shortcut.
func TestGitHubTestsChunkedFinalMetadataOverflowShortcutExcludesReportParseCauses(t *testing.T) {
	claim := nativeTestClaim("github", "cicd")
	intermediateBinaryCursor := githubTestsChunkCursor{
		Phase: "done", Repo: "acme/api", Requests: 3, Pages: 2,
		Incomplete: []GitHubTestsIncomplete{
			{Component: githubTestsReportMemberComponent, Cause: githubTestsArtifactUnavailableCause, Count: 1},
			{Component: githubTestsReportMemberComponent, Cause: githubTestsMalformedCause, Count: 1},
		},
		// No marker for either cause -- only the aggregate overflow counter,
		// exactly what an intermediate binary's cursor decodes as: it wrote
		// SkippedArtifactsOverflow (CHAOS-4394 machinery) but never a
		// malformed marker (CHAOS-4592 did not exist yet).
		SkippedArtifactsOverflow: 1,
	}
	batch, err := githubTestsFinalMetadataBatch(claim, intermediateBinaryCursor)
	if err != nil {
		t.Fatal(err)
	}
	if batch.Watermark != nil {
		t.Fatalf(
			"watermark=%v, want nil -- overflow must not excuse an unmarked malformed skip",
			batch.Watermark,
		)
	}
	mustCompareGitHubTestsCompletionOK(t, claim, batch)

	// The identical overflow value, with ONLY the original cause present,
	// must still advance -- proving the narrowing did not also break the
	// CHAOS-4394 shortcut for the causes it was built for.
	originalCauseOnly := intermediateBinaryCursor
	originalCauseOnly.Incomplete = []GitHubTestsIncomplete{
		{Component: githubTestsReportMemberComponent, Cause: githubTestsArtifactUnavailableCause, Count: 1},
	}
	batch, err = githubTestsFinalMetadataBatch(claim, originalCauseOnly)
	if err != nil {
		t.Fatal(err)
	}
	if batch.Watermark == nil || !batch.Watermark.Equal(*claim.BeforeAt) {
		t.Fatalf(
			"watermark=%v, want %v -- overflow must still excuse an unmarked ORIGINAL cause",
			batch.Watermark, claim.BeforeAt,
		)
	}
	mustCompareGitHubTestsCompletionOK(t, claim, batch)

	// A marker for malformed specifically, no overflow needed, must advance.
	malformedMarked := intermediateBinaryCursor
	malformedMarked.SkippedArtifactsOverflow = 0
	malformedMarked.SkippedArtifacts = []GitHubTestsSkippedArtifact{
		{RunID: "42", ArtifactID: "7", Cause: githubTestsArtifactUnavailableCause},
		{RunID: "42", ArtifactID: "7", Cause: githubTestsMalformedCause},
	}
	batch, err = githubTestsFinalMetadataBatch(claim, malformedMarked)
	if err != nil {
		t.Fatal(err)
	}
	if batch.Watermark == nil || !batch.Watermark.Equal(*claim.BeforeAt) {
		t.Fatalf(
			"watermark=%v, want %v -- a literal marker must always be sufficient for malformed",
			batch.Watermark, claim.BeforeAt,
		)
	}
	mustCompareGitHubTestsCompletionOK(t, claim, batch)
}

// mustCompareGitHubTestsCompletionOK runs the SAME comparator production
// completion goes through and fails the test if it rejects the batch --
// batch.Watermark alone is not proof of correctness (codex review round 3,
// P1): the comparator independently re-derives the blocking verdict from
// batch.Result and must reach the identical answer, or a route that computed
// a "correct" Watermark still fails to complete in production.
func mustCompareGitHubTestsCompletionOK(t *testing.T, claim Claim, batch CompleteRouteBatch) {
	t.Helper()
	if _, err := (ProductionContractComparator{}).CompareCompleteRoute(
		context.Background(), claim, batch,
	); err != nil {
		t.Fatalf("production comparator rejected a batch this test expects to be valid: %v", err)
	}
}
