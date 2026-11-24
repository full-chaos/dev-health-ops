package providersync

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"
)

// githubTestsWidestArtifactsCursor is the worst-case artifacts-phase cursor of
// TestGitHubTestsChunkCursorWorstCaseStaysWithinBudget (same fields, same caps).
func githubTestsWidestArtifactsCursor(t *testing.T) githubTestsChunkCursor {
	t.Helper()
	cursor := githubTestsRealisticCursorBase()
	name := strings.Repeat("x", githubTestsMaxArtifactNameBytes)
	maxID := strings.Repeat("9", githubTestsMaxArtifactIDBytes)
	for i := 0; i < githubTestsMaxSkippedArtifactRecords; i++ {
		cursor.SkippedArtifacts = append(cursor.SkippedArtifacts, GitHubTestsSkippedArtifact{
			RunID: maxID, ArtifactID: maxID, Name: name, Cause: githubTestsUnreadableArchiveCause,
			SizeBytes: 9223372036854775807, CapBytes: 9223372036854775807,
		})
	}
	causeOverflow, causeCount := map[string]bool{}, map[string]int{}
	for _, cause := range []string{
		githubTestsArtifactOversizedCause, githubTestsArtifactUnavailableCause,
		githubTestsUnreadableArchiveCause, githubTestsMalformedCause, githubTestsUnreadableCause, githubTestsUnsafeDoctypeCause, githubTestsArtifactExpiredCause,
		githubTestsCompressionRatioCause, githubTestsArchiveEntryCapCause,
	} {
		causeOverflow[cause], causeCount[cause] = true, 999999999
	}
	cursor.SkippedArtifactCauseOverflow, cursor.SkippedArtifactCauseCount = causeOverflow, causeCount
	for i := 0; i < githubTestsMaxExcludedArtifactSampleRecords; i++ {
		cursor.ExcludedArtifactSample = append(cursor.ExcludedArtifactSample, name+" (non_report_artifact_suffix)")
	}
	cursor.RangeHi, cursor.RangeOldest, cursor.RangeItems = "2026-07-20T23:59:59Z", "2026-07-20T00:00:00Z", githubTestsSearchResultCap
	cursor.ActiveCutKeys, cursor.CarriedCutKeys = githubTestsWidestCutKeys(t), githubTestsWidestCutKeys(t)
	cursor.AnchorID = githubTestsWidestAnchorID
	cursor.ReseekPages, cursor.PagesEntered, cursor.RecordCredit = githubTestsWidestReseekPages, githubTestsWidestReseekPages, githubTestsWidestRecordCredit
	cursor.DeletionProven = 1
	cursor.LateRecords = githubTestsMaxWalkRecords
	cursor.ExtraRecords = githubTestsMaxWalkRecords
	boundsWidestBackfillCounters(&cursor)
	return cursor
}

// githubTestsRepoNameCapCursor is the widest cursor of one repository mid-way through the first page of an artifacts-phase
// runs listing: Repo is owner/name (CollectChunks) and NextURL is that page's own URL (page.CursorBefore), the relative
// path providerRelativePath builds plus githubTestsArtifactRunQueryAt, as VisitGitHubLinkPages requests it (pageURL).
// A later page's URL is GitHub's Link next, /repositories/{id}/..., which holds no name.
func githubTestsRepoNameCapCursor(t *testing.T, owner, repository string) githubTestsChunkCursor {
	t.Helper()
	claim := nativeTestClaim("github", "cicd")
	since, before := time.Date(2026, 7, 13, 0, 0, 0, 0, time.UTC), time.Date(2026, 7, 20, 23, 59, 59, 0, time.UTC)
	claim.SinceAt, claim.BeforeAt = &since, &before
	cursor := githubTestsWidestArtifactsCursor(t)
	cursor.Repo = owner + "/" + repository
	cursor.NextURL = providerRelativePath(nil, "repos", owner, repository) + "/actions/runs?" +
		githubTestsArtifactRunQueryAt(claim, cursor.RangeHi).Encode()
	return cursor
}

// githubTestsRepoNameCapEncode is what emitCursorAs does with a cursor: fit it (githubTestsFitCursor), then encode it.
func githubTestsRepoNameCapEncode(cursor githubTestsChunkCursor) (encoded string, dropped []string, err error) {
	fitted, dropped := githubTestsFitCursor(cursor)
	encoded, err = encodeGitHubTestsChunkCursor(fitted)
	return encoded, dropped, err
}

// A repository at GitHub's name limits (owner 39 characters, repository name 100; GitHub's sign-up and repository-name
// validation, not stated in the REST docs) must still checkpoint its tests. Repo and NextURL both carry owner/name and have
// no length bound in the cursor, and githubTestsFitCursor never drops them: over maxChunkCursorBytes,
// encodeGitHubTestsChunkCursor returns ErrChunkCheckpointConflict, which providerunit classes as checkpoint_conflict
// (terminal, no retry), so that repository's tests would never sync. At CHAOS-14580 the max shape is 4139 bytes before
// the fit and 4079 after it, the active cut list dropped: 17 bytes before the fit drops the carried cut list too (a
// re-list), 105 bytes before the conflict. The cell holds the fit to the active cut list.
// Any new chunk-cursor field must keep this cell green at the max name shape.
func TestGitHubTestsChunkCursorFitsARepositoryAtGitHubNameLimits(t *testing.T) {
	const maxOwner, maxRepository = 39, 100
	short, _, err := githubTestsRepoNameCapEncode(githubTestsRepoNameCapCursor(t, "acme", "api"))
	if err != nil {
		t.Fatalf("premise: the widest cursor of acme/api does not encode: %v", err)
	}
	headroom := maxChunkCursorBytes - len(short)
	smallest := 0
	for total := 2; total <= maxOwner+maxRepository && smallest == 0; total++ {
		owner := min(total-1, maxOwner)
		cursor := githubTestsRepoNameCapCursor(t, strings.Repeat("o", owner), strings.Repeat("r", total-owner))
		if _, _, err := githubTestsRepoNameCapEncode(cursor); err != nil {
			smallest = total + 1
		}
	}
	cursor := githubTestsRepoNameCapCursor(t, strings.Repeat("o", maxOwner), strings.Repeat("r", maxRepository))
	fitted, dropped := githubTestsFitCursor(cursor)
	raw, marshalErr := json.Marshal(fitted)
	if marshalErr != nil {
		t.Fatal(marshalErr)
	}
	unfitted, marshalErr := json.Marshal(cursor)
	if marshalErr != nil {
		t.Fatal(marshalErr)
	}
	_, _, err = githubTestsRepoNameCapEncode(cursor)
	t.Logf("acme/api widest cursor %d bytes, headroom %d; at %d+1+%d: Repo %d bytes, NextURL %d bytes, %d bytes before the fit, %d after it (dropped %v), cap %d, margin %d",
		len(short), headroom, maxOwner, maxRepository, len(cursor.Repo), len(cursor.NextURL), len(unfitted), len(raw), dropped, maxChunkCursorBytes, maxChunkCursorBytes-len(raw))
	if errors.Is(err, ErrChunkCheckpointConflict) {
		t.Fatalf("a %d-character owner and a %d-character repository name: the cursor is %d bytes after the fit, %d over maxChunkCursorBytes (%d): "+
			"ErrChunkCheckpointConflict, terminal checkpoint_conflict; the smallest owner/name that tips it is %d characters",
			maxOwner, maxRepository, len(raw), len(raw)-maxChunkCursorBytes, maxChunkCursorBytes, smallest)
	}
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	if len(dropped) > 1 || len(dropped) == 1 && dropped[0] != "active" {
		t.Fatalf("at the max name shape the fit drops %v, want at most the active cut list: the margin before the carried cut list is dropped is spent",
			dropped)
	}
}
