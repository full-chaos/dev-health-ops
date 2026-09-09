package providersync

import (
	"encoding/json"
	"reflect"
	"testing"
	"time"

	"github.com/google/uuid"
)

// TestGitHubWorkItemTeamAttributionRowSurvivesTheEffectsJSONRoundTrip is
// CHAOS-4320's red-first pin for codex round 2's P1 (NOT CLEAN, executed
// repro): githubWorkItemTeamAttributionRow does not go directly from
// buildGitHubWorkItemTeamAttributions to WriteGitHubWorkItemEffect -- it is
// json.Marshal'd into an EffectBatch's json.RawMessage rows
// (marshalGitHubWorkItemDerivedRows / derivedRows, the effects/outbox
// layer) and json.Unmarshal'd back into this same struct type in
// validateGitHubWorkItemDerivedEffect before the write boundary ever reads
// it. Priority (`json:"-"`, CHAOS-4321) and OwnershipReason (`json:"-"`,
// this ticket's own r1 fix) were BOTH dropped by that round trip -- a field
// tagged `json:"-"` marshals to nothing and unmarshals back as its zero
// value, regardless of what was actually set. This made round 1's F1 fix a
// no-op in production: every unit test that calls Resolve()/
// buildGitHubWorkItemTeamAttributions directly (never going through the
// effects layer) passed, hiding the defect completely.
//
// This test goes through the REAL round trip (json.Marshal, then the same
// validateGitHubWorkItemDerivedEffect the write path calls) rather than
// just asserting on the struct directly, so it fails the way production
// actually failed.
func TestGitHubWorkItemTeamAttributionRowSurvivesTheEffectsJSONRoundTrip(t *testing.T) {
	original := githubWorkItemTeamAttributionRow{
		WorkItemID: "gh:acme/api#1", Provider: "github", Source: "assignee_membership",
		IsPrimary: 1, Confidence: "high", Evidence: "assignee=dev@example.com",
		OrgID: "org-acme", Priority: 10, OwnershipReason: "ownership_unknown",
	}
	raw, err := json.Marshal(original)
	if err != nil {
		t.Fatal(err)
	}
	effect, err := BuildEffectBatch(
		githubTeamAttributionsDestination, EffectReadbackRequired,
		[]json.RawMessage{raw},
	)
	if err != nil {
		t.Fatal(err)
	}
	identity := GitHubWorkItemEffectIdentity{
		OrgID: "org-acme", Provider: "github", Destination: githubTeamAttributionsDestination,
		ContentDigest: effect.ContentDigest, RowCount: len(effect.Rows),
	}
	rows, err := validateGitHubWorkItemDerivedEffect[githubWorkItemTeamAttributionRow](
		identity, effect, githubTeamAttributionsDestination,
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 {
		t.Fatalf("rows = %+v, want exactly 1", rows)
	}
	if rows[0].Priority != 10 {
		t.Fatalf("rows[0].Priority = %d after the effects round trip, want 10 (the value actually set) -- a json:\"-\" tag silently drops this field", rows[0].Priority)
	}
	if rows[0].OwnershipReason != "ownership_unknown" {
		t.Fatalf("rows[0].OwnershipReason = %q after the effects round trip, want ownership_unknown", rows[0].OwnershipReason)
	}
}

// TestGitHubWorkItemTeamAttributionRowNoExportedFieldReadsBackZero is a
// RECURRENCE GUARD (codex round 2 follow-up, team-lead): a test scoped to
// Priority/OwnershipReason only proves those two fields, not the STRUCT --
// the next field added to githubWorkItemTeamAttributionRow with an
// accidental `json:"-"` would reproduce the exact same defect class and
// this test would say nothing about it. This test instead populates EVERY
// exported field to a deliberately non-zero value via reflection, round-
// trips the row through the real effects path (identical mechanism to the
// test above), and fails if ANY exported field reads back as its Go zero
// value -- so a future silently-dropped field is caught structurally,
// without needing its own dedicated test.
func TestGitHubWorkItemTeamAttributionRowNoExportedFieldReadsBackZero(t *testing.T) {
	repoID := uuid.MustParse("c7198fbc-1945-3717-05d8-eb78866b4e79")
	teamID := "team-repo"
	teamName := "Repository Team"
	original := githubWorkItemTeamAttributionRow{
		WorkItemID: "gh:acme/api#1", Provider: "github", Source: "assignee_membership",
		IsPrimary: 1, Confidence: "high", Evidence: "assignee=dev@example.com",
		ComputedAt: time.Date(2026, 8, 4, 12, 0, 0, 0, time.UTC),
		RepoID:     &repoID, TeamID: &teamID, TeamName: &teamName,
		OrgID: "org-acme", Priority: 10, OwnershipReason: "ownership_unknown",
	}

	// Sanity control: EVERY exported field on the original must itself be
	// non-zero, or this test would vacuously pass on a field it forgot to
	// populate -- the same "positive control" discipline every other
	// mutation/round-trip proof in this codebase requires.
	assertNoExportedFieldIsZero(t, "original", original)

	raw, err := json.Marshal(original)
	if err != nil {
		t.Fatal(err)
	}
	effect, err := BuildEffectBatch(
		githubTeamAttributionsDestination, EffectReadbackRequired,
		[]json.RawMessage{raw},
	)
	if err != nil {
		t.Fatal(err)
	}
	identity := GitHubWorkItemEffectIdentity{
		OrgID: "org-acme", Provider: "github", Destination: githubTeamAttributionsDestination,
		ContentDigest: effect.ContentDigest, RowCount: len(effect.Rows),
	}
	rows, err := validateGitHubWorkItemDerivedEffect[githubWorkItemTeamAttributionRow](
		identity, effect, githubTeamAttributionsDestination,
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 {
		t.Fatalf("rows = %+v, want exactly 1", rows)
	}
	assertNoExportedFieldIsZero(t, "round-tripped", rows[0])
}

// assertNoExportedFieldIsZero fails the test naming every exported field of
// value that reflect.Value.IsZero reports as unset.
func assertNoExportedFieldIsZero(t *testing.T, label string, value any) {
	t.Helper()
	reflected := reflect.ValueOf(value)
	reflectedType := reflected.Type()
	var zeroFields []string
	for index := 0; index < reflected.NumField(); index++ {
		field := reflectedType.Field(index)
		if !field.IsExported() {
			continue
		}
		if reflected.Field(index).IsZero() {
			zeroFields = append(zeroFields, field.Name)
		}
	}
	if len(zeroFields) > 0 {
		t.Fatalf("%s githubWorkItemTeamAttributionRow has zero-valued exported field(s): %v -- either the fixture forgot to populate them (fix the fixture) or the effects JSON round trip silently dropped them (check for a json:\"-\" tag)", label, zeroFields)
	}
}
