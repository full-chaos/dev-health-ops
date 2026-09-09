package providersync

import (
	"encoding/json"
	"testing"
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
