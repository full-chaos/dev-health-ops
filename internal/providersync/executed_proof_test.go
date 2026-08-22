package providersync

import "testing"

func TestExecutedProofSatisfiedRequiresProofUnlessWaivedOrNeverAttempted(t *testing.T) {
	proven := CompleteRouteDescriptor{
		Provider: "github", RequestedDataset: "prs", CanonicalDataset: "prs",
	}
	attemptedUnproven := CompleteRouteDescriptor{
		// The CHAOS-4048/CHAOS-4049 motivating shape: tried repeatedly,
		// never once produced a real row.
		Provider: "pagerduty", RequestedDataset: "teams", CanonicalDataset: "teams",
	}
	neverAttempted := CompleteRouteDescriptor{
		// No sync_run_units row at all -- a brand-new pair, or any pair in a
		// fresh database. Must bootstrap through, not deadlock forever.
		Provider: "linear", RequestedDataset: "work-items", CanonicalDataset: "work-items",
	}
	waived := CompleteRouteDescriptor{
		Provider: "github", RequestedDataset: "repo-metadata", CanonicalDataset: "repo-metadata",
		ExecutedProofWaiver: &ExecutedProofWaiver{Reason: "test waiver", Ticket: "CHAOS-4060"},
	}
	alias := CompleteRouteDescriptor{
		// pr-reviews folds onto prs: it never accumulates its own
		// sync_run_units evidence, so it must inherit proof through
		// CanonicalDataset, not RequestedDataset.
		Provider: "github", RequestedDataset: "pr-reviews", CanonicalDataset: "prs",
	}
	evidence := &ExecutedProofEvidence{
		Proven:    map[string]bool{"github/prs": true},
		Attempted: map[string]bool{"github/prs": true, "pagerduty/teams": true},
	}

	cases := []struct {
		name       string
		descriptor CompleteRouteDescriptor
		evidence   *ExecutedProofEvidence
		want       bool
	}{
		{"proven pair with matching evidence", proven, evidence, true},
		{"attempted-unproven pair: gate fails closed", attemptedUnproven, evidence, false},
		{"never-attempted pair: bootstraps through even with non-nil evidence", neverAttempted, evidence, true},
		{"waived pair with no evidence at all: waiver bypasses the gate", waived, evidence, true},
		{"waived pair even against nil evidence", waived, nil, true},
		{"alias identity inherits its canonical writer's evidence", alias, evidence, true},
		{
			"nil evidence: caller has not wired the gate, pre-CHAOS-4060 pass-through",
			attemptedUnproven, nil, true,
		},
		{
			"non-nil but entirely empty evidence: nothing attempted anywhere, bootstrap passes",
			proven, &ExecutedProofEvidence{Proven: map[string]bool{}, Attempted: map[string]bool{}}, true,
		},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			if got := testCase.descriptor.ExecutedProofSatisfied(testCase.evidence); got != testCase.want {
				t.Fatalf("ExecutedProofSatisfied() = %v, want %v", got, testCase.want)
			}
		})
	}
}

func TestExecutedProofEvidenceHasExecutedProofIsCaseInsensitiveOnMatrixKey(t *testing.T) {
	evidence := &ExecutedProofEvidence{Proven: map[string]bool{"github/prs": true}}
	if !evidence.HasExecutedProof("GitHub", "PRs") {
		t.Fatal("HasExecutedProof should normalize case exactly like matrixKey does")
	}
	if evidence.HasExecutedProof("github", "commits") {
		t.Fatal("HasExecutedProof must not manufacture proof for an absent key")
	}
	var nilEvidence *ExecutedProofEvidence
	if nilEvidence.HasExecutedProof("github", "prs") {
		t.Fatal("a nil evidence pointer must never report proof directly")
	}
	if nilEvidence.HasBeenAttempted("github", "prs") {
		t.Fatal("a nil evidence pointer must never report an attempt directly")
	}
}

// TestExecutedProofSatisfiedDegradedRevokesOnlyTheNeverAttemptedPassThrough
// is the codex round-4 fix: a Degraded snapshot must not un-prove a pair
// that already has durable Proven evidence (that fact does not stop being
// true because a LATER refresh failed), but it MUST stop granting the
// never-attempted bootstrap pass-through -- once the evidence query itself
// is failing, "this pair has no history" can no longer be trusted as a true
// negative.
func TestExecutedProofSatisfiedDegradedRevokesOnlyTheNeverAttemptedPassThrough(t *testing.T) {
	proven := CompleteRouteDescriptor{
		Provider: "github", RequestedDataset: "prs", CanonicalDataset: "prs",
	}
	attemptedUnproven := CompleteRouteDescriptor{
		Provider: "pagerduty", RequestedDataset: "teams", CanonicalDataset: "teams",
	}
	neverAttempted := CompleteRouteDescriptor{
		Provider: "linear", RequestedDataset: "work-items", CanonicalDataset: "work-items",
	}
	waived := CompleteRouteDescriptor{
		Provider: "github", RequestedDataset: "repo-metadata", CanonicalDataset: "repo-metadata",
		ExecutedProofWaiver: &ExecutedProofWaiver{Reason: "test waiver", Ticket: "CHAOS-4060"},
	}
	degraded := &ExecutedProofEvidence{
		Proven:    map[string]bool{"github/prs": true},
		Attempted: map[string]bool{"github/prs": true, "pagerduty/teams": true},
		Degraded:  true,
	}

	cases := []struct {
		name       string
		descriptor CompleteRouteDescriptor
		want       bool
	}{
		{"already-proven pair survives a degraded snapshot unchanged", proven, true},
		{"already attempted-unproven pair stays blocked under a degraded snapshot", attemptedUnproven, false},
		{
			"never-attempted pair is blocked under a degraded snapshot -- " +
				"the bootstrap pass-through cannot be trusted once the query itself is failing",
			neverAttempted, false,
		},
		{"a waiver still bypasses a degraded snapshot entirely", waived, true},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			if got := testCase.descriptor.ExecutedProofSatisfied(degraded); got != testCase.want {
				t.Fatalf("ExecutedProofSatisfied() = %v, want %v", got, testCase.want)
			}
		})
	}
}

func TestExecutedProofEvidenceHasBeenAttempted(t *testing.T) {
	evidence := &ExecutedProofEvidence{
		Attempted: map[string]bool{"pagerduty/teams": true},
	}
	if !evidence.HasBeenAttempted("PagerDuty", "Teams") {
		t.Fatal("HasBeenAttempted should normalize case exactly like matrixKey does")
	}
	if evidence.HasBeenAttempted("github", "commits") {
		t.Fatal("HasBeenAttempted must not manufacture an attempt for an absent key")
	}
}
