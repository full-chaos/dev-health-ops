package providersync

import "testing"

func TestExecutedProofSatisfiedRequiresEvidenceOrWaiver(t *testing.T) {
	proven := CompleteRouteDescriptor{
		Provider: "github", RequestedDataset: "prs", CanonicalDataset: "prs",
	}
	unproven := CompleteRouteDescriptor{
		Provider: "pagerduty", RequestedDataset: "teams", CanonicalDataset: "teams",
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
	evidence := ExecutedProofEvidence{"github/prs": true}

	cases := []struct {
		name       string
		descriptor CompleteRouteDescriptor
		evidence   ExecutedProofEvidence
		want       bool
	}{
		{"proven pair with matching evidence", proven, evidence, true},
		{"unproven pair, non-nil evidence lacking it: gate fails closed", unproven, evidence, false},
		{"waived pair with no evidence at all: waiver bypasses the gate", waived, evidence, true},
		{"waived pair even against nil evidence", waived, nil, true},
		{"alias identity inherits its canonical writer's evidence", alias, evidence, true},
		{
			"nil evidence: caller has not wired the gate, pre-CHAOS-4060 pass-through",
			unproven, nil, true,
		},
		{"non-nil but empty evidence: nothing proven anywhere", proven, ExecutedProofEvidence{}, false},
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
	evidence := ExecutedProofEvidence{"github/prs": true}
	if !evidence.HasExecutedProof("GitHub", "PRs") {
		t.Fatal("HasExecutedProof should normalize case exactly like matrixKey does")
	}
	if evidence.HasExecutedProof("github", "commits") {
		t.Fatal("HasExecutedProof must not manufacture proof for an absent key")
	}
	var nilEvidence ExecutedProofEvidence
	if nilEvidence.HasExecutedProof("github", "prs") {
		t.Fatal("a nil evidence map must never report proof directly")
	}
}
