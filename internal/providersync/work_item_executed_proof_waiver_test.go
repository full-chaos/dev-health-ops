package providersync

import (
	"sort"
	"testing"
)

// TestWorkItemFamilyExecutedProofWaiverIsOneMechanism pins ruling D7011: the
// canonical work-items claim of github, gitlab, jira and linear carries the
// same executed-proof waiver from one helper, no alias carries one, and no
// other pair gains a waiver by accident.
func TestWorkItemFamilyExecutedProofWaiverIsOneMechanism(t *testing.T) {
	want := workItemFamilyExecutedProofWaiver()
	for _, provider := range []string{"github", "gitlab", "jira", "linear"} {
		descriptor, ok := Descriptor(provider, "work-items")
		if !ok {
			t.Fatalf("Descriptor(%s, work-items) unknown", provider)
		}
		got := descriptor.ExecutedProofWaiver
		if got == nil {
			t.Fatalf("%s/work-items has no executed-proof waiver", provider)
		}
		if *got != *want {
			t.Fatalf("%s/work-items waiver = %+v, want the one family waiver %+v", provider, *got, *want)
		}
	}

	waived := []string{}
	for provider := range datasetCapabilities {
		for dataset := range datasetCapabilities[provider] {
			descriptor, ok := Descriptor(provider, dataset)
			if !ok || descriptor.ExecutedProofWaiver == nil {
				continue
			}
			waived = append(waived, provider+"/"+dataset)
		}
	}
	sort.Strings(waived)
	wantWaived := []string{
		"github/repo-metadata",
		"github/work-items",
		"gitlab/work-items",
		"jira/work-items",
		"linear/work-items",
	}
	if len(waived) != len(wantWaived) {
		t.Fatalf("waived pairs = %v, want %v", waived, wantWaived)
	}
	for index := range wantWaived {
		if waived[index] != wantWaived[index] {
			t.Fatalf("waived pairs = %v, want %v", waived, wantWaived)
		}
	}
}

// TestWorkItemFamilyWaiverBypassesAttemptedUnprovenEvidence is the gate-level
// cell: each waived work-items claim satisfies the executed-proof check
// against evidence that records it attempted and never proven, while an
// unwaived pair in the same snapshot still fails.
func TestWorkItemFamilyWaiverBypassesAttemptedUnprovenEvidence(t *testing.T) {
	evidence := &ExecutedProofEvidence{
		Proven: map[string]bool{},
		Attempted: map[string]bool{
			"github/work-items": true, "gitlab/work-items": true,
			"jira/work-items": true, "linear/work-items": true,
			"gitlab/commits": true,
		},
	}
	for _, provider := range []string{"github", "gitlab", "jira", "linear"} {
		descriptor, _ := Descriptor(provider, "work-items")
		if !descriptor.ExecutedProofSatisfied(evidence) {
			t.Fatalf("%s/work-items: waiver must satisfy attempted-but-unproven evidence", provider)
		}
	}
	commits, ok := Descriptor("gitlab", "commits")
	if !ok {
		t.Fatal("Descriptor(gitlab, commits) unknown")
	}
	if commits.ExecutedProofSatisfied(evidence) {
		t.Fatal("gitlab/commits has no waiver and must still fail attempted-but-unproven evidence")
	}
}
