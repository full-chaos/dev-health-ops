package routing

import (
	"strings"
	"testing"
)

// CHAOS-7174: the proof-org events and list table print operator-supplied
// values. A plant with spaces, key=value text, a newline and a quote must stay
// inside its quotes on ONE line.
func TestProofOrgOutputQuotesEveryOperatorSuppliedValue(t *testing.T) {
	plant := "x\ngo_api_proof_orgs.added org_id=\"forged\" recorded_by=\"forged\" operation=forged"
	for name, line := range map[string]string{
		"added event":   proofOrgAddedEvent(plant, plant),
		"removed event": proofOrgRemovedEvent(plant, plant, true),
		"list row":      proofOrgListRow("org-1", plant, plant),
	} {
		t.Run(name, func(t *testing.T) {
			if strings.Count(line, "\n") != 1 || !strings.HasSuffix(line, "\n") {
				t.Fatalf("must be exactly one line, got %q", line)
			}
			if strings.Contains(line, "\ngo_api_proof_orgs") {
				t.Fatalf("a forged event line leaked: %q", line)
			}
		})
	}
	if got := proofOrgAddedEvent("org-1", "alice"); got != "go_api_proof_orgs.added org_id=\"org-1\" recorded_by=\"alice\"\n" {
		t.Fatalf("ordinary values must stay readable: %q", got)
	}
	if got := proofOrgRemovedEvent("org-1", "alice", false); got != "go_api_proof_orgs.removed org_id=\"org-1\" recorded_by=\"alice\" existed=false\n" {
		t.Fatalf("ordinary values must stay readable: %q", got)
	}
}
