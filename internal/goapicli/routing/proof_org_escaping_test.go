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
		"list row":      proofOrgListRow(plant, plant, plant),
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

// -org is echoed into stdout lines and the list table, so add and remove refuse
// a value that carries a control or line-separator character BEFORE any
// database is consulted (no -postgres-uri here: the refusal must come first).
func TestProofOrgAddAndRemoveRefuseAControlCharacterInOrg(t *testing.T) {
	t.Setenv("POSTGRES_URI", "")
	for _, sub := range []string{"add", "remove"} {
		for name, org := range map[string]string{
			"newline":        "x\ngo-api-routing: forged",
			"carriage":       "x\rforged",
			"line separator": "x\u2028forged",
			"escape":         "x\x1b[2Kforged",
		} {
			t.Run(sub+" "+name, func(t *testing.T) {
				err := run([]string{"proof-org", sub, "-org", org, "-recorded-by", "w", "-review-evidence", "y"})
				if err == nil || !strings.Contains(err.Error(), "-org must not contain control or line-separator") {
					t.Fatalf("proof-org %s -org %q = %v, want the control-character refusal", sub, org, err)
				}
			})
		}
	}
}
