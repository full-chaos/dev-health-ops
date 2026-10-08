package server

import (
	"os"
	"strings"
	"testing"
)

// CHAOS-8991 on top of CHAOS-8000 dual accept: five operations ask for the CHAOS-8954 name fields in their current
// text and keep the text they accepted before as a legacy text, so the web build live before this change keeps working.

var nameFieldOperations = []struct {
	operation string
	current   string
	previous  string
	fixture   string // wire_capture file of the previous text
	added     []string
	// legacyCount is how many legacy texts the operation lists in all, the previous one last.
	legacyCount int
}{
	{"aiAttributionOverview", registeredAiAttributionOverviewDocument, registeredAiAttributionOverviewV1Document, "aiattributionoverview_v1_captured.graphql", []string{"repoName", "teamName", "subjectTitle"}, 1},
	{"aiGovernanceSummary", registeredAiGovernanceSummaryDocument, registeredAiGovernanceSummaryV1Document, "aigovernancesummary_v1_captured.graphql", []string{"repoName", "teamName", "subjectTitle", "ruleName"}, 1},
	{"dataHealthIdentity", registeredDataHealthIdentityDocument, registeredDataHealthIdentityV1Document, "data_health_identity_v1_captured.graphql", []string{"suggestedCanonicalName"}, 1},
	{"improveOpportunities", registeredImproveOpportunitiesDocument, registeredImproveOpportunitiesV2Document, "improveopportunities_v2_captured.graphql", []string{"entityDisplayName"}, 2},
	{"testopsRisk", registeredTestopsRiskDocument, registeredTestopsRiskV1Document, "testopsrisk_v1_captured.graphql", []string{"name"}, 1},
}

// The current text is the previous text plus exactly the name selections: take them out and the previous text is
// left, byte for byte.
func TestNameFieldDocuments_CurrentIsThePreviousPlusTheNameSelections(t *testing.T) {
	for _, c := range nameFieldOperations {
		without := c.current
		for _, field := range c.added {
			lines := 0
			var kept []string
			for _, line := range strings.Split(without, "\n") {
				if strings.TrimSpace(line) == field {
					lines++
					continue
				}
				kept = append(kept, line)
			}
			if lines == 0 {
				t.Errorf("%s: the current document does not select %s", c.operation, field)
			}
			if strings.Contains(c.previous, "\n"+strings.Repeat(" ", 2)+field+"\n") {
				t.Errorf("%s: the previous document already selects %s: it is not the old text", c.operation, field)
			}
			without = strings.Join(kept, "\n")
		}
		if without != c.previous {
			t.Errorf("%s: the current document less the name selections is not the previous text:\n%s", c.operation, without)
		}
		if digestHex(c.current) == digestHex(c.previous) {
			t.Errorf("%s: the current and the previous text have the same digest: nothing would be dual-accepted", c.operation)
		}
	}
}

func TestNameFieldDocuments_BothTextsResolveToTheOneOperation(t *testing.T) {
	for _, c := range nameFieldOperations {
		legacy := legacyDigestsByOperation[c.operation]
		if len(legacy) != c.legacyCount || legacy[len(legacy)-1] != digestHex(c.previous) {
			t.Errorf("legacyDigestsByOperation[%s] = %v, want %d digests ending with the previous text", c.operation, legacy, c.legacyCount)
			continue
		}
		byDigest, err := buildOperationByDigest(
			map[string]string{c.operation: digestHex(c.current)},
			map[string][]string{c.operation: legacy},
		)
		if err != nil {
			t.Fatal(err)
		}
		previous, err := os.ReadFile("testdata/wire_capture/" + c.fixture)
		if err != nil {
			t.Fatalf("read %s: %v", c.fixture, err)
		}
		for name, text := range map[string]string{"previous wire form": string(previous), "current": c.current} {
			operation, ok := operationForDocument(text, byDigest)
			if !ok || operation != c.operation {
				t.Errorf("%s %s resolves to %q, %v; want %s, true", c.operation, name, operation, ok, c.operation)
			}
		}
	}
}
