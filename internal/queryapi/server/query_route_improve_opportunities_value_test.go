package server

import (
	"os"
	"strings"
	"testing"
)

// CHAOS-8500 on top of CHAOS-8000 dual accept: `improveOpportunities` has ONE current text, the one that asks for
// the measured value, its threshold, the unit and the threshold direction of an opportunity (the fields of
// CHAOS-7626), and keeps the text it accepted before as a legacy text, so a web build still on the old text keeps
// working while the new web rolls out.

var improveOpportunitiesValueSelections = []string{"value", "threshold", "unit", "thresholdDirection"}

// The current text is the old text plus exactly the four selections, each on its own line after recommendedAction:
// take them out and the legacy text is left, byte for byte.
func TestImproveOpportunitiesCurrentAndV1_DifferByTheFourValueSelections(t *testing.T) {
	current := registeredImproveOpportunitiesDocument
	without := current
	for _, selection := range improveOpportunitiesValueSelections {
		line := "      " + selection + "\n"
		if strings.Count(current, line) != 1 {
			t.Fatalf("the current improveOpportunities document does not ask for %s exactly once", selection)
		}
		if strings.Contains(registeredImproveOpportunitiesV1Document, line) {
			t.Fatalf("the legacy V1 improveOpportunities document asks for %s: it is not the old text", selection)
		}
		without = strings.Replace(without, line, "", 1)
	}
	if without != registeredImproveOpportunitiesV1Document {
		t.Fatalf("the current document less the four selections is not the V1 text:\n%s", without)
	}
	if digestHex(current) == digestHex(registeredImproveOpportunitiesV1Document) {
		t.Fatal("the current and the legacy text have the same digest: nothing would be dual-accepted")
	}
}

func TestImproveOpportunities_BothTextsResolveToTheOneOperation(t *testing.T) {
	legacy := legacyDigestsByOperation["improveOpportunities"]
	if len(legacy) != 1 || legacy[0] != digestHex(registeredImproveOpportunitiesV1Document) {
		t.Fatalf("legacyDigestsByOperation[improveOpportunities] = %v, want exactly the V1 digest", legacy)
	}

	byDigest, err := buildOperationByDigest(
		map[string]string{
			"improveOpportunities": digestHex(registeredImproveOpportunitiesDocument),
			"aiOpportunities":      digestHex(registeredAiOpportunitiesDocument),
		},
		// Only this operation's legacy digests: the index refuses a legacy entry of an operation that the map
		// above does not register.
		map[string][]string{"improveOpportunities": legacy},
	)
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"improveopportunities_captured.graphql", "improveopportunities_v1_captured.graphql"} {
		text, err := os.ReadFile("testdata/wire_capture/" + name)
		if err != nil {
			t.Fatalf("read %s: %v", name, err)
		}
		operation, ok := operationForDocument(string(text), byDigest)
		if !ok || operation != "improveOpportunities" {
			t.Errorf("%s resolves to %q, %v; want improveOpportunities, true", name, operation, ok)
		}
	}
}
