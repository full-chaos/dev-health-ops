package server

import (
	"os"
	"strings"
	"testing"
)

// CHAOS-8113 on top of CHAOS-8000 dual accept: `aiWorkflowDrilldown` has ONE current text, the one that asks for
// the display name of a node and for whether the node's type carries a name, and keeps the text it accepted
// before as a legacy text, so a web build still on the old text keeps working while the new web rolls out.

// The current text is the old text plus exactly the two node selections: take them out and the legacy text is
// left, byte for byte.
func TestAiWorkflowDrilldownCurrentAndV1_DifferByTheTwoNodeSelections(t *testing.T) {
	current := registeredAiWorkflowDrilldownDocument
	added := "      nodeId\n      displayName\n      nameExpected\n"
	if strings.Count(current, added) != 1 {
		t.Fatal("the current aiWorkflowDrilldown document does not ask for displayName and nameExpected right after nodeId, once")
	}
	for _, selection := range []string{"displayName", "nameExpected"} {
		if strings.Contains(registeredAiWorkflowDrilldownV1Document, selection) {
			t.Fatalf("the legacy V1 aiWorkflowDrilldown document asks for %s: it is not the old text", selection)
		}
	}
	without := strings.Replace(current, added, "      nodeId\n", 1)
	if without != registeredAiWorkflowDrilldownV1Document {
		t.Fatalf("the current document less the two node selections is not the V1 text:\n%s", without)
	}
	if digestHex(current) == digestHex(registeredAiWorkflowDrilldownV1Document) {
		t.Fatal("the current and the legacy text have the same digest: nothing would be dual-accepted")
	}
}

func TestAiWorkflowDrilldown_BothTextsResolveToTheOneOperation(t *testing.T) {
	legacy := legacyDigestsByOperation["aiWorkflowDrilldown"]
	if len(legacy) != 1 || legacy[0] != digestHex(registeredAiWorkflowDrilldownV1Document) {
		t.Fatalf("legacyDigestsByOperation[aiWorkflowDrilldown] = %v, want exactly the V1 digest", legacy)
	}
	byDigest, err := buildOperationByDigest(
		map[string]string{
			"aiWorkflowDrilldown": digestHex(registeredAiWorkflowDrilldownDocument),
			"aiOpportunities":     digestHex(registeredAiOpportunitiesDocument),
		},
		// Only this operation's legacy digests: the index refuses a legacy entry of an operation that the map
		// above does not register.
		map[string][]string{"aiWorkflowDrilldown": legacy},
	)
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"aiworkflowdrilldown_captured.graphql", "aiworkflowdrilldown_v1_captured.graphql"} {
		text, err := os.ReadFile("testdata/wire_capture/" + name)
		if err != nil {
			t.Fatalf("read %s: %v", name, err)
		}
		operation, ok := operationForDocument(string(text), byDigest)
		if !ok || operation != "aiWorkflowDrilldown" {
			t.Errorf("%s resolves to %q, %v; want aiWorkflowDrilldown, true", name, operation, ok)
		}
	}
}
