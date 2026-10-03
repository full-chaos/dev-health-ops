package server

import (
	"os"
	"strings"
	"testing"
)

// CHAOS-8114 on top of CHAOS-8000 dual accept: `aiOpportunities` has ONE current text, the one that asks for the
// served repository and team names of an opportunity, and keeps the text it accepted before as a legacy text, so a
// web build still on the old text keeps working while the new web rolls out.

// The current text is the old text plus exactly the two name selections, each on the line after its id: take them
// out and the legacy text is left, byte for byte.
func TestAiOpportunitiesCurrentAndV1_DifferByTheTwoNameSelections(t *testing.T) {
	current := registeredAiOpportunitiesDocument
	without := current
	for id, name := range map[string]string{"repoId": "repoName", "teamId": "teamName"} {
		pair := "      " + id + "\n      " + name + "\n"
		if strings.Count(current, pair) != 1 {
			t.Fatalf("the current aiOpportunities document does not ask for %s on the line after %s exactly once", name, id)
		}
		if strings.Contains(registeredAiOpportunitiesV1Document, "      "+name+"\n") {
			t.Fatalf("the legacy V1 aiOpportunities document asks for %s: it is not the old text", name)
		}
		without = strings.Replace(without, pair, "      "+id+"\n", 1)
	}
	if without != registeredAiOpportunitiesV1Document {
		t.Fatalf("the current document less the two name selections is not the V1 text:\n%s", without)
	}
	if digestHex(current) == digestHex(registeredAiOpportunitiesV1Document) {
		t.Fatal("the current and the legacy text have the same digest: nothing would be dual-accepted")
	}
}

func TestAiOpportunities_BothTextsResolveToTheOneOperation(t *testing.T) {
	legacy := legacyDigestsByOperation["aiOpportunities"]
	if len(legacy) != 1 || legacy[0] != digestHex(registeredAiOpportunitiesV1Document) {
		t.Fatalf("legacyDigestsByOperation[aiOpportunities] = %v, want exactly the V1 digest", legacy)
	}
	byDigest, err := buildOperationByDigest(
		map[string]string{
			"aiOpportunities":      digestHex(registeredAiOpportunitiesDocument),
			"improveOpportunities": digestHex(registeredImproveOpportunitiesDocument),
		},
		// Only this operation's legacy digests: the index refuses a legacy entry of an operation that the map
		// above does not register.
		map[string][]string{"aiOpportunities": legacy},
	)
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"aiopportunities_captured.graphql", "aiopportunities_v1_captured.graphql"} {
		text, err := os.ReadFile("testdata/wire_capture/" + name)
		if err != nil {
			t.Fatalf("read %s: %v", name, err)
		}
		operation, ok := operationForDocument(string(text), byDigest)
		if !ok || operation != "aiOpportunities" {
			t.Errorf("%s resolves to %q, %v; want aiOpportunities, true", name, operation, ok)
		}
	}
}
