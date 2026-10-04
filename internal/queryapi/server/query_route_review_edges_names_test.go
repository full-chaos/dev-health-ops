package server

import (
	"os"
	"strings"
	"testing"
)

// CHAOS-8485 on top of CHAOS-8000 dual accept: `reviewEdges` has ONE current text, the one that asks for the
// served display names and the opaque keys of the two people of a row, and keeps the text it accepted before as a
// legacy text, so a web build still on the old text keeps working while the new web rolls out.

// The current text asks for the four served fields and NOT for the stored `reviewer` / `author` strings, which can
// be an e-mail address; the legacy text is the one that asks for those two.
func TestReviewEdgesCurrentAsksForNamesAndKeysNotTheStoredStrings(t *testing.T) {
	current, legacy := registeredReviewEdgesDocument, registeredReviewEdgesV1Document
	for _, selection := range []string{"reviewerKey", "authorKey", "reviewerName", "authorName"} {
		line := "      " + selection + "\n"
		if strings.Count(current, line) != 1 {
			t.Errorf("the current reviewEdges document does not ask for %s exactly once", selection)
		}
		if strings.Contains(legacy, line) {
			t.Errorf("the legacy V1 reviewEdges document asks for %s: it is not the old text", selection)
		}
	}
	for _, selection := range []string{"reviewer", "author"} {
		line := "      " + selection + "\n"
		if strings.Contains(current, line) {
			t.Errorf("the current reviewEdges document asks for the stored %s string", selection)
		}
		if strings.Count(legacy, line) != 1 {
			t.Errorf("the legacy V1 reviewEdges document does not ask for %s exactly once", selection)
		}
	}
	// Apart from the people fields the two texts are one text.
	withOld := strings.Replace(current, "      reviewerKey\n      authorKey\n      reviewerName\n      authorName\n", "      reviewer\n      author\n", 1)
	if withOld != legacy {
		t.Fatalf("the current document with the four people fields put back to the two stored ones is not the V1 text:\n%s", withOld)
	}
	if digestHex(current) == digestHex(legacy) {
		t.Fatal("the current and the legacy text have the same digest: nothing would be dual-accepted")
	}
}

func TestReviewEdges_BothTextsResolveToTheOneOperation(t *testing.T) {
	legacy := legacyDigestsByOperation["reviewEdges"]
	if len(legacy) != 1 || legacy[0] != digestHex(registeredReviewEdgesV1Document) {
		t.Fatalf("legacyDigestsByOperation[reviewEdges] = %v, want exactly the V1 digest", legacy)
	}
	byDigest, err := buildOperationByDigest(
		map[string]string{
			"reviewEdges":   digestHex(registeredReviewEdgesDocument),
			"cognitiveLoad": digestHex(registeredCognitiveLoadDocument),
		},
		// Only this operation's legacy digests: the index refuses a legacy entry of an operation that the map
		// above does not register.
		map[string][]string{"reviewEdges": legacy},
	)
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"reviewedges_captured.graphql", "reviewedges_v1_captured.graphql"} {
		text, err := os.ReadFile("testdata/wire_capture/" + name)
		if err != nil {
			t.Fatalf("read %s: %v", name, err)
		}
		operation, ok := operationForDocument(string(text), byDigest)
		if !ok || operation != "reviewEdges" {
			t.Errorf("%s resolves to %q, %v; want reviewEdges, true", name, operation, ok)
		}
	}
}
