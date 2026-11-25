package server

import (
	"net/http"
	"strings"
	"testing"
)

// CHAOS-14628 dual accept: `cognitiveLoad` has ONE current text, the one that asks for each signal's developerState,
// and keeps the text it accepted before as a legacy text, so the web build and the acr catalogue document live before
// this change keep working.

func TestCognitiveLoadV1Document_MatchesTheWireFormOfWebMain(t *testing.T) {
	fixture := readWireFormFile(t, "cognitiveLoad.v1.graphql")
	if got, want := digestHex(registeredCognitiveLoadV1Document), digestHex(fixture); got != want {
		t.Fatalf("registeredCognitiveLoadV1Document digests to %s, its fixture (the wire form of web main's COGNITIVE_LOAD_QUERY) to %s", got, want)
	}
	if !strings.Contains(fixture, "__typename") {
		t.Fatal("the V1 fixture carries no __typename: it is the raw web source, not a wire form")
	}
}

func TestCognitiveLoadCurrentAndV1_DifferByTheDeveloperStateSelection(t *testing.T) {
	if !strings.Contains(registeredCognitiveLoadDocument, "developerState") {
		t.Error("the current cognitiveLoad document does not ask for developerState")
	}
	if strings.Contains(registeredCognitiveLoadV1Document, "developerState") {
		t.Error("the legacy V1 cognitiveLoad document asks for developerState: it is not the old text")
	}
	if digestHex(registeredCognitiveLoadDocument) == digestHex(registeredCognitiveLoadV1Document) {
		t.Fatal("the current and the legacy text have the same digest: nothing would be dual-accepted")
	}
}

func TestCognitiveLoad_BothTextsResolveToTheOneOperation(t *testing.T) {
	legacy := legacyDigestsByOperation["cognitiveLoad"]
	if len(legacy) != 1 || legacy[0] != digestHex(registeredCognitiveLoadV1Document) {
		t.Fatalf("legacyDigestsByOperation[cognitiveLoad] = %v, want exactly the V1 digest", legacy)
	}
	byDigest, err := buildOperationByDigest(
		map[string]string{"cognitiveLoad": digestHex(registeredCognitiveLoadDocument)},
		map[string][]string{"cognitiveLoad": legacy},
	)
	if err != nil {
		t.Fatal(err)
	}
	for name, text := range map[string]string{
		"old web and acr (wire form of web main)": readWireFormFile(t, "cognitiveLoad.v1.graphql"),
		"current": registeredCognitiveLoadDocument,
	} {
		operation, ok := operationForDocument(text, byDigest)
		if !ok || operation != "cognitiveLoad" {
			t.Errorf("%s resolves to %q, %v; want cognitiveLoad, true", name, operation, ok)
		}
	}
}

// The text of main (web's COGNITIVE_LOAD_QUERY and acr's catalogue document, digest dcdfb269...) stays accepted by the
// served registry byte for byte: a POST of it reaches the cognitiveLoad handler.
func TestCognitiveLoadTextOfMainStaysAcceptedByteForByte(t *testing.T) {
	const mainDigest = "dcdfb2692addf52ed6117162963c698b628af37d01d08e4a8724f90388d8427d"
	served, byDigest, post, ran := servedRegistryPost(t, "cognitiveLoad")
	if got := digestHex(registeredCognitiveLoadV1Document); got != mainDigest {
		t.Fatalf("the text of main digests to %s, want %s (main's go_api_operations.json value)", got, mainDigest)
	}
	if served["cognitiveLoad"] == mainDigest {
		t.Errorf("the served current digest is still main's %s: the developerState selection did not land", mainDigest)
	}
	if got, ok := operationForDocument(registeredCognitiveLoadV1Document, byDigest); !ok || got != "cognitiveLoad" {
		t.Errorf("the text of main resolves to %q, %v; want cognitiveLoad, true", got, ok)
	}
	if got := post(registeredCognitiveLoadV1Document); got.Code != http.StatusOK || ran["cognitiveLoad"] != 1 {
		t.Errorf("POST the text of main: %d %s, handler ran %d; want 200 and one run", got.Code, strings.TrimSpace(got.Body.String()), ran["cognitiveLoad"])
	}
	if got := post(registeredCognitiveLoadDocument); got.Code != http.StatusOK || ran["cognitiveLoad"] != 2 {
		t.Errorf("POST the current text: %d %s, handler ran %d; want 200 and a second run", got.Code, strings.TrimSpace(got.Body.String()), ran["cognitiveLoad"])
	}
}
