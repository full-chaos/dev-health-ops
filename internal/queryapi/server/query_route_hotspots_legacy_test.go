package server

import (
	"strings"
	"testing"
)

// CHAOS-8488 on top of CHAOS-8000 dual accept: `hotspots` has ONE current text, the one that asks for `repos`, and
// keeps the text it accepted before as a legacy text, so the web build live before this change keeps working.

func TestHotspotsV1Document_MatchesTheWireFormOfWebMain(t *testing.T) {
	fixture := readWireFormFile(t, "hotspots.v1.graphql")
	if got, want := digestHex(registeredHotspotsV1Document), digestHex(fixture); got != want {
		t.Fatalf("registeredHotspotsV1Document digests to %s, its fixture (the wire form of web main's HOTSPOTS_QUERY) to %s", got, want)
	}
	if !strings.Contains(fixture, "__typename") {
		t.Fatal("the V1 fixture carries no __typename: it is the raw web source, not a wire form")
	}
}

func TestHotspotsCurrentAndV1_DifferByTheReposSelection(t *testing.T) {
	if !strings.Contains(registeredHotspotsDocument, "repos {") {
		t.Error("the current hotspots document does not ask for repos")
	}
	if strings.Contains(registeredHotspotsV1Document, "repos") {
		t.Error("the legacy V1 hotspots document asks for repos: it is not the old text")
	}
	if digestHex(registeredHotspotsDocument) == digestHex(registeredHotspotsV1Document) {
		t.Fatal("the current and the legacy text have the same digest: nothing would be dual-accepted")
	}
}

func TestHotspots_BothTextsResolveToTheOneOperation(t *testing.T) {
	legacy := legacyDigestsByOperation["hotspots"]
	if len(legacy) != 1 || legacy[0] != digestHex(registeredHotspotsV1Document) {
		t.Fatalf("legacyDigestsByOperation[hotspots] = %v, want exactly the V1 digest", legacy)
	}
	byDigest, err := buildOperationByDigest(
		map[string]string{"hotspots": digestHex(registeredHotspotsDocument)},
		map[string][]string{"hotspots": legacy},
	)
	if err != nil {
		t.Fatal(err)
	}
	for name, text := range map[string]string{
		"old web (wire form of web main)": readWireFormFile(t, "hotspots.v1.graphql"),
		"current":                         registeredHotspotsDocument,
	} {
		operation, ok := operationForDocument(text, byDigest)
		if !ok || operation != "hotspots" {
			t.Errorf("%s resolves to %q, %v; want hotspots, true", name, operation, ok)
		}
	}
}
