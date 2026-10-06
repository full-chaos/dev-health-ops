package server

import (
	"os"
	"strings"
	"testing"
)

func TestHomeCurrentAndLegacyDocumentsCarryTheirOwnSelections(t *testing.T) {
	if strings.Count(registeredHomeDocument, "scopeDataConfidence") != 1 {
		t.Fatal("current Home document must select scopeDataConfidence exactly once")
	}
	if strings.Contains(registeredHomeV2Document, "scopeDataConfidence") || strings.Contains(registeredHomeV1Document, "scopeDataConfidence") {
		t.Fatal("a pre-CHAOS-8107 legacy Home document selects scopeDataConfidence")
	}
	for _, line := range []string{"      hasData\n", "      hasPriorData\n"} {
		if strings.Count(registeredHomeDocument, line) != 1 || strings.Count(registeredHomeV2Document, line) != 1 {
			t.Fatalf("current and V2 Home documents must each contain %q once", line)
		}
		if strings.Contains(registeredHomeV1Document, strings.TrimSpace(line)) {
			t.Fatalf("V1 Home document contains %q", strings.TrimSpace(line))
		}
	}
	if digestHex(registeredHomeDocument) == digestHex(registeredHomeV2Document) ||
		digestHex(registeredHomeDocument) == digestHex(registeredHomeV1Document) ||
		digestHex(registeredHomeV2Document) == digestHex(registeredHomeV1Document) {
		t.Fatal("current and legacy Home documents must have distinct digests")
	}
}

func TestHomeCurrentAndLegacyTextsResolveToHome(t *testing.T) {
	legacy := legacyDigestsByOperation["home"]
	if len(legacy) != 2 || legacy[0] != digestHex(registeredHomeV1Document) || legacy[1] != digestHex(registeredHomeV2Document) {
		t.Fatalf("legacyDigestsByOperation[home] = %v, want the V1 and V2 document digests", legacy)
	}
	byDigest, err := buildOperationByDigest(
		map[string]string{"home": digestHex(registeredHomeDocument)},
		map[string][]string{"home": legacy},
	)
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"home_captured.graphql", "home_v2_captured.graphql", "home_v1_captured.graphql"} {
		text, err := os.ReadFile("testdata/wire_capture/" + name)
		if err != nil {
			t.Fatalf("read %s: %v", name, err)
		}
		operation, ok := operationForDocument(string(text), byDigest)
		if !ok || operation != "home" {
			t.Errorf("%s resolves to %q, %t; want home, true", name, operation, ok)
		}
	}
}
