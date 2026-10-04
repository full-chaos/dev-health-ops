package server

import (
	"os"
	"strings"
	"testing"
)

func TestHomeCurrentAndLegacyDocumentsCarryTheirOwnSelections(t *testing.T) {
	if strings.Count(registeredHomeDocument, "\n      attribution {\n") != 1 {
		t.Fatal("current Home document must select attribution exactly once")
	}
	for _, document := range []string{registeredHomeV3Document, registeredHomeV2Document, registeredHomeV1Document} {
		if strings.Contains(document, "\n      attribution {\n") {
			t.Fatal("a pre-CHAOS-8102 legacy Home document selects attribution")
		}
	}
	if strings.Count(registeredHomeDocument, "scopeDataConfidence") != 1 {
		t.Fatal("current Home document must select scopeDataConfidence exactly once")
	}
	if strings.Count(registeredHomeV3Document, "scopeDataConfidence") != 1 {
		t.Fatal("V3 Home document must select scopeDataConfidence exactly once")
	}
	if strings.Contains(registeredHomeV2Document, "scopeDataConfidence") || strings.Contains(registeredHomeV1Document, "scopeDataConfidence") {
		t.Fatal("a pre-CHAOS-8107 legacy Home document selects scopeDataConfidence")
	}
	for _, line := range []string{"      hasData\n", "      hasPriorData\n"} {
		if strings.Count(registeredHomeDocument, line) != 1 || strings.Count(registeredHomeV3Document, line) != 1 || strings.Count(registeredHomeV2Document, line) != 1 {
			t.Fatalf("current, V3, and V2 Home documents must each contain %q once", line)
		}
		if strings.Contains(registeredHomeV1Document, strings.TrimSpace(line)) {
			t.Fatalf("V1 Home document contains %q", strings.TrimSpace(line))
		}
	}
	documents := map[string]string{
		"current": registeredHomeDocument,
		"V3":      registeredHomeV3Document,
		"V2":      registeredHomeV2Document,
		"V1":      registeredHomeV1Document,
	}
	digests := make(map[string]string, len(documents))
	for name, document := range documents {
		digest := digestHex(document)
		if other, exists := digests[digest]; exists {
			t.Fatalf("%s and %s Home documents share digest %s", name, other, digest)
		}
		digests[digest] = name
	}
}

func TestHomeCurrentAndLegacyTextsResolveToHome(t *testing.T) {
	legacy := legacyDigestsByOperation["home"]
	if len(legacy) != 3 || legacy[0] != digestHex(registeredHomeV1Document) || legacy[1] != digestHex(registeredHomeV2Document) || legacy[2] != digestHex(registeredHomeV3Document) {
		t.Fatalf("legacyDigestsByOperation[home] = %v, want the V1, V2, and V3 document digests", legacy)
	}
	byDigest, err := buildOperationByDigest(
		map[string]string{"home": digestHex(registeredHomeDocument)},
		map[string][]string{"home": legacy},
	)
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"home_captured.graphql", "home_v3_captured.graphql", "home_v2_captured.graphql", "home_v1_captured.graphql"} {
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
