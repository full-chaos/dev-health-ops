package server

import (
	"os"
	"strings"
	"testing"
)

func TestHomeCurrentAndLegacyDocumentsCarryTheirOwnSelections(t *testing.T) {
	if strings.Count(registeredHomeV6Document, "\n      attribution {\n") != 1 {
		t.Fatal("V6 Home document must select attribution exactly once")
	}
	if strings.Count(registeredHomeV5Document, "\n      attribution {\n") != 1 {
		t.Fatal("V5 Home document must select attribution exactly once")
	}
	if strings.Count(registeredHomeV4Document, "\n      attribution {\n") != 1 {
		t.Fatal("V4 Home document must select attribution exactly once")
	}
	for _, document := range []string{registeredHomeV3Document, registeredHomeV2Document, registeredHomeV1Document} {
		if strings.Contains(document, "\n      attribution {\n") {
			t.Fatal("a pre-CHAOS-8102 legacy Home document selects attribution")
		}
	}
	// CHAOS-6545: the current text is the V6 text plus exactly the coverage of a signal, before its attribution.
	coverage := "      coverage\n      attribution {\n"
	if strings.Count(registeredHomeDocument, coverage) != 1 {
		t.Fatal("current Home document must select a signal's coverage exactly once, before attribution")
	}
	if without := strings.Replace(registeredHomeDocument, coverage, "      attribution {\n", 1); without != registeredHomeV6Document {
		t.Fatalf("the current Home document less the coverage selection is not the V6 text:\n%s", without)
	}
	if strings.Contains(registeredHomeV6Document, "      coverage\n") {
		t.Fatal("the V6 Home document selects coverage; it must be the text from before")
	}
	// CHAOS-9072: the V6 text is the V5 text plus exactly the coverage of a rate of a delta, after its
	// state, and no legacy text asks for it.
	rateCoverage := "      rateCoverage\n"
	if strings.Count(registeredHomeV6Document, "      rateState\n"+rateCoverage) != 1 || strings.Count(registeredHomeV6Document, "rateCoverage") != 1 {
		t.Fatal("V6 Home document must select rateCoverage exactly once, directly after rateState")
	}
	if without := strings.Replace(registeredHomeV6Document, rateCoverage, "", 1); without != registeredHomeV5Document {
		t.Fatalf("the V6 Home document less the rateCoverage selection is not the V5 text:\n%s", without)
	}
	for _, document := range []string{registeredHomeV5Document, registeredHomeV4Document, registeredHomeV3Document, registeredHomeV2Document, registeredHomeV1Document} {
		if strings.Contains(document, "rateCoverage") {
			t.Fatal("a pre-CHAOS-9072 legacy Home document selects rateCoverage")
		}
	}
	// CHAOS-8981: the V5 text is the V4 text plus exactly the state of a rate of a delta, and no earlier text
	// asks for it.
	rateState := "      rateState\n"
	if strings.Count(registeredHomeV6Document, rateState) != 1 || strings.Count(registeredHomeV6Document, "rateState") != 1 {
		t.Fatal("V6 Home document must select rateState exactly once")
	}
	if strings.Count(registeredHomeV5Document, rateState) != 1 || strings.Count(registeredHomeV5Document, "rateState") != 1 {
		t.Fatal("V5 Home document must select rateState exactly once")
	}
	if without := strings.Replace(registeredHomeV5Document, rateState, "", 1); without != registeredHomeV4Document {
		t.Fatalf("the V5 Home document less the rateState selection is not the V4 text:\n%s", without)
	}
	for _, document := range []string{registeredHomeV4Document, registeredHomeV3Document, registeredHomeV2Document, registeredHomeV1Document} {
		if strings.Contains(document, "rateState") {
			t.Fatal("a pre-CHAOS-8981 legacy Home document selects rateState")
		}
	}
	if strings.Count(registeredHomeV6Document, "scopeDataConfidence") != 1 {
		t.Fatal("V6 Home document must select scopeDataConfidence exactly once")
	}
	if strings.Count(registeredHomeV3Document, "scopeDataConfidence") != 1 || strings.Count(registeredHomeV4Document, "scopeDataConfidence") != 1 || strings.Count(registeredHomeV5Document, "scopeDataConfidence") != 1 {
		t.Fatal("V3, V4 and V5 Home documents must each select scopeDataConfidence exactly once")
	}
	if strings.Contains(registeredHomeV2Document, "scopeDataConfidence") || strings.Contains(registeredHomeV1Document, "scopeDataConfidence") {
		t.Fatal("a pre-CHAOS-8107 legacy Home document selects scopeDataConfidence")
	}
	for _, line := range []string{"      hasData\n", "      hasPriorData\n"} {
		if strings.Count(registeredHomeV6Document, line) != 1 || strings.Count(registeredHomeV5Document, line) != 1 || strings.Count(registeredHomeV4Document, line) != 1 || strings.Count(registeredHomeV3Document, line) != 1 || strings.Count(registeredHomeV2Document, line) != 1 {
			t.Fatalf("V6, V5, V4, V3, and V2 Home documents must each contain %q once", line)
		}
		if strings.Contains(registeredHomeV1Document, strings.TrimSpace(line)) {
			t.Fatalf("V1 Home document contains %q", strings.TrimSpace(line))
		}
	}
	documents := map[string]string{
		"current": registeredHomeDocument,
		"V6":      registeredHomeV6Document,
		"V5":      registeredHomeV5Document,
		"V4":      registeredHomeV4Document,
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
	if len(legacy) != 6 || legacy[0] != digestHex(registeredHomeV1Document) || legacy[1] != digestHex(registeredHomeV2Document) || legacy[2] != digestHex(registeredHomeV3Document) || legacy[3] != digestHex(registeredHomeV4Document) || legacy[4] != digestHex(registeredHomeV5Document) || legacy[5] != digestHex(registeredHomeV6Document) {
		t.Fatalf("legacyDigestsByOperation[home] = %v, want the V1 to V6 document digests", legacy)
	}
	byDigest, err := buildOperationByDigest(
		map[string]string{"home": digestHex(registeredHomeDocument)},
		map[string][]string{"home": legacy},
	)
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"home_captured.graphql", "home_v6_captured.graphql", "home_v5_captured.graphql", "home_v4_captured.graphql", "home_v3_captured.graphql", "home_v2_captured.graphql", "home_v1_captured.graphql"} {
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
