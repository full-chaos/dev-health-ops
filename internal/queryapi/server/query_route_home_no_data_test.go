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
	// CHAOS-9098: the current text is the V9 text plus exactly the answer's filterEmptyReason, the last selection
	// of home, after scopeDataConfidence; no older text asks for it.
	emptyReasonLine := "    filterEmptyReason\n"
	if strings.Count(registeredHomeDocument, "filterEmptyReason") != 1 || strings.Count(registeredHomeDocument, emptyReasonLine+"    __typename\n  }\n}") != 1 {
		t.Fatal("current Home document must select filterEmptyReason exactly once, last in home")
	}
	if without := strings.Replace(registeredHomeDocument, emptyReasonLine, "", 1); without != registeredHomeV9Document {
		t.Fatalf("the current Home document less the filterEmptyReason selection is not the V9 text:\n%s", without)
	}
	if strings.Contains(registeredHomeV9Document, "filterEmptyReason") {
		t.Fatal("the V9 Home document selects filterEmptyReason; it must be the text from before")
	}
	// CHAOS-9094: the V9 text is the V8 text plus exactly the four link fields of a delta, after its
	// repoFilterApplied; no legacy text asks for them.
	linkFields := "      repoLinkState\n      repoLinkBasis {\n        native\n        explicitText\n        heuristic\n        __typename\n      }\n      repoLinkMultiRepoItems\n      repoLinkCoverage {\n        linkedItems\n        itemsInWindow\n        __typename\n      }\n"
	if strings.Count(registeredHomeV9Document, linkFields) != 1 || strings.Count(registeredHomeV9Document, "repoLink") != 4 {
		t.Fatal("V9 Home document must select the four repoLink fields exactly once, together")
	}
	if strings.Count(registeredHomeV9Document, "      repoFilterApplied\n"+linkFields) != 1 {
		t.Fatal("V9 Home document must select the repoLink fields right after a delta's repoFilterApplied")
	}
	if without := strings.Replace(registeredHomeV9Document, linkFields, "", 1); without != registeredHomeV8Document {
		t.Fatalf("the V9 Home document less the repoLink selections is not the V8 text:\n%s", without)
	}
	if strings.Contains(registeredHomeV8Document, "repoLink") {
		t.Fatal("the V8 Home document selects a repoLink field; it must be the text from before")
	}
	// CHAOS-9093: the V8 text is the V7 text plus exactly the repository-filter flag of a delta (after
	// rateCoverage) and of a signal (after coverage); no older text asks for it.
	filterLine := "      repoFilterApplied\n"
	if strings.Count(registeredHomeV8Document, filterLine) != 2 || strings.Count(registeredHomeV8Document, "repoFilterApplied") != 2 {
		t.Fatal("V8 Home document must select repoFilterApplied exactly twice (deltas, signals)")
	}
	if strings.Count(registeredHomeV8Document, "      rateCoverage\n"+filterLine) != 1 || strings.Count(registeredHomeV8Document, "      coverage\n"+filterLine+"      attribution {\n") != 1 {
		t.Fatal("V8 Home document must select repoFilterApplied after rateCoverage and after a signal's coverage")
	}
	if without := strings.ReplaceAll(registeredHomeV8Document, filterLine, ""); without != registeredHomeV7Document {
		t.Fatalf("the V8 Home document less the repoFilterApplied selections is not the V7 text:\n%s", without)
	}
	if strings.Contains(registeredHomeV7Document, "repoFilterApplied") {
		t.Fatal("the V7 Home document selects repoFilterApplied; it must be the text from before")
	}
	// CHAOS-6545: the V7 text is the V6 text plus exactly the coverage of a signal, before its attribution.
	coverage := "      coverage\n      attribution {\n"
	if strings.Count(registeredHomeV7Document, coverage) != 1 {
		t.Fatal("current Home document must select a signal's coverage exactly once, before attribution")
	}
	if without := strings.Replace(registeredHomeV7Document, coverage, "      attribution {\n", 1); without != registeredHomeV6Document {
		t.Fatalf("the V7 Home document less the coverage selection is not the V6 text:\n%s", without)
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
		"V9":      registeredHomeV9Document,
		"V8":      registeredHomeV8Document,
		"V7":      registeredHomeV7Document,
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
	if len(legacy) != 9 || legacy[0] != digestHex(registeredHomeV1Document) || legacy[1] != digestHex(registeredHomeV2Document) || legacy[2] != digestHex(registeredHomeV3Document) || legacy[3] != digestHex(registeredHomeV4Document) || legacy[4] != digestHex(registeredHomeV5Document) || legacy[5] != digestHex(registeredHomeV6Document) || legacy[6] != digestHex(registeredHomeV7Document) || legacy[7] != digestHex(registeredHomeV8Document) || legacy[8] != digestHex(registeredHomeV9Document) {
		t.Fatalf("legacyDigestsByOperation[home] = %v, want the V1 to V9 document digests", legacy)
	}
	byDigest, err := buildOperationByDigest(
		map[string]string{"home": digestHex(registeredHomeDocument)},
		map[string][]string{"home": legacy},
	)
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"home_captured.graphql", "home_v9_captured.graphql", "home_v8_captured.graphql", "home_v7_captured.graphql", "home_v6_captured.graphql", "home_v5_captured.graphql", "home_v4_captured.graphql", "home_v3_captured.graphql", "home_v2_captured.graphql", "home_v1_captured.graphql"} {
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
