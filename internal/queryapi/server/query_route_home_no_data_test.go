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
	if strings.Count(registeredHomeV4Document, "\n      attribution {\n") != 1 || strings.Count(registeredHomeV5Document, "\n      attribution {\n") != 1 {
		t.Fatal("V4 and V5 Home documents must select attribution exactly once")
	}
	for _, document := range []string{registeredHomeV3Document, registeredHomeV2Document, registeredHomeV1Document} {
		if strings.Contains(document, "\n      attribution {\n") {
			t.Fatal("a pre-CHAOS-8102 legacy Home document selects attribution")
		}
	}
	// CHAOS-8981: the current text is the V4 text plus exactly the state of change failure rate of a delta, and
	// no legacy text asks for it.
	rateState := "      rateState\n"
	if strings.Count(registeredHomeDocument, rateState) != 1 || strings.Count(registeredHomeDocument, "rateState") != 1 {
		t.Fatal("current Home document must select rateState exactly once")
	}
	// CHAOS-9094: the current text is the V7 text plus exactly the four link fields of a delta, after its
	// repoFilterApplied; no legacy text asks for them.
	linkFields := "      repoLinkState\n      repoLinkBasis {\n        native\n        explicitText\n        heuristic\n        __typename\n      }\n      repoLinkMultiRepoItems\n      repoLinkCoverage {\n        linkedItems\n        itemsInWindow\n        __typename\n      }\n"
	if strings.Count(registeredHomeDocument, linkFields) != 1 || strings.Count(registeredHomeDocument, "repoLink") != 4 {
		t.Fatal("current Home document must select the four repoLink fields exactly once, together")
	}
	if strings.Count(registeredHomeDocument, "      repoFilterApplied\n"+linkFields) != 1 {
		t.Fatal("current Home document must select the repoLink fields right after a delta's repoFilterApplied")
	}
	if without := strings.Replace(registeredHomeDocument, linkFields, "", 1); without != registeredHomeV7Document {
		t.Fatalf("the current Home document less the repoLink selections is not the V7 text:\n%s", without)
	}
	if strings.Contains(registeredHomeV7Document, "repoLink") {
		t.Fatal("the V7 Home document selects a repoLink field; it must be the text from before")
	}
	// CHAOS-9093: the V7 text is the V6 text plus exactly the repository-filter flag of a delta (after
	// rateState) and of a signal (after coverage); no older text asks for it.
	filterLine := "      repoFilterApplied\n"
	if strings.Count(registeredHomeV7Document, filterLine) != 2 || strings.Count(registeredHomeV7Document, "repoFilterApplied") != 2 {
		t.Fatal("V7 Home document must select repoFilterApplied exactly twice (deltas, signals)")
	}
	if strings.Count(registeredHomeV7Document, "      rateState\n"+filterLine) != 1 || strings.Count(registeredHomeV7Document, "      coverage\n"+filterLine+"      attribution {\n") != 1 {
		t.Fatal("V7 Home document must select repoFilterApplied after rateState and after a signal's coverage")
	}
	if without := strings.ReplaceAll(registeredHomeV7Document, filterLine, ""); without != registeredHomeV6Document {
		t.Fatalf("the V7 Home document less the repoFilterApplied selections is not the V6 text:\n%s", without)
	}
	if strings.Contains(registeredHomeV6Document, "repoFilterApplied") {
		t.Fatal("the V6 Home document selects repoFilterApplied; it must be the text from before")
	}
	// CHAOS-6545: the V6 text is the V5 text plus exactly the coverage of a signal.
	coverage := "      coverage\n      attribution {\n"
	if strings.Count(registeredHomeV6Document, coverage) != 1 {
		t.Fatal("V6 Home document must select a signal's coverage exactly once, before attribution")
	}
	if without := strings.Replace(registeredHomeV6Document, coverage, "      attribution {\n", 1); without != registeredHomeV5Document {
		t.Fatalf("the V6 Home document less the coverage selection is not the V5 text:\n%s", without)
	}
	if strings.Contains(registeredHomeV5Document, "      coverage\n") {
		t.Fatal("the V5 Home document selects coverage; it must be the text from before")
	}
	if without := strings.Replace(registeredHomeV5Document, rateState, "", 1); without != registeredHomeV4Document {
		t.Fatalf("the V5 Home document less the rateState selection is not the V4 text:\n%s", without)
	}
	for _, document := range []string{registeredHomeV4Document, registeredHomeV3Document, registeredHomeV2Document, registeredHomeV1Document} {
		if strings.Contains(document, "rateState") {
			t.Fatal("a pre-CHAOS-8981 legacy Home document selects rateState")
		}
	}
	if strings.Count(registeredHomeDocument, "scopeDataConfidence") != 1 {
		t.Fatal("current Home document must select scopeDataConfidence exactly once")
	}
	if strings.Count(registeredHomeV3Document, "scopeDataConfidence") != 1 || strings.Count(registeredHomeV4Document, "scopeDataConfidence") != 1 {
		t.Fatal("V3 and V4 Home documents must each select scopeDataConfidence exactly once")
	}
	if strings.Contains(registeredHomeV2Document, "scopeDataConfidence") || strings.Contains(registeredHomeV1Document, "scopeDataConfidence") {
		t.Fatal("a pre-CHAOS-8107 legacy Home document selects scopeDataConfidence")
	}
	for _, line := range []string{"      hasData\n", "      hasPriorData\n"} {
		if strings.Count(registeredHomeDocument, line) != 1 || strings.Count(registeredHomeV4Document, line) != 1 || strings.Count(registeredHomeV3Document, line) != 1 || strings.Count(registeredHomeV2Document, line) != 1 {
			t.Fatalf("current, V4, V3, and V2 Home documents must each contain %q once", line)
		}
		if strings.Contains(registeredHomeV1Document, strings.TrimSpace(line)) {
			t.Fatalf("V1 Home document contains %q", strings.TrimSpace(line))
		}
	}
	documents := map[string]string{
		"current": registeredHomeDocument,
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
	if len(legacy) != 7 || legacy[0] != digestHex(registeredHomeV1Document) || legacy[1] != digestHex(registeredHomeV2Document) || legacy[2] != digestHex(registeredHomeV3Document) || legacy[3] != digestHex(registeredHomeV4Document) || legacy[4] != digestHex(registeredHomeV5Document) || legacy[5] != digestHex(registeredHomeV6Document) || legacy[6] != digestHex(registeredHomeV7Document) {
		t.Fatalf("legacyDigestsByOperation[home] = %v, want the V1, V2, V3, V4, V5, V6 and V7 document digests", legacy)
	}
	byDigest, err := buildOperationByDigest(
		map[string]string{"home": digestHex(registeredHomeDocument)},
		map[string][]string{"home": legacy},
	)
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"home_captured.graphql", "home_v7_captured.graphql", "home_v6_captured.graphql", "home_v5_captured.graphql", "home_v4_captured.graphql", "home_v3_captured.graphql", "home_v2_captured.graphql", "home_v1_captured.graphql"} {
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
