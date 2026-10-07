package server

import (
	"os"
	"strings"
	"testing"
)

// CHAOS-8115 on top of CHAOS-8000 dual accept: `operatingReview` has ONE current text, the one that asks whether
// each week of a metric holds data, and keeps the text it accepted before as a legacy text, so a web build still
// on the old text keeps working while the new web rolls out.

// The current text is the old text plus exactly the two selections: take them out and the legacy text is left,
// byte for byte.
func TestOperatingReviewCurrentAndV1_DifferByTheTwoDataSelections(t *testing.T) {
	// Since CHAOS-8516 the current text also asks for the scope of a metric; the text of this ticket is the
	// legacy V2 text, and it is V2 that differs from V1 by the two selections.
	current := registeredOperatingReviewV2Document
	without := current
	for _, line := range []string{"        hasData\n", "          hasPriorData\n"} {
		if strings.Count(current, line) != 1 {
			t.Fatalf("the V2 operatingReview document does not hold the line %q exactly once", line)
		}
		if strings.Contains(registeredOperatingReviewV1Document, strings.TrimSpace(line)) {
			t.Fatalf("the legacy V1 operatingReview document asks for %s: it is not the old text", strings.TrimSpace(line))
		}
		without = strings.Replace(without, line, "", 1)
	}
	if without != registeredOperatingReviewV1Document {
		t.Fatalf("the V2 document less the two selections is not the V1 text:\n%s", without)
	}
	if digestHex(current) == digestHex(registeredOperatingReviewV1Document) {
		t.Fatal("the current and the legacy text have the same digest: nothing would be dual-accepted")
	}
}

func TestOperatingReview_BothTextsResolveToTheOneOperation(t *testing.T) {
	legacy := legacyDigestsByOperation["operatingReview"]
	if len(legacy) != 2 || legacy[0] != digestHex(registeredOperatingReviewV1Document) || legacy[1] != digestHex(registeredOperatingReviewV2Document) {
		t.Fatalf("legacyDigestsByOperation[operatingReview] = %v, want exactly the V1 and the V2 digest", legacy)
	}
	byDigest, err := buildOperationByDigest(
		map[string]string{
			"operatingReview": digestHex(registeredOperatingReviewDocument),
			"reviewEdges":     digestHex(registeredReviewEdgesDocument),
		},
		// Only this operation's legacy digests: the index refuses a legacy entry of an operation that the map
		// above does not register.
		map[string][]string{"operatingReview": legacy},
	)
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"operatingreview_captured.graphql", "operatingreview_v1_captured.graphql", "operatingreview_v2_captured.graphql"} {
		text, err := os.ReadFile("testdata/wire_capture/" + name)
		if err != nil {
			t.Fatalf("read %s: %v", name, err)
		}
		operation, ok := operationForDocument(string(text), byDigest)
		if !ok || operation != "operatingReview" {
			t.Errorf("%s resolves to %q, %v; want operatingReview, true", name, operation, ok)
		}
	}
}

// CHAOS-8516: the current text is the V2 text plus exactly the scope of a metric.
func TestOperatingReviewCurrentAndV2_DifferByTheMetricScope(t *testing.T) {
	current := registeredOperatingReviewDocument
	added := "        hasData\n        scope\n"
	if strings.Count(current, added) != 1 {
		t.Fatal("the current operatingReview document does not ask for scope right after hasData, once")
	}
	if strings.Contains(registeredOperatingReviewV2Document, "scope") || strings.Contains(registeredOperatingReviewV1Document, "scope") {
		t.Fatal("a legacy operatingReview document asks for scope: it is not an old text")
	}
	if without := strings.Replace(current, added, "        hasData\n", 1); without != registeredOperatingReviewV2Document {
		t.Fatalf("the current document less the scope selection is not the V2 text:\n%s", without)
	}
	digests := map[string]bool{digestHex(current): true, digestHex(registeredOperatingReviewV1Document): true, digestHex(registeredOperatingReviewV2Document): true}
	if len(digests) != 3 {
		t.Fatal("two of the three operatingReview texts have the same digest")
	}
}
