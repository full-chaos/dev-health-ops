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
	current := registeredOperatingReviewDocument
	without := current
	for _, line := range []string{"        hasData\n", "          hasPriorData\n"} {
		if strings.Count(current, line) != 1 {
			t.Fatalf("the current operatingReview document does not hold the line %q exactly once", line)
		}
		if strings.Contains(registeredOperatingReviewV1Document, strings.TrimSpace(line)) {
			t.Fatalf("the legacy V1 operatingReview document asks for %s: it is not the old text", strings.TrimSpace(line))
		}
		without = strings.Replace(without, line, "", 1)
	}
	if without != registeredOperatingReviewV1Document {
		t.Fatalf("the current document less the two selections is not the V1 text:\n%s", without)
	}
	if digestHex(current) == digestHex(registeredOperatingReviewV1Document) {
		t.Fatal("the current and the legacy text have the same digest: nothing would be dual-accepted")
	}
}

func TestOperatingReview_BothTextsResolveToTheOneOperation(t *testing.T) {
	legacy := legacyDigestsByOperation["operatingReview"]
	if len(legacy) != 1 || legacy[0] != digestHex(registeredOperatingReviewV1Document) {
		t.Fatalf("legacyDigestsByOperation[operatingReview] = %v, want exactly the V1 digest", legacy)
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
	for _, name := range []string{"operatingreview_captured.graphql", "operatingreview_v1_captured.graphql"} {
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
