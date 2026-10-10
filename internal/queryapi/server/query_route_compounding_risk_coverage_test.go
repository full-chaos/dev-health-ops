package server

import (
	"strings"
	"testing"
)

// CHAOS-6545 dual accept: a point of `compoundingRisk` carries its `coverage` (the share of the score's weight that was
// present). The registered text selects it; the text from before stays the operation's legacy text, so a web build that
// does not select `coverage` yet keeps working while the web changes first or last.
func TestCompoundingRisk_AcceptsTheOldAndTheNewText(t *testing.T) {
	byDigest, err := buildOperationByDigest(
		map[string]string{"compoundingRisk": digestHex(registeredCompoundingRiskDocument)},
		map[string][]string{"compoundingRisk": legacyDigestsByOperation["compoundingRisk"]},
	)
	if err != nil {
		t.Fatal(err)
	}
	for name, text := range map[string]string{
		"new text": registeredCompoundingRiskDocument,
		"old text": registeredCompoundingRiskV1Document,
	} {
		if op, ok := operationForDocument(text, byDigest); !ok || op != "compoundingRisk" {
			t.Errorf("%s resolves to %q, %v; want compoundingRisk, true", name, op, ok)
		}
	}
	if strings.Contains(registeredCompoundingRiskV1Document, "coverage") {
		t.Error("the legacy text selects coverage; it must be the text from before")
	}
	if !strings.Contains(registeredCompoundingRiskDocument, "      score\n      coverage\n      severity\n") {
		t.Error("the current text does not select coverage after score in each row")
	}
	if registeredCompoundingRiskDocument == registeredCompoundingRiskV1Document {
		t.Error("the two texts must differ")
	}
	if got := legacyDigestsByOperation["compoundingRisk"]; len(got) != 1 || got[0] != digestHex(registeredCompoundingRiskV1Document) {
		t.Errorf("legacyDigestsByOperation[compoundingRisk] = %v, want the digest of the V1 text", got)
	}
}
