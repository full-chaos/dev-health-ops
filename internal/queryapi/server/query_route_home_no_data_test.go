package server

import (
	"os"
	"strings"
	"testing"
)

// CHAOS-9695 (D6014): the Home text carries no lines-of-code selection. The field left the schema, so every earlier
// Home text (each selected it) no longer validates and none is registered.
func TestHomeDocumentCarriesItsSelectionsAndNoLinesOfCode(t *testing.T) {
	for _, want := range []string{
		"\n      attribution {\n", "      coverage\n", "      rateState\n", "      rateCoverage\n", "      repoFilterApplied\n",
		"      repoLinkState\n", "      changeFailureEvidence {\n", "    filterEmptyReason\n", "scopeDataConfidence",
		"      hasData\n", "      hasPriorData\n",
	} {
		if strings.Count(registeredHomeDocument, want) == 0 {
			t.Errorf("the Home document does not select %q", strings.TrimSpace(want))
		}
	}
	reworkThemes := registeredHomeDocument[strings.Index(registeredHomeDocument, "    reworkThemeAllocation {"):]
	reworkThemes = reworkThemes[:strings.Index(reworkThemes, "    summary {")]
	if strings.Contains(reworkThemes, "churn") {
		t.Errorf("the Home document selects a lines-of-code field of reworkThemeAllocation:\n%s", reworkThemes)
	}
	if !strings.Contains(reworkThemes, "      prsMerged\n") {
		t.Errorf("the Home document lost prsMerged of reworkThemeAllocation:\n%s", reworkThemes)
	}
}

func TestHomeHasNoLegacyTextsAndTheCaptureResolvesToHome(t *testing.T) {
	if legacy, has := legacyDigestsByOperation["home"]; has {
		t.Fatalf("legacyDigestsByOperation[home] = %v, want none: every earlier text selected a field that left the schema", legacy)
	}
	if legacy, has := webPathSmokeLegacyOperations["home"]; has {
		t.Fatalf("webPathSmokeLegacyOperations[home] = %d texts, want none", len(legacy))
	}
	byDigest, err := buildOperationByDigest(map[string]string{"home": digestHex(registeredHomeDocument)}, nil)
	if err != nil {
		t.Fatal(err)
	}
	text, err := os.ReadFile("testdata/wire_capture/home_captured.graphql")
	if err != nil {
		t.Fatalf("read home_captured.graphql: %v", err)
	}
	if operation, ok := operationForDocument(string(text), byDigest); !ok || operation != "home" {
		t.Errorf("home_captured.graphql resolves to %q, %t; want home, true", operation, ok)
	}
}
