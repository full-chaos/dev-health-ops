package server

import (
	"os"
	"strings"
	"testing"
)

const testopsTestWindowReportCompletenessSelection = `    reportCompleteness {
      reportRuns
      partialRuns
      unmarkedRuns
      unmeasuredDays
      __typename
    }
`

// The current testopsTestWindow text selects reportCompleteness. The texts a web build sent before it (V2, V1) stay
// registered, so an older web keeps working while the new web rolls out.
func TestTestopsTestWindowReportCompletenessDocuments_AllTextsRoute(t *testing.T) {
	byDigest, err := buildOperationByDigest(
		map[string]string{"testopsTestWindow": digestHex(registeredTestopsTestWindowDocument)},
		map[string][]string{"testopsTestWindow": legacyDigestsByOperation["testopsTestWindow"]},
	)
	if err != nil {
		t.Fatal(err)
	}
	for name, fixture := range map[string]string{
		"current wire text": "testdata/wire_capture/testopstestwindow_captured.graphql",
		"V2 wire text":      "testdata/wire_capture/testopstestwindow_v2_captured.graphql",
		"V1 wire text":      "testdata/wire_capture/testopstestwindow_v1_captured.graphql",
	} {
		raw, err := os.ReadFile(fixture)
		if err != nil {
			t.Fatal(err)
		}
		if op, ok := operationForDocument(string(raw), byDigest); !ok || op != "testopsTestWindow" {
			t.Errorf("%s resolves to %q, %v; want testopsTestWindow, true", name, op, ok)
		}
	}
}

// The current text is V2 plus exactly the reportCompleteness selection, and the legacy order is V1, V2.
func TestTestopsTestWindowReportCompletenessDocuments_CurrentIsV2PlusTheSelection(t *testing.T) {
	if strings.Contains(registeredTestopsTestWindowV2Document, "reportCompleteness") || strings.Contains(registeredTestopsTestWindowV1Document, "reportCompleteness") {
		t.Fatal("a legacy text selects reportCompleteness")
	}
	if !strings.Contains(registeredTestopsTestWindowDocument, testopsTestWindowReportCompletenessSelection) {
		t.Fatal("the current text does not select reportCompleteness with its four counts")
	}
	if got := strings.Replace(registeredTestopsTestWindowDocument, testopsTestWindowReportCompletenessSelection, "", 1); got != registeredTestopsTestWindowV2Document {
		t.Errorf("the current text less the reportCompleteness selection is not V2:\n%s", got)
	}
	legacy := legacyDigestsByOperation["testopsTestWindow"]
	if len(legacy) != 2 || legacy[0] != digestHex(registeredTestopsTestWindowV1Document) || legacy[1] != digestHex(registeredTestopsTestWindowV2Document) {
		t.Errorf("legacyDigestsByOperation[testopsTestWindow] = %v, want V1 then V2", legacy)
	}
}

// testopsTestWindow took filterEmptyReason in V2 (the filterEmptyReason documents test covers the other TestOps documents):
// V2 is V1 plus exactly that selection.
func TestTestopsTestWindowV2IsV1PlusTheFilterEmptyReasonSelection(t *testing.T) {
	const line = "    filterEmptyReason\n"
	if strings.Contains(registeredTestopsTestWindowV1Document, "filterEmptyReason") {
		t.Fatal("V1 selects filterEmptyReason")
	}
	if !strings.Contains(registeredTestopsTestWindowV2Document, line) {
		t.Fatal("V2 does not select filterEmptyReason")
	}
	if got := strings.Replace(registeredTestopsTestWindowV2Document, line, "", 1); got != registeredTestopsTestWindowV1Document {
		t.Errorf("V2 less the filterEmptyReason selection is not V1:\n%s", got)
	}
}
