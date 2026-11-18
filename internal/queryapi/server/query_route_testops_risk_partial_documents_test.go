package server

import (
	"os"
	"strings"
	"testing"
)

// CHAOS-14001: the current testopsRisk text selects qualityDragPartial. The text a web build sent before it (V2) and
// the older V1 stay registered, so an older web keeps working while the new web rolls out.
func TestTestopsRiskPartialDocuments_AllTextsRoute(t *testing.T) {
	byDigest, err := buildOperationByDigest(
		map[string]string{"testopsRisk": digestHex(registeredTestopsRiskDocument)},
		map[string][]string{"testopsRisk": legacyDigestsByOperation["testopsRisk"]},
	)
	if err != nil {
		t.Fatal(err)
	}
	for name, fixture := range map[string]string{
		"current wire text": "testdata/wire_capture/testopsrisk_captured.graphql",
		"V2 wire text":      "testdata/wire_capture/testopsrisk_v2_captured.graphql",
		"V1 wire text":      "testdata/wire_capture/testopsrisk_v1_captured.graphql",
	} {
		raw, err := os.ReadFile(fixture)
		if err != nil {
			t.Fatal(err)
		}
		if op, ok := operationForDocument(string(raw), byDigest); !ok || op != "testopsRisk" {
			t.Errorf("%s resolves to %q, %v; want testopsRisk, true", name, op, ok)
		}
	}
}

// The current text is V2 plus exactly the qualityDragPartial selection.
func TestTestopsRiskPartialDocuments_CurrentIsV2PlusThePartialSelection(t *testing.T) {
	if strings.Contains(registeredTestopsRiskV2Document, "qualityDragPartial") || strings.Contains(registeredTestopsRiskV1Document, "qualityDragPartial") {
		t.Fatal("an older text selects qualityDragPartial")
	}
	var kept []string
	selected := 0
	for _, line := range strings.Split(registeredTestopsRiskDocument, "\n") {
		if strings.TrimSpace(line) == "qualityDragPartial" {
			selected++
			continue
		}
		kept = append(kept, line)
	}
	if selected != 1 {
		t.Fatalf("the current text selects qualityDragPartial %d times, want 1", selected)
	}
	if got := strings.Join(kept, "\n"); got != registeredTestopsRiskV2Document {
		t.Errorf("the current text less qualityDragPartial is not V2:\n%s", got)
	}
	if digestHex(registeredTestopsRiskDocument) == digestHex(registeredTestopsRiskV2Document) {
		t.Error("the current and the V2 text have the same digest: nothing would be dual-accepted")
	}
}
