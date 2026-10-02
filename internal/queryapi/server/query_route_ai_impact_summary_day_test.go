package server

import (
	"os"
	"strings"
	"testing"
)

// CHAOS-7992: the web Impact page asks aiImpactSummary for `daily.day` (the field the Go API added with
// CHAOS-7774). query-api matches a request to its operation by the digest of the EXACT text, so the registered
// document and the captured wire fixture must carry that selection, and the wire-parity pair (the web branch
// of the same name) must agree.
func TestRegisteredAiImpactSummaryDocument_RequestsDayInDaily(t *testing.T) {
	raw, err := os.ReadFile("testdata/wire_capture/aiimpactsummary_captured.graphql")
	if err != nil {
		t.Fatal(err)
	}
	for name, doc := range map[string]string{
		"registered const":      registeredAiImpactSummaryDocument,
		"captured wire fixture": string(raw),
	} {
		start := strings.Index(doc, "daily {")
		if start < 0 {
			t.Fatalf("%s: no daily selection", name)
		}
		daily := doc[start:]
		daily = daily[:strings.Index(daily, "}")]
		if !strings.Contains(daily, "\n      day\n") {
			t.Errorf("%s: the daily selection does not request day:\n%s", name, daily)
		}
	}
}

// CHAOS-8000 dual accept: the OLD text (no daily.day) is the operation's legacy text.
func TestAiImpactSummary_AcceptsTheOldAndTheNewText(t *testing.T) {
	byDigest, err := buildOperationByDigest(
		map[string]string{"aiImpactSummary": digestHex(registeredAiImpactSummaryDocument)},
		map[string][]string{"aiImpactSummary": legacyDigestsByOperation["aiImpactSummary"]},
	)
	if err != nil {
		t.Fatal(err)
	}
	for name, text := range map[string]string{
		"new text": registeredAiImpactSummaryDocument,
		"old text": registeredAiImpactSummaryV1Document,
	} {
		if op, ok := operationForDocument(text, byDigest); !ok || op != "aiImpactSummary" {
			t.Errorf("%s resolves to %q, %v; want aiImpactSummary, true", name, op, ok)
		}
	}
	daily := func(doc string) string {
		s := doc[strings.Index(doc, "daily {"):]
		return s[:strings.Index(s, "}")]
	}
	if strings.Contains(daily(registeredAiImpactSummaryV1Document), "\n      day\n") {
		t.Error("the legacy text requests day; it must be the text from before")
	}
	if !strings.Contains(daily(registeredAiImpactSummaryDocument), "\n      day\n") {
		t.Error("the current text does not request day")
	}
	if got := legacyDigestsByOperation["aiImpactSummary"]; len(got) != 1 || got[0] != digestHex(registeredAiImpactSummaryV1Document) {
		t.Errorf("legacyDigestsByOperation[aiImpactSummary] = %v, want the digest of the V1 text", got)
	}
}
