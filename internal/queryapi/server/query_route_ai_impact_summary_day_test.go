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
