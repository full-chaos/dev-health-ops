package server

import (
	"os"
	"strings"
	"testing"
)

// edgeGoOnlyFromBirth are the registered operations that were Go-only from their first day: no Python resolver
// ever existed for them (CHAOS-8513 is the first). The /graphql edge oracle compares the answers of two planes,
// and only one plane ever had these, so there is nothing to compare: the oracle (live and frozen) leaves them
// out, by name (registeredEdgeDocuments). The frozen golden was recorded before they existed and is not recorded
// again, so it holds no request for them.
//
// An operation is listed with its registered document. Its request text is pinned by the wire fixture test, its
// fields by the registered-document gate, and its request shape by the proof spec (goapiproof.operationSpecs).
//
// This list must not become a way to stop measuring an operation the golden does measure: the test below fails
// for a listed operation that has a frozen request.
var edgeGoOnlyFromBirth = map[string]string{
	"coverageBaselines":     registeredCoverageBaselinesDocument,
	"coverageScopeBaseline": registeredCoverageScopeBaselineDocument,
	"testopsJobFailures":    registeredTestopsJobFailuresDocument,
}

func TestEdgeOracleGoOnlyOperationsHaveNoFrozenRequest(t *testing.T) {
	golden, err := os.ReadFile("testdata/venue/graphql-edge-decisions.json")
	if err != nil {
		t.Fatalf("read the frozen edge golden: %v", err)
	}
	// The golden names a request by method and operation; a measured operation has both.
	for _, measured := range []string{`"POST testopsRisk"`, `"GET testopsRisk"`} {
		if !strings.Contains(string(golden), measured) {
			t.Fatalf("the frozen edge golden holds no %s request: the label form this test reads has changed, so it measures nothing", measured)
		}
	}
	if len(edgeGoOnlyFromBirth) == 0 {
		t.Fatal("no Go-only operation is listed: remove the list and this test with the last entry")
	}
	for operation, document := range edgeGoOnlyFromBirth {
		if !strings.Contains(document, "  "+operation+"(") {
			t.Errorf("%s: the listed document does not select the operation's root field", operation)
		}
		for _, method := range []string{"POST", "GET"} {
			if label := `"` + method + " " + operation + `"`; strings.Contains(string(golden), label) {
				t.Errorf("%s is listed as Go-only from birth, but the frozen edge golden holds the request %s: it has a Python answer and must be measured", operation, label)
			}
		}
	}
}
