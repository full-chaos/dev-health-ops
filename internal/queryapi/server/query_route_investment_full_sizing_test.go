package server

import (
	"os"
	"strings"
	"testing"
)

// CHAOS-9808: the web Investment page selects sizingCoverage and unsizedUnitCount on the Sankey coverage. query-api
// matches a request to its operation by the digest of the EXACT text, so the text the web sends now and the text it
// sent before must both route: an unregistered text is a 404 before GraphQL runs.
func TestInvestmentFullSizingDocuments_BothTextsRoute(t *testing.T) {
	byDigest, err := buildOperationByDigest(
		map[string]string{"investmentFull": digestHex(registeredInvestmentFullDocument)},
		map[string][]string{"investmentFull": legacyDigestsByOperation["investmentFull"]},
	)
	if err != nil {
		t.Fatal(err)
	}
	for name, fixture := range map[string]string{
		"new wire text": "testdata/wire_capture/investmentfull_captured.graphql",
		"old wire text": "testdata/wire_capture/investmentfull_v1_captured.graphql",
	} {
		raw, err := os.ReadFile(fixture)
		if err != nil {
			t.Fatal(err)
		}
		if op, ok := operationForDocument(string(raw), byDigest); !ok || op != "investmentFull" {
			t.Errorf("%s resolves to %q, %v; want investmentFull, true", name, op, ok)
		}
	}
	if digestHex(registeredInvestmentFullDocument) != "f8b95e4455abf87f4dc7a13a147cac27535367db174d8e136d672c77b9bcbaa1" {
		t.Errorf("the current InvestmentFull digest is %s, not the web's", digestHex(registeredInvestmentFullDocument))
	}
	if got := legacyDigestsByOperation["investmentFull"]; len(got) != 1 || got[0] != "663e516c6152c81e28780d27809fcbe5aeae8c92b07d34b19978b7d59913cb3d" {
		t.Errorf("legacyDigestsByOperation[investmentFull] = %v, want the digest of the text before CHAOS-9808", got)
	}
	for _, field := range []string{"        sizingCoverage\n", "        unsizedUnitCount\n"} {
		if !strings.Contains(registeredInvestmentFullDocument, field) || strings.Contains(registeredInvestmentFullV1Document, field) {
			t.Errorf("the current text must select %q and the V1 text must not", strings.TrimSpace(field))
		}
	}
}
