package server

import (
	"os"
	"strings"
	"testing"
)

// CHAOS-8682: the web coverage views send repoIds and teamIds to restrict the
// scope baseline. The current registered text must carry each argument; the
// V1 text remains only to serve an already-deployed unscoped web build.
func TestCoverageScopeBaselineCurrentTextCarriesScopeArguments(t *testing.T) {
	current, err := os.ReadFile("testdata/wire_capture/coveragescopebaseline_captured.graphql")
	if err != nil {
		t.Fatal(err)
	}
	for name, document := range map[string]string{
		"registered current document":   string(registeredCoverageScopeBaselineDocument),
		"captured scoped wire document": string(current),
	} {
		for _, argument := range []string{"$repoIds: [String!]", "$teamIds: [String!]", "repoIds: $repoIds", "teamIds: $teamIds"} {
			if strings.Count(document, argument) != 1 {
				t.Errorf("%s has %d occurrences of %q; want exactly one", name, strings.Count(document, argument), argument)
			}
		}
	}
	if strings.Contains(registeredCoverageScopeBaselineV1Document, "$repoIds") || strings.Contains(registeredCoverageScopeBaselineV1Document, "$teamIds") {
		t.Fatal("the V1 document carries a scope argument; it is not the prior unscoped text")
	}
}

func TestCoverageScopeBaselineBothTextsResolveToOneOperation(t *testing.T) {
	legacy := legacyDigestsByOperation["coverageScopeBaseline"]
	if len(legacy) != 1 || legacy[0] != digestHex(registeredCoverageScopeBaselineV1Document) {
		t.Fatalf("legacyDigestsByOperation[coverageScopeBaseline] = %v, want exactly the V1 digest", legacy)
	}
	byDigest, err := buildOperationByDigest(
		map[string]string{"coverageScopeBaseline": digestHex(registeredCoverageScopeBaselineDocument)},
		map[string][]string{"coverageScopeBaseline": legacy},
	)
	if err != nil {
		t.Fatal(err)
	}
	for _, fixture := range []string{
		"coveragescopebaseline_captured.graphql",
		"coveragescopebaseline_v1_captured.graphql",
	} {
		document, err := os.ReadFile("testdata/wire_capture/" + fixture)
		if err != nil {
			t.Fatalf("read %s: %v", fixture, err)
		}
		operation, ok := operationForDocument(string(document), byDigest)
		if !ok || operation != "coverageScopeBaseline" {
			t.Errorf("%s resolves to %q, %v; want coverageScopeBaseline, true", fixture, operation, ok)
		}
	}
}
