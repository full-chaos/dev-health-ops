package server

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// CHAOS-7994 on top of CHAOS-8000 dual accept: capacityForecast has ONE current text (the one that asks for
// completionDistribution) and keeps the text it accepted before as a legacy text, so a web build still on the
// old text keeps working while the new web rolls out.

func readWireFormFile(t *testing.T, name string) string {
	t.Helper()
	data, err := os.ReadFile(filepath.Join("testdata", "wire_form", name))
	if err != nil {
		t.Fatalf("read %s: %v", name, err)
	}
	return string(data)
}

func TestCapacityForecastV1Document_MatchesItsWireFormFixture(t *testing.T) {
	fixture := readWireFormFile(t, "capacityForecast.v1.graphql")
	if got, want := digestHex(registeredCapacityForecastV1Document), digestHex(fixture); got != want {
		t.Fatalf("registeredCapacityForecastV1Document digests to %s, its fixture to %s", got, want)
	}
	if !strings.Contains(fixture, "__typename") {
		t.Fatal("the V1 fixture carries no __typename: it is the raw web source, not a wire form")
	}
}

// The two texts differ in exactly one thing the page cares about: only the current one asks for the distribution.
func TestCapacityForecastCurrentAndV1_DifferByTheDistributionSelection(t *testing.T) {
	if !strings.Contains(registeredCapacityForecastDocument, "completionDistribution") {
		t.Error("the current capacityForecast document does not ask for completionDistribution")
	}
	if strings.Contains(registeredCapacityForecastV1Document, "completionDistribution") {
		t.Error("the legacy V1 capacityForecast document asks for completionDistribution: it is not the old text")
	}
	withoutBlock := strings.ReplaceAll(registeredCapacityForecastDocument, "completionDistribution", "")
	if len(withoutBlock) <= len(registeredCapacityForecastV1Document) {
		t.Error("the current document is not the V1 text plus a selection")
	}
	if digestHex(registeredCapacityForecastDocument) == digestHex(registeredCapacityForecastV1Document) {
		t.Fatal("the current and the legacy text have the same digest: nothing would be dual-accepted")
	}
}

func TestCapacityForecast_BothTextsResolveToTheOneOperation(t *testing.T) {
	legacy := legacyDigestsByOperation["capacityForecast"]
	if len(legacy) != 1 || legacy[0] != digestHex(registeredCapacityForecastV1Document) {
		t.Fatalf("legacyDigestsByOperation[capacityForecast] = %v, want exactly the V1 digest", legacy)
	}

	byDigest, err := buildOperationByDigest(
		map[string]string{
			"capacityForecast":   digestHex(registeredCapacityForecastDocument),
			"capacityForecasts":  digestHex(registeredCapacityForecastsDocument),
			"throughputForecast": digestHex(registeredThroughputForecastDocument),
		},
		legacyDigestsByOperation,
	)
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"capacityForecast.graphql", "capacityForecast.v1.graphql"} {
		operation, ok := operationForDocument(readWireFormFile(t, name), byDigest)
		if !ok || operation != "capacityForecast" {
			t.Errorf("%s resolves to %q, %v; want capacityForecast, true", name, operation, ok)
		}
	}
}
