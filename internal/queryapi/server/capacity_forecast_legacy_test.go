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
	if len(legacy) != 2 || legacy[0] != digestHex(registeredCapacityForecastV1Document) || legacy[1] != digestHex(registeredCapacityForecastV2Document) {
		t.Fatalf("legacyDigestsByOperation[capacityForecast] = %v, want exactly the V1 and the V2 digest", legacy)
	}

	byDigest, err := buildOperationByDigest(
		map[string]string{
			"capacityForecast":   digestHex(registeredCapacityForecastDocument),
			"capacityForecasts":  digestHex(registeredCapacityForecastsDocument),
			"throughputForecast": digestHex(registeredThroughputForecastDocument),
		},
		// Only this operation's legacy digests: the index refuses a legacy entry of an operation that the map
		// above does not register, and other operations get legacy texts of their own.
		map[string][]string{"capacityForecast": legacyDigestsByOperation["capacityForecast"]},
	)
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"capacityForecast.graphql", "capacityForecast.v1.graphql", "capacityForecast.v2.graphql"} {
		operation, ok := operationForDocument(readWireFormFile(t, name), byDigest)
		if !ok || operation != "capacityForecast" {
			t.Errorf("%s resolves to %q, %v; want capacityForecast, true", name, operation, ok)
		}
	}
}

// CHAOS-8477: the current text asks for the run total and for the cumulative share of each bin; the text CHAOS-7994
// registered stays accepted as the legacy V2 text.

func TestCapacityForecastV2Document_MatchesItsWireFormFixture(t *testing.T) {
	fixture := readWireFormFile(t, "capacityForecast.v2.graphql")
	if got, want := digestHex(registeredCapacityForecastV2Document), digestHex(fixture); got != want {
		t.Fatalf("registeredCapacityForecastV2Document digests to %s, its fixture to %s", got, want)
	}
	if !strings.Contains(fixture, "__typename") {
		t.Fatal("the V2 fixture carries no __typename: it is the raw web source, not a wire form")
	}
}

// The current text and V2 differ in exactly the three selections CHAOS-8477 adds: take them out of the current
// text and the V2 text is left, byte for byte.
func TestCapacityForecastCurrentAndV2_DifferByTheRunTotalAndTheCumulativeShare(t *testing.T) {
	current := registeredCapacityForecastDocument
	if strings.Count(current, "      runs\n") != 1 {
		t.Fatalf("the current capacityForecast document does not ask for runs exactly once")
	}
	if strings.Count(current, "        cumulativeShare\n") != 2 {
		t.Fatalf("the current capacityForecast document does not ask for cumulativeShare on both bin lists")
	}
	if strings.Contains(registeredCapacityForecastV2Document, "runs") || strings.Contains(registeredCapacityForecastV2Document, "cumulativeShare") {
		t.Fatal("the legacy V2 capacityForecast document asks for runs or cumulativeShare: it is not the old text")
	}
	without := strings.ReplaceAll(strings.ReplaceAll(current, "      runs\n", ""), "        cumulativeShare\n", "")
	if without != registeredCapacityForecastV2Document {
		t.Fatalf("the current document less the three new selections is not the V2 text:\n%s", without)
	}
	// No field of a dropped ticket came in with it (CHAOS-8467).
	for _, dropped := range []string{"throughputHistory", "throughputDistribution"} {
		if strings.Contains(current, dropped) {
			t.Errorf("the current capacityForecast document asks for %s", dropped)
		}
	}
}
