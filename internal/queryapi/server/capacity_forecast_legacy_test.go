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
	if len(legacy) != 3 || legacy[0] != digestHex(registeredCapacityForecastV1Document) ||
		legacy[1] != digestHex(registeredCapacityForecastV2Document) || legacy[2] != digestHex(registeredCapacityForecastV3Document) {
		t.Fatalf("legacyDigestsByOperation[capacityForecast] = %v, want exactly the V1, the V2 and the V3 digest", legacy)
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
	for _, name := range []string{"capacityForecast.graphql", "capacityForecast.v1.graphql", "capacityForecast.v2.graphql", "capacityForecast.v3.graphql"} {
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

// The current text and V2 differ in exactly the selections CHAOS-8477 adds (the run total, the unfinished runs and
// the horizon on the distribution; the cumulative share on both bin lists): take them out of the current text and
// the V2 text is left, byte for byte.
func TestCapacityForecastCurrentAndV2_DifferByTheRunTotalAndTheCumulativeShare(t *testing.T) {
	current := withoutBacklogState(t, registeredCapacityForecastDocument)
	without := current
	for line, times := range map[string]int{
		"      runs\n": 1, "      unfinishedRuns\n": 1, "      horizonDays\n": 1, "        cumulativeShare\n": 2,
	} {
		field := strings.TrimSpace(line)
		if strings.Count(current, line) != times {
			t.Fatalf("the current capacityForecast document does not ask for %s exactly %d time(s)", field, times)
		}
		if strings.Contains(registeredCapacityForecastV2Document, field) {
			t.Fatalf("the legacy V2 capacityForecast document asks for %s: it is not the old text", field)
		}
		without = strings.ReplaceAll(without, line, "")
	}
	if without != registeredCapacityForecastV2Document {
		t.Fatalf("the current document less the new selections is not the V2 text:\n%s", without)
	}
	// No field of a dropped ticket came in with it (CHAOS-8467).
	for _, dropped := range []string{"throughputHistory", "throughputDistribution"} {
		if strings.Contains(current, dropped) {
			t.Errorf("the current capacityForecast document asks for %s", dropped)
		}
	}
}

// CHAOS-7508: the current text asks how its backlogSize was found; the text CHAOS-8477 registered stays accepted as the
// legacy V3 text.

func TestCapacityForecastV3Document_MatchesItsWireFormFixture(t *testing.T) {
	fixture := readWireFormFile(t, "capacityForecast.v3.graphql")
	if got, want := digestHex(registeredCapacityForecastV3Document), digestHex(fixture); got != want {
		t.Fatalf("registeredCapacityForecastV3Document digests to %s, its fixture to %s", got, want)
	}
	if !strings.Contains(fixture, "__typename") {
		t.Fatal("the V3 fixture carries no __typename: it is the raw web source, not a wire form")
	}
}

// withoutBacklogState is the current capacityForecast text less the two selections CHAOS-7508 added after backlogSize.
func withoutBacklogState(t *testing.T, document string) string {
	t.Helper()
	const added = "    backlogSize\n    backlogState\n    backlogAsOfDay\n"
	if strings.Count(document, added) != 1 {
		t.Fatal("the current capacityForecast document does not ask for backlogState and backlogAsOfDay after backlogSize exactly once")
	}
	return strings.Replace(document, added, "    backlogSize\n", 1)
}

// Take backlogState and backlogAsOfDay out of the current text and the V3 text is left, byte for byte.
func TestCapacityForecastCurrentAndV3_DifferByTheBacklogState(t *testing.T) {
	for _, field := range []string{"backlogState", "backlogAsOfDay"} {
		if strings.Contains(registeredCapacityForecastV3Document, field) {
			t.Fatalf("the legacy V3 capacityForecast document asks for %s: it is not the old text", field)
		}
	}
	if without := withoutBacklogState(t, registeredCapacityForecastDocument); without != registeredCapacityForecastV3Document {
		t.Fatalf("the current document less backlogState and backlogAsOfDay is not the V3 text:\n%s", without)
	}
}

// CHAOS-7508: the list operation returns the same CapacityForecast type; its current text asks for backlogState and
// backlogAsOfDay on the node, and the text CHAOS-5349 registered stays accepted as the legacy V1 text.

func TestCapacityForecastsV1Document_MatchesItsWireFormFixture(t *testing.T) {
	fixture := readWireFormFile(t, "capacityForecasts.v1.graphql")
	if got, want := digestHex(registeredCapacityForecastsV1Document), digestHex(fixture); got != want {
		t.Fatalf("registeredCapacityForecastsV1Document digests to %s, its fixture to %s", got, want)
	}
	if !strings.Contains(fixture, "__typename") {
		t.Fatal("the V1 fixture carries no __typename: it is the raw web source, not a wire form")
	}
}

// Take backlogState and backlogAsOfDay out of the current list text and the V1 text is left, byte for byte.
func TestCapacityForecastsCurrentAndV1_DifferByTheBacklogState(t *testing.T) {
	const added = "        backlogSize\n        backlogState\n        backlogAsOfDay\n"
	if strings.Count(registeredCapacityForecastsDocument, added) != 1 {
		t.Fatal("the current capacityForecasts document does not ask for backlogState and backlogAsOfDay after backlogSize exactly once")
	}
	for _, field := range []string{"backlogState", "backlogAsOfDay"} {
		if strings.Contains(registeredCapacityForecastsV1Document, field) {
			t.Fatalf("the legacy V1 capacityForecasts document asks for %s: it is not the old text", field)
		}
	}
	if without := strings.Replace(registeredCapacityForecastsDocument, added, "        backlogSize\n", 1); without != registeredCapacityForecastsV1Document {
		t.Fatalf("the current list document less backlogState and backlogAsOfDay is not the V1 text:\n%s", without)
	}
}

func TestCapacityForecasts_BothTextsResolveToTheOneOperation(t *testing.T) {
	legacy := legacyDigestsByOperation["capacityForecasts"]
	if len(legacy) != 1 || legacy[0] != digestHex(registeredCapacityForecastsV1Document) {
		t.Fatalf("legacyDigestsByOperation[capacityForecasts] = %v, want exactly the V1 digest", legacy)
	}
	byDigest, err := buildOperationByDigest(
		map[string]string{"capacityForecasts": digestHex(registeredCapacityForecastsDocument)},
		map[string][]string{"capacityForecasts": legacy},
	)
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"capacityForecasts.graphql", "capacityForecasts.v1.graphql"} {
		operation, ok := operationForDocument(readWireFormFile(t, name), byDigest)
		if !ok || operation != "capacityForecasts" {
			t.Errorf("%s resolves to %q, %v; want capacityForecasts, true", name, operation, ok)
		}
	}
}
