package server

import (
	"strings"
	"testing"
)

// CHAOS-8167, CHAOS-8172: throughputForecast has ONE current text (the one that asks how the backlog was found and
// how many coverage grains were read) and keeps the text CHAOS-5349 registered as a legacy text, so a web build still
// on the old text keeps working while the new web rolls out.

func TestThroughputForecastV1Document_MatchesItsWireFormFixture(t *testing.T) {
	fixture := readWireFormFile(t, "throughputForecast.v1.graphql")
	if got, want := digestHex(registeredThroughputForecastV1Document), digestHex(fixture); got != want {
		t.Fatalf("registeredThroughputForecastV1Document digests to %s, its fixture to %s", got, want)
	}
	if !strings.Contains(fixture, "__typename") {
		t.Fatal("the V1 fixture carries no __typename: it is the raw web source, not a wire form")
	}
}

// Take the three new selections out of the current text and the V1 text is left, byte for byte.
func TestThroughputForecastCurrentAndV1_DifferByTheBacklogStateAndTheGrainCount(t *testing.T) {
	current := registeredThroughputForecastDocument
	without := current
	for _, line := range []string{"    backlogState\n", "    backlogAsOfDay\n", "      grainCount\n"} {
		field := strings.TrimSpace(line)
		if strings.Count(current, line) != 1 {
			t.Fatalf("the current throughputForecast document does not ask for %s exactly once", field)
		}
		if strings.Contains(registeredThroughputForecastV1Document, field) {
			t.Fatalf("the legacy V1 throughputForecast document asks for %s: it is not the old text", field)
		}
		without = strings.ReplaceAll(without, line, "")
	}
	if without != registeredThroughputForecastV1Document {
		t.Fatalf("the current document less the new selections is not the V1 text:\n%s", without)
	}
}

func TestThroughputForecast_BothTextsResolveToTheOneOperation(t *testing.T) {
	legacy := legacyDigestsByOperation["throughputForecast"]
	if len(legacy) != 1 || legacy[0] != digestHex(registeredThroughputForecastV1Document) {
		t.Fatalf("legacyDigestsByOperation[throughputForecast] = %v, want exactly the V1 digest", legacy)
	}
	byDigest, err := buildOperationByDigest(
		map[string]string{"throughputForecast": digestHex(registeredThroughputForecastDocument)},
		map[string][]string{"throughputForecast": legacy},
	)
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"throughputForecast.graphql", "throughputForecast.v1.graphql"} {
		operation, ok := operationForDocument(readWireFormFile(t, name), byDigest)
		if !ok || operation != "throughputForecast" {
			t.Errorf("%s resolves to %q, %v; want throughputForecast, true", name, operation, ok)
		}
	}
}
