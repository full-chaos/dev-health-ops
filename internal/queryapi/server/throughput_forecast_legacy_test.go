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

func TestThroughputForecastV2Document_MatchesItsWireFormFixture(t *testing.T) {
	fixture := readWireFormFile(t, "throughputForecast.v2.graphql")
	if got, want := digestHex(registeredThroughputForecastV2Document), digestHex(fixture); got != want {
		t.Fatalf("registeredThroughputForecastV2Document digests to %s, its fixture to %s", got, want)
	}
	if !strings.Contains(fixture, "__typename") {
		t.Fatal("the V2 fixture carries no __typename: it is the raw web source, not a wire form")
	}
}

func TestThroughputForecastV3Document_MatchesItsWireFormFixture(t *testing.T) {
	fixture := readWireFormFile(t, "throughputForecast.v3.graphql")
	if got, want := digestHex(registeredThroughputForecastV3Document), digestHex(fixture); got != want {
		t.Fatalf("registeredThroughputForecastV3Document digests to %s, its fixture to %s", got, want)
	}
	if !strings.Contains(fixture, "__typename") {
		t.Fatal("the V3 fixture carries no __typename: it is the raw web source, not a wire form")
	}
}

// withoutNewestDayFields is the current throughputForecast text less the selections CHAOS-7508 added: the three
// wipCongestion fields, state, asOfDay and grainCount of staleWip, state and asOfDay of estimateCoverage. Each segment
// must be in the text exactly once.
func withoutNewestDayFields(t *testing.T, document string) string {
	t.Helper()
	for _, cut := range []struct{ from, to string }{
		{"    wipCongestionState\n    wipCongestionAsOfDay\n    wipCongestionGrainCount\n", ""},
		{"      p90AgeHours\n      state\n      asOfDay\n      grainCount\n", "      p90AgeHours\n"},
		{"      grainCount\n      state\n      asOfDay\n", "      grainCount\n"},
	} {
		if strings.Count(document, cut.from) != 1 {
			t.Fatalf("the current throughputForecast document does not hold %q exactly once", cut.from)
		}
		document = strings.Replace(document, cut.from, cut.to, 1)
	}
	return document
}

// CHAOS-7508: take the newest-day selections out of the current text and the V3 text is left, byte for byte.
func TestThroughputForecastCurrentAndV3_DifferByTheNewestDayFields(t *testing.T) {
	for _, field := range []string{"wipCongestionState", "wipCongestionAsOfDay", "wipCongestionGrainCount", "asOfDay", "      state\n"} {
		if strings.Contains(registeredThroughputForecastV3Document, field) {
			t.Fatalf("the legacy V3 throughputForecast document asks for %q: it is not the old text", field)
		}
	}
	if without := withoutNewestDayFields(t, registeredThroughputForecastDocument); without != registeredThroughputForecastV3Document {
		t.Fatalf("the current document less the newest-day fields is not the V3 text:\n%s", without)
	}
}

// CHAOS-14142: take the newest-day selections and the degradedReason selection out of the current text and the V2
// text is left, byte for byte.
func TestThroughputForecastCurrentAndV2_DifferByTheDegradedReason(t *testing.T) {
	const line = "    degradedReason\n"
	if strings.Count(registeredThroughputForecastDocument, line) != 1 {
		t.Fatal("the current throughputForecast document does not ask for degradedReason exactly once")
	}
	if strings.Contains(registeredThroughputForecastV2Document, "degradedReason") {
		t.Fatal("the legacy V2 throughputForecast document asks for degradedReason: it is not the old text")
	}
	if without := strings.ReplaceAll(withoutNewestDayFields(t, registeredThroughputForecastDocument), line, ""); without != registeredThroughputForecastV2Document {
		t.Fatalf("the current document less degradedReason is not the V2 text:\n%s", without)
	}
}

// Take the newest-day selections and the four selections after V1 out of the current text and the V1 text is left,
// byte for byte.
func TestThroughputForecastCurrentAndV1_DifferByTheBacklogStateTheDegradedReasonAndTheGrainCount(t *testing.T) {
	current := withoutNewestDayFields(t, registeredThroughputForecastDocument)
	without := current
	for _, line := range []string{"    backlogState\n", "    backlogAsOfDay\n", "    degradedReason\n", "      grainCount\n"} {
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
	if len(legacy) != 3 ||
		legacy[0] != digestHex(registeredThroughputForecastV1Document) ||
		legacy[1] != digestHex(registeredThroughputForecastV2Document) ||
		legacy[2] != digestHex(registeredThroughputForecastV3Document) {
		t.Fatalf("legacyDigestsByOperation[throughputForecast] = %v, want exactly the V1, V2 and V3 digests", legacy)
	}
	byDigest, err := buildOperationByDigest(
		map[string]string{"throughputForecast": digestHex(registeredThroughputForecastDocument)},
		map[string][]string{"throughputForecast": legacy},
	)
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"throughputForecast.graphql", "throughputForecast.v1.graphql", "throughputForecast.v2.graphql", "throughputForecast.v3.graphql"} {
		operation, ok := operationForDocument(readWireFormFile(t, name), byDigest)
		if !ok || operation != "throughputForecast" {
			t.Errorf("%s resolves to %q, %v; want throughputForecast, true", name, operation, ok)
		}
	}
}
