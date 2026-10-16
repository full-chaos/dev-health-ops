package goapiproof

// CHAOS-9905: the body-size gate must not count declared volatile subtrees (capacityForecast's drawn days bins), however the raw body is formatted.

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"
)

func volatileBinsBody(t *testing.T, plane string, counts []int, extra map[string]any) string {
	t.Helper()
	bins := make([]any, 0, len(counts))
	for i, count := range counts {
		bins = append(bins, map[string]any{"value": i + 2, "count": count, "cumulativeShare": 1.0, "__typename": "CapacityDistributionBin"})
	}
	overrides := map[string]any{"completionDistribution": map[string]any{
		"__typename": "CapacityDistribution", "runs": 10000, "unfinishedRuns": 0, "horizonDays": 30, "days": bins, "items": nil,
	}}
	for key, value := range extra {
		overrides[key] = value
	}
	return forecastBody(t, plane, overrides)
}

func reindented(t *testing.T, body, indent string) string {
	t.Helper()
	var out bytes.Buffer
	if err := json.Indent(&out, []byte(body), "", indent); err != nil {
		t.Fatal(err)
	}
	return out.String()
}

func constantBins(n int) []int {
	counts := make([]int, n)
	for i := range counts {
		counts[i] = 10000 / n
	}
	return counts
}

func TestSizeGateIgnoresTheDrawnBinsHoweverTheBodyIsFormatted(t *testing.T) {
	cases := map[string]struct{ candidate, reference string }{
		"compact 2 bins against compact 29 bins": {
			volatileBinsBody(t, "go", constantBins(2), nil), volatileBinsBody(t, "go", constantBins(29), nil)},
		"compact 2 bins against whitespace-heavy 29 bins": {
			volatileBinsBody(t, "go", constantBins(2), nil), reindented(t, volatileBinsBody(t, "go", constantBins(29), nil), strings.Repeat(" ", 12))},
		"whitespace-heavy 2 bins against compact 29 bins": {
			reindented(t, volatileBinsBody(t, "go", constantBins(2), nil), strings.Repeat(" ", 12)), volatileBinsBody(t, "go", constantBins(29), nil)},
	}
	for name, c := range cases {
		t.Run(name, func(t *testing.T) {
			result := compareForecastBodies(t, c.candidate, c.reference)
			if result.StructuralRefusal != "" || result.DifferencesOutsideBaselineDefect != 0 {
				t.Fatalf("refusal=%q %s outside=%d", result.StructuralRefusal, result.StructuralDetail, result.DifferencesOutsideBaselineDefect)
			}
		})
	}
}

// What is not declared volatile still counts: a gross size skew outside the bins is refused as before.
func TestSizeGateStillRefusesAGrossSkewOutsideTheDeclaredVolatileFields(t *testing.T) {
	skewed := map[string]any{"teamId": strings.Repeat("x", 4000)}
	result := compareForecastBodies(t,
		volatileBinsBody(t, "go", constantBins(2), skewed), volatileBinsBody(t, "go", constantBins(2), nil))
	if result.StructuralRefusal != RefusalLegsDoNotOverlap {
		t.Fatalf("a 4000-byte undeclared field was not refused on size: refusal=%q", result.StructuralRefusal)
	}
}

func decodedPair(t *testing.T, baseline, candidate string) (Snapshot, Snapshot) {
	t.Helper()
	b, err := DecodeSnapshot([]byte(baseline))
	if err != nil {
		t.Fatal(err)
	}
	c, err := DecodeSnapshot([]byte(candidate))
	if err != nil {
		t.Fatal(err)
	}
	return b, c
}

func volatileEnvelope(stable, volatileLen int, errorsJSON string) string {
	body := `{"data":{"root":{"note":"` + strings.Repeat("n", stable) + `","volatile":["` + strings.Repeat("v", volatileLen) + `"]}}`
	if errorsJSON != "" {
		body += `,"errors":` + errorsJSON
	}
	return body + `}`
}

var volatileOnlyOptions = Options{VolatileFields: map[string]string{"data.root.volatile": "drawn per request"}}

// A small stable part must not switch the check off: the adjusted sizes keep the minimum of the ratio check.
func TestSizeGateStillRefusesAGrossSkewWhenTheStablePartIsSmall(t *testing.T) {
	baseline, candidate := decodedPair(t, volatileEnvelope(5, 300, ""), volatileEnvelope(1000, 300, ""))
	if reason, _ := bodySizeDisagreement(baseline.BodyBytes, candidate.BodyBytes); reason == "" {
		t.Fatalf("setup: raw sizes %d/%d do not trip the raw gate", baseline.BodyBytes, candidate.BodyBytes)
	}
	result := Compare(baseline, candidate, volatileOnlyOptions)
	if result.StructuralRefusal != RefusalLegsDoNotOverlap {
		t.Fatalf("a 1000-byte undeclared field against a 5-byte one was not refused on size: refusal %q %s", result.StructuralRefusal, result.StructuralDetail)
	}
}

// The errors beside data are observable and are not volatile: a gross skew in them still counts.
func TestSizeGateStillRefusesAGrossSkewInTheErrors(t *testing.T) {
	small := `[{"message":"m","path":["root"]}]`
	large := `[{"message":"` + strings.Repeat("m", 2000) + `","path":["root"]}]`
	baseline, candidate := decodedPair(t, volatileEnvelope(5, 300, small), volatileEnvelope(5, 300, large))
	result := Compare(baseline, candidate, volatileOnlyOptions)
	if result.StructuralRefusal != RefusalLegsDoNotOverlap {
		t.Fatalf("a 2000-byte error message against a short one was not refused on size: refusal %q %s", result.StructuralRefusal, result.StructuralDetail)
	}
}

// Without a volatile declaration nothing is excused, and a pair the raw gate lets through is never refused by the volatile re-check.
func TestSizeGateIsTheRawGateWithoutVolatileFields(t *testing.T) {
	baseline, candidate := decodedPair(t, volatileEnvelope(5, 300, ""), volatileEnvelope(5, 1300, ""))
	if result := Compare(baseline, candidate, Options{}); result.StructuralRefusal != RefusalLegsDoNotOverlap {
		t.Fatalf("no volatile declaration: refusal %q, want the raw gate's", result.StructuralRefusal)
	}
	if result := Compare(baseline, candidate, volatileOnlyOptions); result.StructuralRefusal != "" {
		t.Fatalf("a drawn volatile list of different length was refused: %q %s", result.StructuralRefusal, result.StructuralDetail)
	}
	near, nearOther := decodedPair(t, volatileEnvelope(300, 300, ""), volatileEnvelope(300, 900, ""))
	if reason, _ := volatileAwareBodySizeDisagreement(near, nearOther, volatileOnlyOptions); reason != "" {
		t.Fatalf("a pair inside the raw ratio was refused: %q", reason)
	}
}

// compareForecastBodies compares two capacityForecast answers under the operation's own declared comparator options.
func compareForecastBodies(t *testing.T, candidate, reference string) Result {
	t.Helper()
	spec, err := SpecFor("capacityForecast")
	if err != nil {
		t.Fatal(err)
	}
	baselineSnapshot, candidateSnapshot := decodedPair(t, reference, candidate)
	return Compare(baselineSnapshot, candidateSnapshot, spec.Parity)
}

// Declaring a subtree and a path inside it must not take the inner bytes out twice.
func TestSizeGateCountsOverlappingVolatilePathsOnce(t *testing.T) {
	drawn := `"drawn":{"n":"` + strings.Repeat("v", 300) + `"}`
	baseline, candidate := decodedPair(t,
		`{"data":{"root":{"note":"`+strings.Repeat("n", 5)+`",`+drawn+`}}}`, `{"data":{"root":{"note":"`+strings.Repeat("n", 1000)+`",`+drawn+`}}}`)
	opts := Options{VolatileFields: map[string]string{"data.root.drawn": "drawn", "data.root.drawn.n": "drawn inner"}}
	if reason, _ := bodySizeDisagreement(baseline.BodyBytes, candidate.BodyBytes); reason == "" {
		t.Fatalf("setup: raw sizes %d/%d do not trip the raw gate", baseline.BodyBytes, candidate.BodyBytes)
	}
	if result := Compare(baseline, candidate, opts); result.StructuralRefusal != RefusalLegsDoNotOverlap {
		t.Fatalf("a 1000-byte undeclared field against a 5-byte one was admitted because inner volatile bytes were counted twice: %q", result.StructuralRefusal)
	}
}
