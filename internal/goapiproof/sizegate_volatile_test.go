package goapiproof

// CHAOS-9905: the body-size gate must not count declared volatile subtrees (capacityForecast's drawn days bins), however the raw body is formatted.

import (
	"bytes"
	"context"
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
			outcomes, _ := stochasticDocRouteRun(t, c.candidate, c.reference)
			if o := outcomes[0]; !o.Executed || o.RefusalReason != "" || o.DifferencesOutsideBaselineDefect != 0 {
				t.Fatalf("executed=%v refusal=%q %s outside=%d", o.Executed, o.RefusalReason, o.RefusalDetail, o.DifferencesOutsideBaselineDefect)
			}
		})
	}
}

// What is not declared volatile still counts: a gross size skew outside the bins is refused as before.
func TestSizeGateStillRefusesAGrossSkewOutsideTheDeclaredVolatileFields(t *testing.T) {
	skewed := map[string]any{"teamId": strings.Repeat("x", 4000)}
	outcomes, _ := runStochasticDocRoute(t,
		volatileBinsBody(t, "go", constantBins(2), skewed), volatileBinsBody(t, "go", constantBins(2), nil))
	if o := outcomes[0]; o.Executed || o.RefusalReason != RefusalLegsDoNotOverlap {
		t.Fatalf("a 4000-byte undeclared field was not refused on size: executed=%v refusal=%q", o.Executed, o.RefusalReason)
	}
}

func TestSizeGateBytesIsTheRawLengthWithoutVolatileFields(t *testing.T) {
	snapshot := Snapshot{Data: map[string]any{"a": []any{1, 2}}, DataPresent: true, BodyBytes: 4321}
	if got := sizeGateBytes(snapshot, Options{}); got != 4321 {
		t.Fatalf("got %d, want the raw body length 4321", got)
	}
}

// runStochasticDocRoute is stochasticDocRouteRun without its Fatal on a run error: a refused run returns no outcome through Run's error.
func runStochasticDocRoute(t *testing.T, candidate, reference string) ([]Outcome, error) {
	t.Helper()
	runner := newDocRouteRunner(t, goLeg(candidate), goLeg(reference), nil)
	runner.Documents = map[string]string{"capacityForecast": capacityForecastDocument(t)}
	runner.Registry.DocumentDigest = map[string]string{"capacityForecast": "b4fb8f07"}
	runner.Routing = map[string]RoutingRow{"capacityForecast": {Mode: "shadow", CandidateBuild: goEdgeBuild}}
	outcomes, _, err := runner.Run(context.Background())
	return outcomes, err
}
