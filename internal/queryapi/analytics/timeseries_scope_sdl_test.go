package analytics

import (
	"regexp"
	"strings"
	"testing"

	schemav1 "github.com/full-chaos/dev-health-ops/contracts/graphql/v1"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/graph/model"
)

// The SDL description of TimeseriesScope names the measures that carry an ORG series. It is pinned to the served set: a
// measure for which CompileOrgTimeseries has terms (orgRateTerms, orgPooledTerms) is named there, and no other measure is.
func TestTimeseriesScopeSDLNamesExactlyTheMeasuresWithAnOrgSeries(t *testing.T) {
	sdl := string(schemav1.SDL)
	const lead = "The measures with an ORG series are "
	start := strings.Index(sdl, lead)
	if start < 0 {
		t.Fatal("the TimeseriesScope description does not name the measures with an ORG series")
	}
	start += len(lead)
	sentence := sdl[start : start+strings.Index(sdl[start:], ", on the non-investment path")]
	named := map[string]bool{}
	for _, m := range regexp.MustCompile(`[A-Z][A-Z0-9_]+`).FindAllString(sentence, -1) {
		named[m] = true
	}
	served := map[string]bool{}
	for _, input := range model.AllMeasureInput {
		measure, err := measureFromInput(input)
		if err != nil {
			t.Fatal(err)
		}
		_, _, _, rate := orgRateTerms(measure)
		_, _, pooled := orgPooledTerms(measure)
		if rate || pooled {
			served[string(input)] = true
		}
	}
	for name := range served {
		if !named[name] {
			t.Errorf("%s has an ORG series and the TimeseriesScope description does not name it", name)
		}
	}
	for name := range named {
		if !served[name] {
			t.Errorf("the TimeseriesScope description names %s, which has no ORG series", name)
		}
	}
	if len(served) == 0 {
		t.Fatal("no measure has an ORG series: the test measures nothing")
	}
}
