package server

import (
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/venueoracle"
)

// The ruled divergence of throughputForecast.estimateCoverage on a scope with no
// throughput history and no estimate-coverage rows (the venue's seed): the
// recorded Python answer is null (forecast.py skipped the coverage read when the
// backlog was 0), and query-api answers the ZERO object, ratio 0 included, by
// chris's decision "Keep zero" (D4373, read as D4376 by the lead: coverage is
// built from its own rows, an all-zero object when there are none, never null).
// The recorded Python answer stays as recorded.
//
// It is NOT a declared whole-answer divergence: that would compare two strings
// and nothing else of the body. Instead the one field is blanked to the same
// placeholder on both sides (coverageDivergenceNormalize) so every other field
// of the answer is still compared by Diff, and the raw Go answer is asserted to
// carry exactly the zero object (coverageDivergenceInspect) so a Go answer of
// null, or of any other object, fails instead of being blanked away.
const (
	pythonNullCoverage      = `"estimateCoverage":null`
	goZeroCoverage          = `"estimateCoverage":{"ratio":0,"estimatedCount":0,"unestimatedCount":0,"backlogSize":0,"__typename":"ThroughputEstimateCoverage"}`
	declaredCoverageBlanked = `"estimateCoverage":"<declared D4373/D4376: no coverage rows>"`
)

func isThroughputForecastRequest(request venueoracle.Request) bool {
	return strings.HasSuffix(request.Name, " throughputForecast")
}

func coverageDivergenceNormalize(request venueoracle.Request, body string) string {
	if !isThroughputForecastRequest(request) {
		return body
	}
	return strings.ReplaceAll(strings.ReplaceAll(body, pythonNullCoverage, declaredCoverageBlanked), goZeroCoverage, declaredCoverageBlanked)
}

// The mask is ONE field wide (D4383): the declared pair (Python null, query-api
// zero object) normalizes equal, a difference in ANY other field of the answer
// still differs after normalization, and an answer of another operation is left
// alone. A pure test, so it runs in the unit leg and not only with the venue.
func TestCoverageDivergenceMaskIsOneFieldWide(t *testing.T) {
	request := venueoracle.Request{Name: "POST throughputForecast"}
	python := `{"data":{"throughputForecast":{"insufficientHistory":true,"estimateCoverage":null}}}`
	goBody := `{"data":{"throughputForecast":{"insufficientHistory":true,` + goZeroCoverage + `}}}`
	if coverageDivergenceNormalize(request, python) != coverageDivergenceNormalize(request, goBody) {
		t.Fatal("the declared pair does not normalize equal")
	}
	goOther := `{"data":{"throughputForecast":{"insufficientHistory":false,` + goZeroCoverage + `}}}`
	if coverageDivergenceNormalize(request, python) == coverageDivergenceNormalize(request, goOther) {
		t.Fatal("another field's difference is masked: the mask is wider than estimateCoverage")
	}
	// A Go answer of null, or of a non-zero object, is NOT blanked into the declared value.
	goNull := `{"data":{"throughputForecast":{"insufficientHistory":true,"estimateCoverage":null}}}`
	goNonZero := `{"data":{"throughputForecast":{"insufficientHistory":true,"estimateCoverage":{"ratio":0,"estimatedCount":0,"unestimatedCount":1,"backlogSize":1,"__typename":"ThroughputEstimateCoverage"}}}}`
	if coverageDivergenceNormalize(request, goNonZero) == coverageDivergenceNormalize(request, goBody) {
		t.Fatal("a non-zero coverage object was blanked to the declared value")
	}
	if !strings.Contains(coverageDivergenceNormalize(request, goNull), declaredCoverageBlanked) {
		t.Fatal("the Python null is not blanked (the pair would differ on the declared field)")
	}
	other := venueoracle.Request{Name: "POST somethingElse"}
	if coverageDivergenceNormalize(other, python) != python {
		t.Fatal("a non-throughputForecast answer was changed")
	}
}
