package server

import (
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/venueoracle"
)

// The ruled divergence of sankey.coverage on a window with nothing measurable
// (CHAOS-6129, D5806, "unknown is not 0"): the recorded Python answer for an
// empty org is the object {teamCoverage: 0, repoCoverage: 0}, the same answer
// as a measured 0 % covered; query-api answers null. The recorded Python answer
// stays as recorded (the frozen JSON is not edited).
//
// Like the throughputForecast declaration beside it, this is NOT a whole-answer
// divergence: the one field is blanked to the same placeholder on both sides
// (sankeyCoverageDivergenceNormalize) so every other field of the answer is
// still compared by Diff, and the raw Go answer is asserted to carry exactly
// null there (sankeyCoverageDivergenceInspect). The blank applies only when the
// recorded Python value is exactly {0, 0}: any other recorded object stays as
// recorded and differs from the blanked Go answer.
const (
	pythonZeroSankeyCoverage = `"coverage":{"teamCoverage":0,"repoCoverage":0,"__typename":"SankeyCoverage"},"unit":"WORK_UNITS"`
	goNullSankeyCoverage     = `"coverage":null,"unit":"WORK_UNITS"`
	declaredSankeyBlanked    = `"coverage":"<declared D5806: nothing measurable>","unit":"WORK_UNITS"`
)

func isInvestmentFullRequest(request venueoracle.Request) bool {
	return strings.HasSuffix(request.Name, " investmentFull")
}

func sankeyCoverageDivergenceNormalize(request venueoracle.Request, body string) string {
	if !isInvestmentFullRequest(request) {
		return body
	}
	return strings.ReplaceAll(strings.ReplaceAll(body, pythonZeroSankeyCoverage, declaredSankeyBlanked), goNullSankeyCoverage, declaredSankeyBlanked)
}

// sankeyCoverageDivergenceInspect holds the Go side of the declaration: the
// answer to an investmentFull request of the venue's empty org carries a null
// sankey coverage, never {0, 0} and never another object.
func sankeyCoverageDivergenceInspect(request venueoracle.Request, goBody string) (bool, string) {
	if !isInvestmentFullRequest(request) {
		return true, ""
	}
	if !strings.Contains(goBody, goNullSankeyCoverage) {
		return false, "query-api must answer a null sankey coverage when nothing is measurable (D5806), got: " + truncateOracleBody(goBody)
	}
	return true, ""
}

func TestSankeyCoverageDivergenceMaskIsOneFieldWide(t *testing.T) {
	request := venueoracle.Request{Name: "POST investmentFull"}
	wrap := func(coverage, extra string) string {
		return `{"data":{"analytics":{"breakdowns":[{"items":[` + extra + `]}],"sankey":{"nodes":[],"edges":[],` + coverage + `}}}}`
	}
	python := wrap(pythonZeroSankeyCoverage, "")
	goNull := wrap(goNullSankeyCoverage, "")
	if sankeyCoverageDivergenceNormalize(request, python) != sankeyCoverageDivergenceNormalize(request, goNull) {
		t.Fatal("the declared pair (Python {0,0}, Go null) does not normalize equal")
	}
	// Another field's difference stays visible after the blank.
	if sankeyCoverageDivergenceNormalize(request, python) == sankeyCoverageDivergenceNormalize(request, wrap(goNullSankeyCoverage, `{"x":1}`)) {
		t.Fatal("another field's difference is masked: the mask is wider than sankey.coverage")
	}
	// A Go answer of {0,0} or of any other object is NOT blanked into the declared value, so it differs from the blanked Python record.
	goZero := wrap(pythonZeroSankeyCoverage, "")
	goHalf := wrap(`"coverage":{"teamCoverage":0.5,"repoCoverage":0,"__typename":"SankeyCoverage"},"unit":"WORK_UNITS"`, "")
	blankedPython := sankeyCoverageDivergenceNormalize(request, python)
	if sankeyCoverageDivergenceNormalize(request, goHalf) == blankedPython {
		t.Fatal("a measured coverage object was blanked to the declared value")
	}
	// Go serving {0,0} is blanked the same as the record (both sides carry the same recorded value), so it is the Inspect that must refuse it.
	if ok, _ := sankeyCoverageDivergenceInspect(request, goZero); ok {
		t.Fatal("inspect accepted a Go answer of {0,0}")
	}
	if ok, _ := sankeyCoverageDivergenceInspect(request, goHalf); ok {
		t.Fatal("inspect accepted a Go answer of a coverage object")
	}
	if ok, why := sankeyCoverageDivergenceInspect(request, goNull); !ok {
		t.Fatalf("inspect refused the declared Go answer: %s", why)
	}
	// A recorded Python value that is NOT {0,0} stays as recorded: the declaration does not hide a changed record.
	recordedChanged := wrap(`"coverage":{"teamCoverage":0.1,"repoCoverage":0,"__typename":"SankeyCoverage"},"unit":"WORK_UNITS"`, "")
	if sankeyCoverageDivergenceNormalize(request, recordedChanged) == sankeyCoverageDivergenceNormalize(request, goNull) {
		t.Fatal("a changed recorded Python value was hidden by the declaration")
	}
	// Another operation is left alone.
	other := venueoracle.Request{Name: "POST somethingElse"}
	if sankeyCoverageDivergenceNormalize(other, python) != python {
		t.Fatal("a non-investmentFull answer was changed")
	}
	if ok, _ := sankeyCoverageDivergenceInspect(other, "{}"); !ok {
		t.Fatal("inspect judged another operation")
	}
}
