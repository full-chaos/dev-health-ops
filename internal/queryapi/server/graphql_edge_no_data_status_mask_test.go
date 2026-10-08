package server

import (
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/venueoracle"
)

// The ruled divergence of operatingReview.sections.metrics.delta.status for a week with no data
// (CHAOS-8525, #3873): the recorded Python answer says "unchanged" on a delta whose value, prior
// value, absolute and percent are all zero; query-api serves "" there, because no data is not
// "unchanged". The recorded Python answer stays as recorded.
//
// The mask is ONE field wide and only on an all-zero delta: the status of that delta is blanked to
// the same placeholder on both sides (noDataStatusNormalize), so every other field of the answer is
// still compared by Diff, and the raw Go answer is asserted to carry no "unchanged" on an all-zero
// delta (noDataStatusInspect), so a regression back to "unchanged" fails instead of being blanked.
const (
	zeroDeltaPrefix   = `"delta":{"value":0,"priorValue":0,"absolute":0,"percent":0,"status":`
	pythonNoDataState = zeroDeltaPrefix + `"unchanged"`
	goNoDataState     = zeroDeltaPrefix + `""`
	declaredNoData    = zeroDeltaPrefix + `"<declared CHAOS-8525: no-data week>"`
)

func isOperatingReviewRequest(request venueoracle.Request) bool {
	return strings.HasSuffix(request.Name, " operatingReview")
}

func noDataStatusNormalize(request venueoracle.Request, body string) string {
	if !isOperatingReviewRequest(request) {
		return body
	}
	return strings.ReplaceAll(strings.ReplaceAll(body, pythonNoDataState, declaredNoData), goNoDataState, declaredNoData)
}

func noDataStatusInspect(request venueoracle.Request, goBody string) (bool, string) {
	if isOperatingReviewRequest(request) && strings.Contains(goBody, pythonNoDataState) {
		return false, "query-api must serve an empty status on an all-zero delta (CHAOS-8525), not \"unchanged\""
	}
	return true, ""
}

// A pure test, so it runs in the unit leg and not only with the venue.
func TestNoDataStatusMaskIsOneFieldWide(t *testing.T) {
	request := venueoracle.Request{Name: "POST operatingReview"}
	wrap := func(delta string) string {
		return `{"data":{"operatingReview":{"metrics":[{"key":"k","value":0,` + delta + `}]}}}`
	}
	python := wrap(pythonNoDataState + `}`)
	goBody := wrap(goNoDataState + `}`)
	if noDataStatusNormalize(request, python) != noDataStatusNormalize(request, goBody) {
		t.Fatal("the declared pair does not normalize equal")
	}
	// A non-zero delta keeps its status: a different status there still differs.
	moved := `"delta":{"value":2,"priorValue":1,"absolute":1,"percent":100,"status":`
	if noDataStatusNormalize(request, wrap(moved+`"worsened"}`)) == noDataStatusNormalize(request, wrap(moved+`"unchanged"}`)) {
		t.Fatal("a non-zero delta's status is masked: the mask is wider than the all-zero delta")
	}
	// Another status on an all-zero delta is not blanked into the declared value.
	if noDataStatusNormalize(request, wrap(zeroDeltaPrefix+`"improved"}`)) == noDataStatusNormalize(request, goBody) {
		t.Fatal("another status on an all-zero delta was blanked to the declared value")
	}
	// Another operation is left alone.
	other := venueoracle.Request{Name: "POST home"}
	if noDataStatusNormalize(other, python) != python {
		t.Fatal("an answer of another operation was changed")
	}
	if ok, _ := noDataStatusInspect(request, python); ok {
		t.Fatal("a Go answer of \"unchanged\" on an all-zero delta passes the inspection")
	}
	if ok, why := noDataStatusInspect(request, goBody); !ok {
		t.Fatalf("the intended Go answer fails the inspection: %s", why)
	}
}
