package server

import (
	"context"
	"regexp"
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
// still compared by Diff, the mask applies ONLY under a precondition the oracle checks at run time
// (operatingReviewFixtureHasNoRows: the metric tables the review reads hold no row, so both weeks are
// missing, the case #3873 clears at operatingreview.go:1466-68: a week with no stored row, never a
// stored 0), and the raw Go answer is asserted to carry no "unchanged" on an all-zero
// delta (noDataStatusInspect), so a regression back to "unchanged" fails instead of being blanked.
const (
	zeroDeltaPrefix   = `"delta":{"value":0,"priorValue":0,"absolute":0,"percent":0,"status":`
	pythonNoDataState = zeroDeltaPrefix + `"unchanged"`
	// CHAOS-9111: the Go answer of a week with no data also has a null percent (a percent has no meaning
	// against a value nobody measured), where the recorded Python answer holds 0. The mask covers the pair
	// as one declared state, under the same precondition and only on an all-zero delta.
	goNoDataPrefix = `"delta":{"value":0,"priorValue":0,"absolute":0,"percent":null,"status":`
	goNoDataState  = goNoDataPrefix + `""`
	declaredNoData = zeroDeltaPrefix + `"<declared CHAOS-8525 / CHAOS-9111: no-data week>"`
)

// operatingReviewTables are the ClickHouse tables the operating review reads (operatingreview.go).
var operatingReviewTables = []string{
	"ai_governance_coverage_daily", "ai_impact_metrics_daily", "deploy_metrics_daily", "file_hotspot_daily",
	"incident_metrics_daily", "investment_metrics_daily", "repo_complexity_daily", "repo_metrics_daily",
	"work_item_metrics_daily", "work_item_state_durations_daily",
}

// operatingReviewTablesEmpty is set by operatingReviewFixtureHasNoRows. The mask applies only when it is true.
var operatingReviewTablesEmpty bool

// operatingReviewFixtureHasNoRows reads the row count of every table the review reads, in the Go copy of
// the venue. Any row is a loud failure (not a skip) and leaves the mask off.
func operatingReviewFixtureHasNoRows(t *testing.T, ctx context.Context, uri string) {
	t.Helper()
	empty := true
	for _, table := range operatingReviewTables {
		if count := venueoracle.CHRows(t, ctx, uri, "SELECT count() FROM "+table); count != "0" {
			t.Errorf("%s holds %s row(s): the no-data mask of the operatingReview oracle (CHAOS-8525) requires the fixture to hold none", table, count)
			empty = false
		}
	}
	operatingReviewTablesEmpty = empty
}

func isOperatingReviewRequest(request venueoracle.Request) bool {
	return strings.HasSuffix(request.Name, " operatingReview")
}

func noDataStatusNormalize(request venueoracle.Request, body string, tablesEmpty bool) string {
	if !tablesEmpty || !isOperatingReviewRequest(request) {
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
	if noDataStatusNormalize(request, python, true) != noDataStatusNormalize(request, goBody, true) {
		t.Fatal("the declared pair does not normalize equal")
	}
	// A non-zero delta keeps its status: a different status there still differs.
	moved := `"delta":{"value":2,"priorValue":1,"absolute":1,"percent":100,"status":`
	if noDataStatusNormalize(request, wrap(moved+`"worsened"}`), true) == noDataStatusNormalize(request, wrap(moved+`"unchanged"}`), true) {
		t.Fatal("a non-zero delta's status is masked: the mask is wider than the all-zero delta")
	}
	// Another status on an all-zero delta is not blanked into the declared value.
	if noDataStatusNormalize(request, wrap(zeroDeltaPrefix+`"improved"}`), true) == noDataStatusNormalize(request, goBody, true) {
		t.Fatal("another status on an all-zero delta was blanked to the declared value")
	}
	// Another operation is left alone.
	other := venueoracle.Request{Name: "POST home"}
	if noDataStatusNormalize(other, python, true) != python {
		t.Fatal("an answer of another operation was changed")
	}
	if ok, _ := noDataStatusInspect(request, python); ok {
		t.Fatal("a Go answer of \"unchanged\" on an all-zero delta passes the inspection")
	}
	if ok, why := noDataStatusInspect(request, goBody); !ok {
		t.Fatalf("the intended Go answer fails the inspection: %s", why)
	}
}

// The planted defect: a 0-vs-0 week WITH stored rows for which Go clears the status (the defect
// #3873 must not have: a stored zero keeps its status, operatingreview.go:1466-68). The earlier
// mask, which keyed on the all-zero delta alone, equalized it with the recorded "unchanged" and
// hid it; the mask under its precondition (a venue with stored rows: tables not empty) does not.
func TestNoDataStatusMaskDoesNotHideAZeroWeekWithStoredRows(t *testing.T) {
	request := venueoracle.Request{Name: "GET operatingReview"}
	wrap := func(delta string) string {
		return `{"data":{"operatingReview":{"metrics":[{"key":"k","value":0,` + delta + `}]}}}`
	}
	python := wrap(pythonNoDataState + `}`)
	goClearedOnStoredZero := wrap(goNoDataState + `}`)

	const tablesEmpty, tablesHoldRows = true, false
	if noDataStatusNormalize(request, python, tablesEmpty) != noDataStatusNormalize(request, goClearedOnStoredZero, tablesEmpty) {
		t.Fatal("the earlier mask (precondition assumed true) would have hidden the planted defect: this guard no longer shows that")
	}
	if noDataStatusNormalize(request, python, tablesHoldRows) == noDataStatusNormalize(request, goClearedOnStoredZero, tablesHoldRows) {
		t.Fatal("with stored rows in the fixture the cleared status of a stored 0-vs-0 week is hidden")
	}
}

// The ruled divergence of operatingReview.sections.metrics (CHAOS-8981): query-api serves two metrics the
// recorded Python answer never had, deployment_failure_rate (the deployment-status ratio that used to be
// stored under the name change_failure_rate) and revert_rate. Only those two metric objects are removed, on
// operating review answers, from both sides; every other metric and field is still compared by Diff. The
// raw Go answer is asserted to carry both (goOnlyMetricsInspect), so dropping one fails instead of being masked.
var goOnlyMetricObject = regexp.MustCompile(`,\{"key":"(?:deployment_failure_rate|revert_rate)",[^{}]*"delta":\{[^{}]*\}[^{}]*\}`)

func goOnlyMetricsNormalize(request venueoracle.Request, body string) string {
	if !isOperatingReviewRequest(request) {
		return body
	}
	return goOnlyMetricObject.ReplaceAllString(body, "")
}

func goOnlyMetricsInspect(request venueoracle.Request, goBody string) (bool, string) {
	if !isOperatingReviewRequest(request) || !strings.HasPrefix(goBody, `{"data":{`) {
		return true, ""
	}
	for _, key := range []string{"deployment_failure_rate", "revert_rate"} {
		if !strings.Contains(goBody, `"key":"`+key+`"`) {
			return false, "query-api must serve the metric " + key + " (CHAOS-8981)"
		}
	}
	return true, ""
}

// A pure test, so it runs in the unit leg and not only with the venue.
func TestGoOnlyMetricsMaskRemovesOnlyThoseTwoMetrics(t *testing.T) {
	request := venueoracle.Request{Name: "POST operatingReview"}
	delta := `"delta":{"value":0,"priorValue":0,"absolute":0,"percent":0,"status":"","hasPriorData":false}`
	metric := func(key string) string {
		return `{"key":"` + key + `","label":"L","value":0,"unit":"ratio",` + delta + `,"hasData":false}`
	}
	wrap := func(keys ...string) string {
		parts := make([]string, 0, len(keys))
		for _, key := range keys {
			parts = append(parts, metric(key))
		}
		return `{"data":{"operatingReview":{"metrics":[` + strings.Join(parts, ",") + `]}}}`
	}
	python := wrap("deployments_count", "change_failure_rate", "incidents_count")
	goBody := wrap("deployments_count", "change_failure_rate", "deployment_failure_rate", "revert_rate", "incidents_count")
	if goOnlyMetricsNormalize(request, goBody) != python {
		t.Fatalf("the two Go-only metrics are not removed:\n%s", goOnlyMetricsNormalize(request, goBody))
	}
	// Another metric that differs is not masked.
	if goOnlyMetricsNormalize(request, wrap("deployments_count", "other_metric", "incidents_count")) == python {
		t.Fatal("a different metric was masked")
	}
	// Another operation is left alone.
	if other := (venueoracle.Request{Name: "POST home"}); goOnlyMetricsNormalize(other, goBody) != goBody {
		t.Fatal("an answer of another operation was changed")
	}
	if ok, _ := goOnlyMetricsInspect(request, python); ok {
		t.Fatal("a Go answer without the two metrics passes the inspection")
	}
	if ok, why := goOnlyMetricsInspect(request, goBody); !ok {
		t.Fatalf("the intended Go answer fails the inspection: %s", why)
	}
}
