package workgraph

// CHAOS-4655 telemetry: root AGENTS.md standing order is that new
// decision-shape logic ships its telemetry in the same PR. Before this
// fix, batchResolveMembership's rows-returned-per-requested-endpoint ratio
// was UNBOUNDED -- a function of the org's stored data (a cross-product of
// independent node_type/node_id IN lists), not of the request. The tupled
// match this PR adds is only worth shipping if the bound it restores is
// OBSERVABLE in production, not just asserted by this PR's own tests --
// otherwise a future regression back to independent IN lists (or a new
// caller that reintroduces the same shape elsewhere) would be invisible
// again until it next times out or trips ClickHouse's max_result_rows.
//
// Substrate: OTel, matching this package's siblings (analytics/telemetry.go,
// analytics/investmentmembershiptelemetry.go) rather than Prometheus
// directly -- there is no Python precedent to port here (this query path,
// and its cross-product defect, predates any Python equivalent instrument).

import (
	"context"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/metric"
)

// membershipRowsPerEndpoint is a histogram, not a gauge: the interesting
// signal is the DISTRIBUTION of the ratio across requests (has the p99
// drifted upward, e.g. from a future regression), not only its latest
// value.
var membershipRowsPerEndpoint = mustMembershipRatioHistogram(
	"devhealth_query_api_workgraph_membership_rows_per_endpoint",
	"batchResolveMembership: ClickHouse rows returned per requested (node_type,node_id) endpoint -- bounded by category_kind's cardinality after CHAOS-4655's tupled-match fix, unbounded (cross-product of stored data) before it",
)

func mustMembershipRatioHistogram(name, description string) metric.Float64Histogram {
	meter := otel.Meter("github.com/full-chaos/dev-health-ops/cmd/query-api/internal/workgraph")
	histogram, err := meter.Float64Histogram(name, metric.WithDescription(description))
	if err != nil {
		// Same otel guarantee analytics/telemetry.go's mustAnalyticsCounter
		// relies on: the *Histogram method never returns a nil instrument
		// even on error from a broken meter provider, so falling back to a
		// noop provider keeps every call site nil-safe without its own
		// error handling.
		histogram, _ = otel.GetMeterProvider().Meter("noop").Float64Histogram(name)
	}
	return histogram
}

// recordMembershipRowsPerEndpoint is a package var, not a plain func, so a
// test can observe that batchResolveMembership actually reported itself --
// same injectable-observable pattern as analytics/telemetry.go's
// recordDegradation, for the same reason: asserting on the returned map
// cannot distinguish "reported a ratio of 1.0" from "never reported at
// all".
var recordMembershipRowsPerEndpoint = defaultRecordMembershipRowsPerEndpoint

func defaultRecordMembershipRowsPerEndpoint(ctx context.Context, rowsReturned, endpointsRequested int) {
	if endpointsRequested <= 0 {
		// batchResolveMembership never reaches its query call with zero
		// requested endpoints (it returns early), but this stays defensive
		// against a future caller of the histogram directly.
		return
	}
	membershipRowsPerEndpoint.Record(ctx, float64(rowsReturned)/float64(endpointsRequested))
}
