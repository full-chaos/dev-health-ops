package graph

// Regression guard for the org-scoping span sweep: the ten delegated
// resolvers below (FeatureFlags, ReviewEdges, CognitiveLoad,
// ComplexityTimeseries, Hotspots, OperatingReview, WorkGraphEdges,
// WorkGraphFlow, WorkGraphArtifacts, Analytics) used to run their
// authorization guard BEFORE starting a span -- same defect
// forecast_resolver_telemetry_test.go documents and fixes for
// CapacityForecast/CapacityForecasts/ThroughputForecast (CHAOS-5349 r1
// P2), left un-widened there deliberately ("filed as its own sweep").
// A rejected request produced no span, no attribute, and no metric at
// all, making "this operation is being called and rejected"
// indistinguishable from "this operation is receiving no traffic".
//
// Reuses recordSpans/requireAuthorizationError/requireDeniedSpan from
// forecast_resolver_telemetry_test.go (same package) rather than
// duplicating them.
//
// Note what is NOT asserted: that any resolver reaches ClickHouse. Every
// resolver below is built with a nil ClickHouse client (via
// &Resolver{}), so if a guard ever stopped short-circuiting, these tests
// would panic on a nil dereference rather than pass quietly.

import (
	"context"
	"testing"

	"go.opentelemetry.io/otel/sdk/trace/tracetest"

	"github.com/full-chaos/dev-health-ops/cmd/query-api/internal/authctx"
	"github.com/full-chaos/dev-health-ops/cmd/query-api/internal/graph/model"
)

// requireDeniedSpanReason extends requireDeniedSpan's check (defined in
// forecast_resolver_telemetry_test.go) with the denial_reason attribute
// the org-scoping sweep adds -- distinguishing "no org on the envelope at
// all" from "the envelope's org doesn't match the requested org" without
// widening the low-cardinality outcome counter label.
func requireDeniedSpanReason(t *testing.T, recorder *tracetest.SpanRecorder, wantName, wantReason string) {
	t.Helper()
	requireDeniedSpan(t, recorder, wantName)

	ended := recorder.Ended()
	if len(ended) != 1 {
		return // requireDeniedSpan already failed the test on this.
	}
	var reason string
	for _, attr := range ended[0].Attributes() {
		if string(attr.Key) == "denial_reason" {
			reason = attr.Value.AsString()
		}
	}
	if reason != wantReason {
		t.Errorf("%s: denial_reason attribute is %q, want %q", wantName, reason, wantReason)
	}
}

func TestFeatureFlagsRecordsAuthorizationDenial_NoOrg(t *testing.T) {
	recorder := recordSpans(t)
	resolver := &queryResolver{&Resolver{}}

	result, err := resolver.FeatureFlags(context.Background(), "org-requested", nil, nil, nil, 10)
	if result != nil {
		t.Errorf("got a result for an unauthorized call: %+v", result)
	}
	requireAuthorizationError(t, err)
	requireDeniedSpanReason(t, recorder, "query-api.featureFlags", "no_org")
}

func TestFeatureFlagsRecordsAuthorizationDenial_OrgMismatch(t *testing.T) {
	recorder := recordSpans(t)
	resolver := &queryResolver{&Resolver{}}
	ctx := authctx.WithClaims(context.Background(), authctx.Claims{OrgID: "org-authorized"})

	result, err := resolver.FeatureFlags(ctx, "org-requested", nil, nil, nil, 10)
	if result != nil {
		t.Errorf("got a result for a mismatched-org call: %+v", result)
	}
	requireAuthorizationError(t, err)
	requireDeniedSpanReason(t, recorder, "query-api.featureFlags", "org_mismatch")
}

func TestReviewEdgesRecordsAuthorizationDenial(t *testing.T) {
	recorder := recordSpans(t)
	resolver := &queryResolver{&Resolver{}}

	result, err := resolver.ReviewEdges(context.Background(), model.ReviewEdgesInput{})
	if result != nil {
		t.Errorf("got a result for an unauthorized call: %+v", result)
	}
	requireAuthorizationError(t, err)
	requireDeniedSpanReason(t, recorder, "query-api.reviewEdges", "no_org")
}

func TestCognitiveLoadRecordsAuthorizationDenial(t *testing.T) {
	recorder := recordSpans(t)
	resolver := &queryResolver{&Resolver{}}

	result, err := resolver.CognitiveLoad(context.Background(), model.CognitiveLoadInput{})
	if result != nil {
		t.Errorf("got a result for an unauthorized call: %+v", result)
	}
	requireAuthorizationError(t, err)
	requireDeniedSpanReason(t, recorder, "query-api.cognitiveLoad", "no_org")
}

func TestComplexityTimeseriesRecordsAuthorizationDenial(t *testing.T) {
	recorder := recordSpans(t)
	resolver := &queryResolver{&Resolver{}}

	result, err := resolver.ComplexityTimeseries(context.Background(), model.ComplexityTimeseriesInput{})
	if result != nil {
		t.Errorf("got a result for an unauthorized call: %+v", result)
	}
	requireAuthorizationError(t, err)
	requireDeniedSpanReason(t, recorder, "query-api.complexityTimeseries", "no_org")
}

func TestHotspotsRecordsAuthorizationDenial(t *testing.T) {
	recorder := recordSpans(t)
	resolver := &queryResolver{&Resolver{}}

	result, err := resolver.Hotspots(context.Background(), model.HotspotsInput{})
	if result != nil {
		t.Errorf("got a result for an unauthorized call: %+v", result)
	}
	requireAuthorizationError(t, err)
	requireDeniedSpanReason(t, recorder, "query-api.hotspots", "no_org")
}

func TestOperatingReviewRecordsAuthorizationDenial(t *testing.T) {
	recorder := recordSpans(t)
	resolver := &queryResolver{&Resolver{}}

	result, err := resolver.OperatingReview(context.Background(), "org-requested", model.OperatingReviewInput{})
	if result != nil {
		t.Errorf("got a result for an unauthorized call: %+v", result)
	}
	requireAuthorizationError(t, err)
	requireDeniedSpanReason(t, recorder, "query-api.operatingReview", "no_org")
}

func TestWorkGraphEdgesRecordsAuthorizationDenial(t *testing.T) {
	recorder := recordSpans(t)
	resolver := &queryResolver{&Resolver{}}

	result, err := resolver.WorkGraphEdges(context.Background(), "org-requested", nil)
	if result != nil {
		t.Errorf("got a result for an unauthorized call: %+v", result)
	}
	requireAuthorizationError(t, err)
	requireDeniedSpanReason(t, recorder, "query-api.workGraphEdges", "no_org")
}

func TestWorkGraphFlowRecordsAuthorizationDenial(t *testing.T) {
	recorder := recordSpans(t)
	resolver := &queryResolver{&Resolver{}}

	result, err := resolver.WorkGraphFlow(context.Background(), "org-requested", nil)
	if result != nil {
		t.Errorf("got a result for an unauthorized call: %+v", result)
	}
	requireAuthorizationError(t, err)
	requireDeniedSpanReason(t, recorder, "query-api.workGraphFlow", "no_org")
}

func TestWorkGraphArtifactsRecordsAuthorizationDenial(t *testing.T) {
	recorder := recordSpans(t)
	resolver := &queryResolver{&Resolver{}}

	result, err := resolver.WorkGraphArtifacts(context.Background(), "org-requested", nil)
	if result != nil {
		t.Errorf("got a result for an unauthorized call: %+v", result)
	}
	requireAuthorizationError(t, err)
	requireDeniedSpanReason(t, recorder, "query-api.workGraphArtifacts", "no_org")
}

func TestAnalyticsRecordsAuthorizationDenial_NoOrg(t *testing.T) {
	recorder := recordSpans(t)
	resolver := &queryResolver{&Resolver{}}

	result, err := resolver.Analytics(context.Background(), "org-requested", model.AnalyticsRequestInput{})
	if result != nil {
		t.Errorf("got a result for an unauthorized call: %+v", result)
	}
	requireAuthorizationError(t, err)
	requireDeniedSpanReason(t, recorder, "query-api.analytics", "no_org")
}

func TestAnalyticsRecordsAuthorizationDenial_OrgMismatch(t *testing.T) {
	recorder := recordSpans(t)
	resolver := &queryResolver{&Resolver{}}
	ctx := authctx.WithClaims(context.Background(), authctx.Claims{OrgID: "org-authorized"})

	result, err := resolver.Analytics(ctx, "org-requested", model.AnalyticsRequestInput{})
	if result != nil {
		t.Errorf("got a result for a mismatched-org call: %+v", result)
	}
	requireAuthorizationError(t, err)
	requireDeniedSpanReason(t, recorder, "query-api.analytics", "org_mismatch")
}
