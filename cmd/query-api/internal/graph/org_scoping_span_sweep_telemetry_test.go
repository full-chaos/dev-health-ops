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
	"errors"
	"testing"

	"github.com/full-chaos/dev-health-go/clickhouse"
	"github.com/vektah/gqlparser/v2/gqlerror"
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

// requireDeniedSpanOrgID extends requireDeniedSpanReason with the org_id
// attribute check codex's r1 review (PR #2369, finding 2) named: an
// org_mismatch denial's org_id attribute must be the AUTHENTICATED
// claims.OrgID, never the client-supplied orgID argument -- a mutant that
// swapped the two would otherwise pass every other test in this file
// unnoticed, since FeatureFlags/Analytics are the only two resolvers where
// the two values genuinely differ.
func requireDeniedSpanOrgID(t *testing.T, recorder *tracetest.SpanRecorder, wantName, wantReason, wantOrgID string) {
	t.Helper()
	requireDeniedSpanReason(t, recorder, wantName, wantReason)

	ended := recorder.Ended()
	if len(ended) != 1 {
		return // requireDeniedSpanReason already failed the test on this.
	}
	var orgID string
	for _, attr := range ended[0].Attributes() {
		if string(attr.Key) == "org_id" {
			orgID = attr.Value.AsString()
		}
	}
	if orgID != wantOrgID {
		t.Errorf("%s: org_id attribute is %q, want %q (the authenticated claim, not the requested orgID argument)", wantName, orgID, wantOrgID)
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
	requireDeniedSpanOrgID(t, recorder, "query-api.featureFlags", "org_mismatch", "org-authorized")
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
	requireDeniedSpanOrgID(t, recorder, "query-api.analytics", "org_mismatch", "org-authorized")
}

// fakeQueryClient is a minimal QueryClient-shaped fake that always errors --
// the same technique analytics_resolver_test.go's fakeAnalyticsCHClient
// uses, generalized here across all ten resolvers this file covers: every
// touched resolver's own package declares a QueryClient interface with the
// identical single-method shape (resolver.go's own doc comment), so one
// fake satisfies all of them without a wrapper.
//
// It exists to catch the deny-all-mutant class codex's r1 review named (PR
// #2369, finding 1): every test above proves a REJECTED call is observable
// -- none of them proves an ACCEPTED (matching-org) call is NOT also
// reported "denied". A resolver whose guard always calls
// finish("denied", ...) regardless of whether claims.OrgID actually
// matches would still pass every test above. The tests below close that
// gap: build the resolver with THIS non-nil client (a nil one, as used
// above, would panic the moment a resolver reaches past its guard) and a
// MATCHING org, and assert the span outcome is never "denied".
type fakeQueryClient struct{}

func (fakeQueryClient) Query(_ context.Context, _ string, _ []clickhouse.Binding) (clickhouse.RowScanner, error) {
	return nil, errors.New("fakeQueryClient: reached past the guard, as expected for a matching-org call")
}

// requireNotAuthorizationError is requireAuthorizationError's converse: a
// matching-org call's error, if any, must never carry the
// AUTHORIZATION_ERROR code -- that would mean the resolver treated an
// authorized caller as rejected. A nil error is NOT itself a failure here:
// some resolvers tolerate fakeQueryClient's error internally (operatingreview
// isolates per-table fetch failures and still returns a successful,
// degraded result -- see its own package doc comment; Analytics short-
// circuits an empty batch to a successful empty result without ever
// touching ClickHouse at all -- see analytics_resolver_test.go's
// TestAnalytics_MatchingOrgIDReachesResolve). The span-outcome check in
// requireNotDeniedSpan is what actually proves the guard was reached; this
// helper only guards against the specific AUTHORIZATION_ERROR regression.
func requireNotAuthorizationError(t *testing.T, err error) {
	t.Helper()
	var gqlErr *gqlerror.Error
	if errors.As(err, &gqlErr) {
		if code, _ := gqlErr.Extensions["code"].(string); code == "AUTHORIZATION_ERROR" {
			t.Fatalf("got an AUTHORIZATION_ERROR for a matching-org call: %v", err)
		}
	}
}

// requireNotDeniedSpan asserts exactly one span was recorded and its
// outcome attribute is anything OTHER than "denied" -- the discriminator
// that catches the deny-all mutant this file's own doc comment (on
// fakeQueryClient) describes.
func requireNotDeniedSpan(t *testing.T, recorder *tracetest.SpanRecorder, wantName string) {
	t.Helper()
	ended := recorder.Ended()
	if len(ended) != 1 {
		t.Fatalf("%s: recorded %d spans, want exactly 1", wantName, len(ended))
	}
	span := ended[0]
	if span.Name() != wantName {
		t.Errorf("span name: got %q, want %q", span.Name(), wantName)
	}
	for _, attr := range span.Attributes() {
		if string(attr.Key) == "outcome" && attr.Value.AsString() == "denied" {
			t.Fatalf("%s: outcome attribute is \"denied\" for a MATCHING-org call -- a deny-all mutant would pass every other test in this file undetected", wantName)
		}
	}
}

func TestFeatureFlagsMatchingOrgIsNotDenied(t *testing.T) {
	recorder := recordSpans(t)
	resolver := &queryResolver{&Resolver{ClickHouse: fakeQueryClient{}}}
	ctx := authctx.WithClaims(context.Background(), authctx.Claims{OrgID: "org-1"})

	_, err := resolver.FeatureFlags(ctx, "org-1", nil, nil, nil, 10)
	requireNotAuthorizationError(t, err)
	requireNotDeniedSpan(t, recorder, "query-api.featureFlags")
}

func TestReviewEdgesMatchingOrgIsNotDenied(t *testing.T) {
	recorder := recordSpans(t)
	resolver := &queryResolver{&Resolver{ClickHouse: fakeQueryClient{}}}
	ctx := authctx.WithClaims(context.Background(), authctx.Claims{OrgID: "org-1"})

	_, err := resolver.ReviewEdges(ctx, model.ReviewEdgesInput{})
	requireNotAuthorizationError(t, err)
	requireNotDeniedSpan(t, recorder, "query-api.reviewEdges")
}

func TestCognitiveLoadMatchingOrgIsNotDenied(t *testing.T) {
	recorder := recordSpans(t)
	resolver := &queryResolver{&Resolver{ClickHouse: fakeQueryClient{}}}
	ctx := authctx.WithClaims(context.Background(), authctx.Claims{OrgID: "org-1"})

	_, err := resolver.CognitiveLoad(ctx, model.CognitiveLoadInput{})
	requireNotAuthorizationError(t, err)
	requireNotDeniedSpan(t, recorder, "query-api.cognitiveLoad")
}

func TestComplexityTimeseriesMatchingOrgIsNotDenied(t *testing.T) {
	recorder := recordSpans(t)
	resolver := &queryResolver{&Resolver{ClickHouse: fakeQueryClient{}}}
	ctx := authctx.WithClaims(context.Background(), authctx.Claims{OrgID: "org-1"})

	_, err := resolver.ComplexityTimeseries(ctx, model.ComplexityTimeseriesInput{})
	requireNotAuthorizationError(t, err)
	requireNotDeniedSpan(t, recorder, "query-api.complexityTimeseries")
}

func TestHotspotsMatchingOrgIsNotDenied(t *testing.T) {
	recorder := recordSpans(t)
	resolver := &queryResolver{&Resolver{ClickHouse: fakeQueryClient{}}}
	ctx := authctx.WithClaims(context.Background(), authctx.Claims{OrgID: "org-1"})

	_, err := resolver.Hotspots(ctx, model.HotspotsInput{})
	requireNotAuthorizationError(t, err)
	requireNotDeniedSpan(t, recorder, "query-api.hotspots")
}

func TestOperatingReviewMatchingOrgIsNotDenied(t *testing.T) {
	recorder := recordSpans(t)
	resolver := &queryResolver{&Resolver{ClickHouse: fakeQueryClient{}}}
	ctx := authctx.WithClaims(context.Background(), authctx.Claims{OrgID: "org-1"})

	_, err := resolver.OperatingReview(ctx, "org-1", model.OperatingReviewInput{})
	requireNotAuthorizationError(t, err)
	requireNotDeniedSpan(t, recorder, "query-api.operatingReview")
}

func TestWorkGraphEdgesMatchingOrgIsNotDenied(t *testing.T) {
	recorder := recordSpans(t)
	resolver := &queryResolver{&Resolver{ClickHouse: fakeQueryClient{}}}
	ctx := authctx.WithClaims(context.Background(), authctx.Claims{OrgID: "org-1"})

	_, err := resolver.WorkGraphEdges(ctx, "org-1", nil)
	requireNotAuthorizationError(t, err)
	requireNotDeniedSpan(t, recorder, "query-api.workGraphEdges")
}

func TestWorkGraphFlowMatchingOrgIsNotDenied(t *testing.T) {
	recorder := recordSpans(t)
	resolver := &queryResolver{&Resolver{ClickHouse: fakeQueryClient{}}}
	ctx := authctx.WithClaims(context.Background(), authctx.Claims{OrgID: "org-1"})

	_, err := resolver.WorkGraphFlow(ctx, "org-1", nil)
	requireNotAuthorizationError(t, err)
	requireNotDeniedSpan(t, recorder, "query-api.workGraphFlow")
}

func TestWorkGraphArtifactsMatchingOrgIsNotDenied(t *testing.T) {
	recorder := recordSpans(t)
	resolver := &queryResolver{&Resolver{ClickHouse: fakeQueryClient{}}}
	ctx := authctx.WithClaims(context.Background(), authctx.Claims{OrgID: "org-1"})

	_, err := resolver.WorkGraphArtifacts(ctx, "org-1", nil)
	requireNotAuthorizationError(t, err)
	requireNotDeniedSpan(t, recorder, "query-api.workGraphArtifacts")
}

func TestAnalyticsMatchingOrgIsNotDenied(t *testing.T) {
	recorder := recordSpans(t)
	resolver := &queryResolver{&Resolver{ClickHouse: fakeQueryClient{}}}
	ctx := authctx.WithClaims(context.Background(), authctx.Claims{OrgID: "org-1"})

	_, err := resolver.Analytics(ctx, "org-1", model.AnalyticsRequestInput{})
	requireNotAuthorizationError(t, err)
	requireNotDeniedSpan(t, recorder, "query-api.analytics")
}
