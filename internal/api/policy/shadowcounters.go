package policy

import (
	"context"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

const (
	shadowMismatchMetric = "dev_health_api_shadow_mismatch_total"
	shadowResultMetric   = "dev_health_api_shadow_results_total"
	shadowMeterName      = "github.com/full-chaos/dev-health-ops/internal/api/policy"
)

type shadowMismatchAdder func(ctx context.Context, namespace, caller, class string)

type shadowResultAdder func(ctx context.Context, result string)

// Tests replace both. Every label value is one of the closed sets in shadow.go: no
// request, user or organization value is ever a label.
var (
	shadowMismatchAdd = newShadowMismatchAdder(otel.Meter(shadowMeterName))
	shadowResultAdd   = newShadowResultAdder(otel.Meter(shadowMeterName))
)

func newShadowMismatchAdder(meter metric.Meter) shadowMismatchAdder {
	mismatchCounter, err := meter.Int64Counter(shadowMismatchMetric,
		metric.WithDescription("Shadow PDP decisions that differ from the legacy guard, by action namespace, caller kind and mismatch class"))
	if err != nil {
		return func(context.Context, string, string, string) {}
	}
	return func(ctx context.Context, namespace, caller, class string) {
		mismatchCounter.Add(ctx, 1, metric.WithAttributes(
			attribute.String("namespace", namespace), attribute.String("caller", caller), attribute.String("class", class)))
	}
}

func newShadowResultAdder(meter metric.Meter) shadowResultAdder {
	resultCounter, err := meter.Int64Counter(shadowResultMetric,
		metric.WithDescription("Shadow PDP calls by result: match, mismatch, error, dropped, skipped"))
	if err != nil {
		return func(context.Context, string) {}
	}
	return func(ctx context.Context, result string) {
		resultCounter.Add(ctx, 1, metric.WithAttributes(attribute.String("result", result)))
	}
}

// countShadowMismatch and countShadowResult never change a decision: a counter failure is swallowed.
func countShadowMismatch(ctx context.Context, namespace, caller, class string) {
	defer func() { _ = recover() }()
	if shadowMismatchAdd != nil {
		shadowMismatchAdd(ctx, namespace, caller, class)
	}
}

func countShadowResult(ctx context.Context, result string) {
	defer func() { _ = recover() }()
	if shadowResultAdd != nil {
		shadowResultAdd(ctx, result)
	}
}
