package policy

import (
	"context"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

const (
	enforceDecisionMetric = "dev_health_authz_enforcement_decisions_total"
	enforceServiceLabel   = "go-api"
)

type enforceDecisionAdder func(ctx context.Context, namespace, outcome string)

// enforceDecisionAdd counts one enforced decision. Tests replace it. Every label value is a closed value: a catalog
// namespace, the service and one of the outcomes in enforce.go; no request, user or organization value is a label.
var enforceDecisionAdd = newEnforceDecisionAdder(otel.Meter(shadowMeterName))

func newEnforceDecisionAdder(meter metric.Meter) enforceDecisionAdder {
	counter, err := meter.Int64Counter(enforceDecisionMetric,
		metric.WithDescription("Enforced decisions by action namespace, service and outcome: allow, deny, error_deny, unjudged_legacy"))
	if err != nil {
		return func(context.Context, string, string) {}
	}
	return func(ctx context.Context, namespace, outcome string) {
		counter.Add(ctx, 1, metric.WithAttributes(attribute.String("namespace", namespace),
			attribute.String("service", enforceServiceLabel), attribute.String("outcome", outcome)))
	}
}

// countEnforce never changes a decision: a counter failure is swallowed.
func countEnforce(ctx context.Context, namespace, outcome string) {
	defer func() { _ = recover() }()
	if enforceDecisionAdd != nil {
		enforceDecisionAdd(ctx, namespace, outcome)
	}
}
