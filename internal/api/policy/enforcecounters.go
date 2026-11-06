package policy

import (
	"context"
	"strings"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

const (
	enforceDecisionMetric = "dev_health_authz_enforcement_decisions_total"
	enforceServiceLabel   = "go-api"
	enforceUnjudgedMetric = "dev_health_authz_enforcement_unjudged_total"
)

type enforceDecisionAdder func(ctx context.Context, namespace, outcome string)

// enforceDecisionAdd counts one enforced decision. Tests replace it. Every label value is a closed value: a catalog
// namespace, the service and one of the outcomes in enforce.go; no request, user or organization value is a label.
var enforceDecisionAdd = newEnforceDecisionAdder(otel.Meter(shadowMeterName))

func newEnforceDecisionAdder(meter metric.Meter) enforceDecisionAdder {
	counter, err := meter.Int64Counter(enforceDecisionMetric,
		metric.WithDescription("Enforced decisions by action namespace, service and outcome: "+strings.Join(enforceOutcomes, ", ")))
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

const (
	enforceUnjudgedHS256 = "hs256"
	enforceUnjudgedNoOrg = "no_org"
	// enforceUnjudgedServedOrg: the request serves another organization than the token's, or the token has none.
	enforceUnjudgedServedOrg = "served_org"
)

// enforceUnjudgedReasons is the closed reason set of the unjudged counter.
var enforceUnjudgedReasons = []string{enforceUnjudgedHS256, enforceUnjudgedNoOrg, enforceUnjudgedServedOrg}

type enforceUnjudgedAdder func(ctx context.Context, route, reason string)

var enforceUnjudgedAdd = newEnforceUnjudgedAdder(otel.Meter(shadowMeterName))

func newEnforceUnjudgedAdder(meter metric.Meter) enforceUnjudgedAdder {
	counter, err := meter.Int64Counter(enforceUnjudgedMetric,
		metric.WithDescription("Requests on an enforced cell whose caller the decision point cannot judge, so the legacy answer stood, by route row and reason: "+strings.Join(enforceUnjudgedReasons, ", ")))
	if err != nil {
		return func(context.Context, string, string) {}
	}
	return func(ctx context.Context, route, reason string) {
		counter.Add(ctx, 1, metric.WithAttributes(attribute.String("route", route), attribute.String("reason", reason)))
	}
}

// countEnforceUnjudged never changes a decision: a counter failure is swallowed.
func countEnforceUnjudged(ctx context.Context, route, reason string) {
	defer func() { _ = recover() }()
	if enforceUnjudgedAdd != nil {
		enforceUnjudgedAdd(ctx, route, reason)
	}
}
