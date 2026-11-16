package policy

import (
	"context"
	"slices"
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

// The closed cause set of the error_deny outcome: one value per path that refuses for want of a verdict. Only error_deny
// carries a cause; allow, deny and impersonation_refused carry no cause attribute.
const (
	enforceCauseNoUserToken         = "no_user_token"
	enforceCauseUnconfigured        = "unconfigured"
	enforceCauseNoVerdict           = "no_verdict"
	enforceCauseObligation          = "obligation"
	enforceCauseNoOrgToken          = "no_org_token"
	enforceCauseAdminOrNoVerdict    = "admin_or_no_verdict"
	enforceCauseImpersonationLookup = "impersonation_lookup"
)

var enforceErrorDenyCauses = []string{enforceCauseNoUserToken, enforceCauseUnconfigured, enforceCauseNoVerdict, enforceCauseObligation,
	enforceCauseNoOrgToken, enforceCauseAdminOrNoVerdict, enforceCauseImpersonationLookup}

type enforceDecisionAdder func(ctx context.Context, namespace, outcome, cause string)

// enforceDecisionAdd counts one enforced decision. Tests replace it. Every label value is a closed value: a catalog
// namespace, the service, one of the outcomes in enforce.go and, on error_deny only, one of enforceErrorDenyCauses; no
// request, user or organization value is a label.
var enforceDecisionAdd = newEnforceDecisionAdder(otel.Meter(shadowMeterName))

func newEnforceDecisionAdder(meter metric.Meter) enforceDecisionAdder {
	counter, err := meter.Int64Counter(enforceDecisionMetric,
		metric.WithDescription("Enforced decisions by action namespace, service and outcome: "+strings.Join(enforceOutcomes, ", ")+
			". An error_deny outcome also carries a cause: "+strings.Join(enforceErrorDenyCauses, ", ")))
	if err != nil {
		return func(context.Context, string, string, string) {}
	}
	return func(ctx context.Context, namespace, outcome, cause string) {
		if cause == "" {
			counter.Add(ctx, 1, metric.WithAttributes(attribute.String("namespace", namespace),
				attribute.String("service", enforceServiceLabel), attribute.String("outcome", outcome)))
			return
		}
		counter.Add(ctx, 1, metric.WithAttributes(attribute.String("namespace", namespace),
			attribute.String("service", enforceServiceLabel), attribute.String("outcome", outcome), attribute.String("cause", cause)))
	}
}

// countEnforce never changes a decision: a counter failure is swallowed.
func countEnforce(ctx context.Context, namespace, outcome, cause string) {
	defer func() { _ = recover() }()
	if enforceDecisionAdd != nil {
		enforceDecisionAdd(ctx, namespace, outcome, cause)
	}
}

const (
	// enforceUnjudgedOrgRequired: a token with no organization on an AdminOrg row, refused before any question.
	enforceUnjudgedOrgRequired = "org_required"
	// enforceUnjudgedDelegatedOrgless: a delegated session's bearer with no organization; the legacy answer stands (F1,
	// the named residual of CHAOS-10872 D6989).
	enforceUnjudgedDelegatedOrgless = "delegated_orgless"
	// enforceUnjudgedImpersonation: a read by a caller with a live go-api impersonation row. The legacy answer stands
	// by design (product rule 1), so this reason does not block a flip.
	enforceUnjudgedImpersonation = "impersonation"
)

// enforceUnjudgedReasons is the closed reason set of the unjudged counter.
var enforceUnjudgedReasons = []string{enforceUnjudgedOrgRequired, enforceUnjudgedDelegatedOrgless, enforceUnjudgedImpersonation}

// EnforceUnjudgedReasons is the closed reason set of the unjudged counter, the one source a reader outside this package
// (the shadow venue judge) checks a scrape against.
func EnforceUnjudgedReasons() []string { return slices.Clone(enforceUnjudgedReasons) }

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
