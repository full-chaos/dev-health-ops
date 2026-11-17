package server

import (
	"context"
	"strings"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/authctx"
)

// carrierOperatorRoles is datahealth.RequireOperator's role set (compared
// lowercased there too). A header carrier claiming one of them is refused on
// both listeners that read the X-DH-Internal-* headers.
var carrierOperatorRoles = map[string]bool{"admin": true, "owner": true, "operator": true}

// carrierElevatedClaims is the one rule the header carrier is refused by on
// the MCP listener and on the internal listener (403 elevated_claim): a
// superuser claim, an impersonation claim or an operator role. acr-api sends
// superuser=false, impersonation=false and the least role as constants, so a
// claim here means a misconfigured or taken-over caller. It returns the claim
// names, a fixed vocabulary, never a value.
func carrierElevatedClaims(claims authctx.Claims) []string {
	var elevated []string
	if claims.IsSuperuser {
		elevated = append(elevated, "superuser")
	}
	if claims.ImpersonationActive {
		elevated = append(elevated, "impersonation")
	}
	if role := strings.ToLower(strings.TrimSpace(claims.Role)); carrierOperatorRoles[role] {
		elevated = append(elevated, "role_"+role)
	}
	return elevated
}

// internalElevatedClaimCounter counts the internal listener's header-carrier
// requests refused for an elevated claim, by claim (the MCP listener's
// devhealth_query_api_mcp_elevated_claims_total has the same label).
var internalElevatedClaimCounter = mustInternalCounter("devhealth_query_api_internal_elevated_claims_total",
	"Internal-listener header-carrier requests refused for claiming superuser, impersonation or an operator role, by claim")

func mustInternalCounter(name, description string) metric.Int64Counter {
	counter, err := otel.Meter("github.com/full-chaos/dev-health-ops/internal/queryapi/server/internal").Int64Counter(name, metric.WithDescription(description))
	if err != nil {
		counter, _ = otel.GetMeterProvider().Meter("noop").Int64Counter(name)
	}
	return counter
}

// countInternalElevatedClaims counts one internal-listener refusal per claim.
func countInternalElevatedClaims(ctx context.Context, elevated []string) {
	for _, claim := range elevated {
		internalElevatedClaimCounter.Add(ctx, 1, metric.WithAttributes(attribute.String("claim", claim)))
	}
}

// The span attributes of a header-carrier identity, set on both listeners
// right after the headers are read: bounded values only, never the org.
const (
	attrInternalRole          = "dev_health.internal.role"
	attrInternalSuperuser     = "dev_health.internal.superuser"
	attrInternalImpersonation = "dev_health.internal.impersonation"
)

// carrierRoles is the closed set the role attribute takes its value from; any
// other role is "other" and an empty one "none".
var carrierRoles = map[string]bool{"viewer": true, "member": true, "admin": true, "owner": true, "operator": true}

func carrierRoleValue(role string) string {
	role = strings.ToLower(strings.TrimSpace(role))
	switch {
	case role == "":
		return "none"
	case carrierRoles[role]:
		return role
	}
	return "other"
}

func boolValue(on bool) string {
	if on {
		return "true"
	}
	return "false"
}

// annotateCarrierSpan records the claims the header carrier made on the
// request's span.
func annotateCarrierSpan(ctx context.Context, claims authctx.Claims) {
	trace.SpanFromContext(ctx).SetAttributes(
		attribute.String(attrInternalRole, carrierRoleValue(claims.Role)),
		attribute.String(attrInternalSuperuser, boolValue(claims.IsSuperuser)),
		attribute.String(attrInternalImpersonation, boolValue(claims.ImpersonationActive)),
	)
}
