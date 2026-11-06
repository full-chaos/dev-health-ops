package graph

import (
	"context"

	"github.com/99designs/gqlgen/graphql"
	"github.com/vektah/gqlparser/v2/gqlerror"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/authctx"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/teamscope"
)

// requireOwnOrg admits a request only when the request identity carries
// an org and the orgId argument names that same org. A request without an
// org identity, or one asking about a different org, is denied with an
// AUTHORIZATION_ERROR so a caller can never read another org's rows by
// passing a different orgId.
func requireOwnOrg(ctx context.Context, orgID string) error {
	claims, ok := authctx.FromContext(ctx)
	if !ok || claims.OrgID == "" || claims.OrgID != orgID || servedOrg(claims) != orgID {
		return &gqlerror.Error{
			Message: "org_id is required for all analytics queries",
			Path:    graphql.GetPath(ctx),
			Extensions: map[string]interface{}{
				"code": "AUTHORIZATION_ERROR",
			},
		}
	}
	return nil
}

// requestOrg returns the org of the request identity for a field that takes no
// orgId argument. A request without an org identity is denied with the same
// AUTHORIZATION_ERROR.
func requestOrg(ctx context.Context) (string, error) {
	claims, ok := authctx.FromContext(ctx)
	if !ok || claims.OrgID == "" || servedOrg(claims) == "" {
		return "", &gqlerror.Error{
			Message: "org_id is required for all analytics queries",
			Path:    graphql.GetPath(ctx),
			Extensions: map[string]interface{}{
				"code": "AUTHORIZATION_ERROR",
			},
		}
	}
	return servedOrg(claims), nil
}

// servedOrg is the organization a root field is served for: the scope's organization when the request carries one
// (on an enforced root, the decided organization; Legacy keeps the caller's), else the claims' own.
func servedOrg(claims authctx.Claims) string {
	if org, ok := claims.Scope.OrgID(); ok {
		return org
	}
	return claims.OrgID
}

// rootCeiling is the repository ceiling of the root field's scope (CHAOS-13161): the repositories of an allowlist
// decision; none for Legacy and an organization-wide allow. A root that carries it reads only those repositories.
func rootCeiling(ctx context.Context, client teamscope.RowQuerier) (teamscope.Ceiling, error) {
	claims, _ := authctx.FromContext(ctx)
	return teamscope.ResolveCeiling(ctx, client, claims.Scope)
}
