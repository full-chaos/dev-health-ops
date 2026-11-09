package graph

import (
	"context"

	"github.com/99designs/gqlgen/graphql"
	"github.com/google/uuid"
	"github.com/vektah/gqlparser/v2/gqlerror"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/authctx"
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

// servedOrg is the organization a root field is served for: the claims' org, as the ingress spelled it, while it is
// the scope's organization as a UUID (on an enforced root, the decided organization); "" when it is not, which the
// org helpers refuse. Without a scope it is the claims' org.
func servedOrg(claims authctx.Claims) string {
	org, ok := claims.Scope.OrgID()
	if !ok || org == claims.OrgID {
		return claims.OrgID
	}
	scoped, err := uuid.Parse(org)
	if err != nil {
		return ""
	}
	if own, err := uuid.Parse(claims.OrgID); err != nil || own != scoped {
		return ""
	}
	return claims.OrgID
}
