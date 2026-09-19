package graph

import (
	"context"

	"github.com/99designs/gqlgen/graphql"
	"github.com/vektah/gqlparser/v2/gqlerror"

	"github.com/full-chaos/dev-health-ops/cmd/query-api/internal/authctx"
)

// requireOwnOrg admits a request only when the request identity carries
// an org and the orgId argument names that same org. A request without an
// org identity, or one asking about a different org, is denied with an
// AUTHORIZATION_ERROR so a caller can never read another org's rows by
// passing a different orgId.
func requireOwnOrg(ctx context.Context, orgID string) error {
	claims, ok := authctx.FromContext(ctx)
	if !ok || claims.OrgID == "" || claims.OrgID != orgID {
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
