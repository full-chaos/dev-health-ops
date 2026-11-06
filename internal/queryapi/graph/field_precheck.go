package graph

import (
	"context"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/authctx"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/datahealth"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/producttelemetry"
)

// fieldPrechecks is the pre-resolver check of each root field that has one: the legacy answer for the field, beside the
// operation guard's. Each entry calls the same function its resolver calls, with the claims the resolver sees and the
// org the field names ("" when it names none). A root with no entry has no such check. The parity tests read the
// resolvers' source and fail when a resolver calls a check that has no entry here.
var fieldPrechecks = map[string]func(claims authctx.Claims, named string) bool{
	"Query.dataHealth": func(claims authctx.Claims, _ string) bool {
		return datahealth.RequireOperator(datahealth.Principal{Present: true, Role: claims.Role, IsSuperuser: claims.IsSuperuser, Platform: claims.Platform}) == nil
	},
	"Query.productTelemetryPlatformDashboard": func(claims authctx.Claims, _ string) bool {
		return producttelemetry.RequirePlatformAdmin(producttelemetry.Principal{Present: true, IsSuperuser: claims.IsSuperuser,
			ImpersonationActive: claims.ImpersonationActive, Platform: claims.Platform}) == nil
	},
	"Mutation.createSavedReport": mutationOrgPrecheck,
	"Mutation.updateSavedReport": mutationOrgPrecheck,
	"Mutation.deleteSavedReport": mutationOrgPrecheck,
	"Mutation.cloneSavedReport":  mutationOrgPrecheck,
	"Mutation.triggerReport":     mutationOrgPrecheck,
}

func mutationOrgPrecheck(claims authctx.Claims, named string) bool {
	return requireMutationOrg(authctx.WithClaims(context.Background(), claims), named) == nil
}
