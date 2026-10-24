package graph

type mapClaim struct {
	value  string
	proofs map[string][]string
}

var mapClaims = []mapClaim{
	{value: "organization_from_argument: the orgId argument, refused by OperationOrgGuard unless it equals the credential's organization; a verified non-impersonating superuser may name another organization", proofs: map[string][]string{
		"operation_org_guard.go": {"rebindTo", "IsSuperuser"},
	}},
	{value: "organization_from_credential: the credential's organization (claims.OrgID); the field takes no orgId argument", proofs: map[string][]string{
		"orgauth.go":          {"func requestOrg"},
		"schema.resolvers.go": {"authctx.FromContext(ctx)"},
	}},
	{value: "none: platform field; no organization is selected and the caller's organization is not read", proofs: map[string][]string{
		"schema.resolvers.go": {"producttelemetry.RequirePlatformAdmin"},
	}},
	{value: "a request with no organization answers AUTHORIZATION_ERROR; an orgId naming another organization answers a whole-operation error before any field runs, except that a verified non-impersonating superuser may name another organization and the operation then runs against it", proofs: map[string][]string{
		"orgauth.go":             {"org_id is required for all analytics queries", "AUTHORIZATION_ERROR"},
		"operation_org_guard.go": {"rebindTo"},
	}},
	{value: "a request with no organization answers AUTHORIZATION_ERROR; the field takes no orgId argument and reads the credential's organization", proofs: map[string][]string{
		"orgauth.go": {"org_id is required for all analytics queries", "AUTHORIZATION_ERROR"},
	}},
	{value: "only a verified non-impersonating superuser passes, any other caller answers AUTHORIZATION_ERROR (producttelemetry.RequirePlatformAdmin)", proofs: map[string][]string{
		"schema.resolvers.go": {"producttelemetry.RequirePlatformAdmin", "AUTHORIZATION_ERROR"},
	}},
	{value: "only a superuser or a caller with the admin, owner or operator role passes, any other caller answers AUTHORIZATION_ERROR (datahealth.RequireOperator)", proofs: map[string][]string{
		"schema.resolvers.go": {"datahealth.RequireOperator", "AUTHORIZATION_ERROR"},
	}},
}
