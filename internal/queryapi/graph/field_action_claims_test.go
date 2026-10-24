package graph

type claimProof struct {
	file    string
	decl    string
	needles []string
	anyOf   []string
}

type mapClaim struct {
	value  string
	proofs []claimProof
}

var mapClaims = map[string]mapClaim{
	"arg": {value: "organization_from_argument: the orgId argument, checked by OperationOrgGuard against the credential's organization", proofs: []claimProof{
		{file: "operation_org_guard.go", decl: "operationOrgDecision", needles: []string{"claims.OrgID == first"}, anyOf: []string{}},
	}},
	"darg": {value: "a whole-operation error before any field runs: 'A valid organization ID is required' for an empty, padded or non-string orgId; 'Only one organization may be queried per operation' for differing orgIds; 'Authorization required' when the caller has no organization; 'Access denied: cannot query org' when the orgId is not the caller's organization, except that a verified non-impersonating superuser with an organization may name another and the operation then runs against it (OperationOrgGuard)", proofs: []claimProof{
		{file: "operation_org_guard.go", decl: "operationOrgDecision", needles: []string{"A valid organization ID is required", "Only one organization may be queried per operation", "\"Authorization required\"", "Access denied: cannot query org", "claims.IsSuperuser && !claims.ImpersonationActive", "if !ok || claims.OrgID == \"\" {", "claims.OrgID == first"}, anyOf: []string{}},
	}},
	"dmut": {value: "a whole-operation error before any field runs: 'A valid organization ID is required' for an empty, padded or non-string orgId; 'Only one organization may be queried per operation' for differing orgIds; 'Authorization required' when the caller has no organization; 'Access denied: cannot query org' when the orgId is not the caller's organization, except that a verified non-impersonating superuser with an organization may name another and the operation then runs against it (OperationOrgGuard); the mutation resolver then applies requireMutationOrg: 'Authorization required' when the caller has no organization, 'Access denied' when the orgId is not the caller's organization", proofs: []claimProof{
		{file: "operation_org_guard.go", decl: "operationOrgDecision", needles: []string{"A valid organization ID is required", "Only one organization may be queried per operation", "\"Authorization required\"", "Access denied: cannot query org", "claims.IsSuperuser && !claims.ImpersonationActive", "if !ok || claims.OrgID == \"\" {", "claims.OrgID == first"}, anyOf: []string{}},
		{file: "reports_mutations.go", decl: "requireMutationOrg", needles: []string{"fail(\"Authorization required\")", "fail(\"Access denied: cannot query org '\" + orgID + \"'\")"}, anyOf: []string{}},
	}},
	"cred": {value: "organization_from_credential: the credential's organization (claims.OrgID); the field takes no orgId argument", proofs: []claimProof{
		{file: "schema.resolvers.go", decl: "{resolver}", needles: []string{}, anyOf: []string{"claims.OrgID", "requestOrg(ctx)"}},
	}},
	"dcred": {value: "a request with no organization answers AUTHORIZATION_ERROR (the field's own check); the field takes no orgId argument", proofs: []claimProof{
		{file: "schema.resolvers.go", decl: "{resolver}", needles: []string{}, anyOf: []string{"AUTHORIZATION_ERROR", "requestOrg(ctx)"}},
	}},
	"platform": {value: "none: platform field; no organization is selected", proofs: []claimProof{
		{file: "schema.resolvers.go", decl: "{resolver}", needles: []string{"producttelemetry.RequirePlatformAdmin("}, anyOf: []string{}},
	}},
	"dplatform": {value: "refused with AUTHORIZATION_ERROR unless producttelemetry.RequirePlatformAdmin passes the caller", proofs: []claimProof{
		{file: "schema.resolvers.go", decl: "{resolver}", needles: []string{"producttelemetry.RequirePlatformAdmin(", "AUTHORIZATION_ERROR"}, anyOf: []string{}},
	}},
	"ddata": {value: "refused with AUTHORIZATION_ERROR unless datahealth.RequireOperator passes the caller", proofs: []claimProof{
		{file: "schema.resolvers.go", decl: "{resolver}", needles: []string{"datahealth.RequireOperator(", "AUTHORIZATION_ERROR"}, anyOf: []string{}},
	}},
}
