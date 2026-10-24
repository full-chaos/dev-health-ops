package server

// claimProof names source (or a test) that must contain every needle for a row
// statement to stand; the guard in endpoint_profile_action_fields_test.go reads it.
type claimProof struct {
	file    string
	needles []string
}

type rowClaim struct {
	value  string
	proofs []claimProof
}

var rowClaims = map[string]map[string]rowClaim{
	"rest": {
		"resource_resolver": {value: "organization_from_credential: the verified credential's organization (claims.OrgID); no request parameter names the organization", proofs: []claimProof{
			{file: "rest_error_response.go", needles: []string{"return authctx.Claims{OrgID: claims.OrgID}, true"}},
		}},
		"disclosure_behavior": {value: "401 for a missing or failing credential (authenticateRESTRequest); 403 Organization context required for a valid edge token with no organization unless the caller is a superuser, who passes; 503 or 500 when the users-row read fails (policy.WriteAuthFailure)", proofs: []claimProof{
			{file: "rest_error_response.go", needles: []string{"writeRESTUnauthorized", "Organization context required", "policy.WriteAuthFailure"}},
			{file: "edge_users_test.go", needles: []string{"TestAuthenticateRESTRequestOrglessEdgeToken", "orgless superuser", "TestAuthenticateRESTRequestEdgeUsersRow"}},
		}},
		"primary_validator": {value: "authenticateRESTRequest: bearer edge access token (HS256, live users row) or EdDSA envelope; every credential failure answers the same 401; a valid edge token with no organization answers 403 Organization context required unless the caller is a superuser, who passes; a failed users-row read answers 503 or 500.", proofs: []claimProof{
			{file: "rest_error_response.go", needles: []string{"writeRESTUnauthorized", "Organization context required", "policy.WriteAuthFailure"}},
			{file: "edge_users_test.go", needles: []string{"TestAuthenticateRESTRequestOrglessEdgeToken", "orgless superuser"}},
		}},
	},
	"gql": {
		"resource_resolver": {value: "organization_from_header_or_credential: the X-Org-Id header when the caller may use it (a superuser, the caller's own organization, or an organization the caller is a member of), else the credential's organization (scope.OrgScope); a field's orgId argument is refused unless it equals that organization, except that a verified non-impersonating superuser may name another (OperationOrgGuard)", proofs: []claimProof{
			{file: "../../api/policy/scope.go", needles: []string{"func (s *Scope) OrgScope", "headerOrgID", "mayUseOrg"}},
			{file: "../graph/operation_org_guard.go", needles: []string{"rebindTo", "IsSuperuser"}},
		}},
		"impersonation_policy": {value: "subject_to_impersonation_override", proofs: []claimProof{
			{file: "graphql_edge_route.go", needles: []string{"handler = scope.Impersonation(handler)"}},
		}},
		"disclosure_behavior": {value: "401 Authentication required (authenticateGraphQLEdge); 403 for an X-Org-Id the caller may not use (scope.OrgScope); 404 for a browser GET; 404 UNREGISTERED_DOCUMENT for an authenticated unregistered document; whole-operation error for an orgId argument naming another organization, except a verified non-impersonating superuser (OperationOrgGuard)", proofs: []claimProof{
			{file: "graphql_edge_route.go", needles: []string{"Authentication required", "refuseIDE", "UNREGISTERED_DOCUMENT"}},
			{file: "../../api/policy/scope.go", needles: []string{"writeOrgDenied"}},
			{file: "../graph/operation_org_guard.go", needles: []string{"rebindTo"}},
			{file: "graphql_edge_route_test.go", needles: []string{"TestGraphQLEdgeChainKeepsThePythonMiddlewareOrder"}},
		}},
	},
	"query": {
		"resource_resolver": {value: "organization_from_credential: claims.OrgID returned by authenticateInternalRequest", proofs: []claimProof{
			{file: "internal_auth.go", needles: []string{"func authenticateInternalRequest", "OrgID: claims.OrgID"}},
		}},
		"impersonation_policy": {value: "subject_to_impersonation_override", proofs: []claimProof{
			{file: "internal_auth.go", needles: []string{"ImpersonationActive: claims.ImpersonationActive", "impersonationActive := session != nil"}},
		}},
		"disclosure_behavior": {value: "401 for a missing or failing credential (authenticateInternalRequest); 404 for an authenticated unregistered document (shared dispatcher)", proofs: []claimProof{
			{file: "internal_auth.go", needles: []string{"http.Error(w, \"unauthorized\", http.StatusUnauthorized)"}},
			{file: "internal_auth_test.go", needles: []string{"TestQueryStillAnswers404ForAnUnregisteredDocumentOnceAuthenticated"}},
		}},
	},
	"proofw": {
		"resource_resolver": {value: "organization_from_credential: claims.OrgID returned by authenticateInternalRequest; the organization must be on the go_api_proof_orgs allowlist", proofs: []claimProof{
			{file: "internal_auth.go", needles: []string{"func authenticateInternalRequest", "OrgID: claims.OrgID"}},
			{file: "query_route.go", needles: []string{"func newProofOrgAllowed", "not on the proof-org allowlist"}},
		}},
		"impersonation_policy": {value: "subject_to_impersonation_override", proofs: []claimProof{
			{file: "internal_auth.go", needles: []string{"ImpersonationActive: claims.ImpersonationActive", "impersonationActive := session != nil"}},
		}},
		"disclosure_behavior": {value: "401 for a missing or failing credential (authenticateInternalRequest); 403 for an organization not on the go_api_proof_orgs allowlist", proofs: []claimProof{
			{file: "internal_auth.go", needles: []string{"http.Error(w, \"unauthorized\", http.StatusUnauthorized)"}},
			{file: "query_route.go", needles: []string{"not on the proof-org allowlist"}},
			{file: "proof_write_dispatch_test.go", needles: []string{"TestProofWriteRefusesAnOrgNotOnTheAllowlist"}},
		}},
	},
	"build": {
		"resource_resolver": {value: "none: the response is the process's build identity; the credential's organization is not read", proofs: []claimProof{
			{file: "buildinfo_route.go", needles: []string{"if _, ok := authenticateInternalRequest(", "buildstamp.Body(info)"}},
		}},
		"impersonation_policy": {value: "exempt_no_org_context", proofs: []claimProof{
			{file: "buildinfo_route.go", needles: []string{"if _, ok := authenticateInternalRequest("}},
		}},
		"disclosure_behavior": {value: "401 for a missing or failing credential (authenticateInternalRequest); any verified credential receives the same process build identity", proofs: []claimProof{
			{file: "internal_auth.go", needles: []string{"http.Error(w, \"unauthorized\", http.StatusUnauthorized)"}},
			{file: "buildinfo_route.go", needles: []string{"buildstamp.Body(info)", "authenticateInternalRequest("}},
		}},
	},
	"mcp": {
		"resource_resolver": {value: "organization_from_credential: the organization of the internal identity headers (mcpAuthenticate); a bearer token is refused", proofs: []claimProof{
			{file: "mcp_route.go", needles: []string{"func mcpAuthenticate", "mcpReasonAuthorization", "return authctx.Claims{OrgID: claims.OrgID}, 0, \"\""}},
		}},
		"disclosure_behavior": {value: "401 for a missing or invalid identity; 403 for an identity carrying superuser, impersonation or an operator role; 403 for a root field outside the allowlist or an orgId argument that does not name the caller's organization; 404 for a root field not enabled for the MCP caller class", proofs: []claimProof{
			{file: "mcp_route.go", needles: []string{"mcpReasonElevatedClaim", "mcpReasonRootField", "every orgId argument must name the caller's org", "mcpReasonRootFieldNotEnabled"}},
			{file: "mcp_route_test.go", needles: []string{"TestMCPRefusesElevatedClaimsLoudlyWithZeroClickHouse"}},
		}},
	},
}
