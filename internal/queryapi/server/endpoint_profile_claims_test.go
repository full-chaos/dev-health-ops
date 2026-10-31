package server

type claimProof struct {
	file    string
	decl    string
	needles []string
	anyOf   []string
}

type rowClaim struct {
	value  string
	proofs []claimProof
}

var rowClaims = map[string]map[string]rowClaim{
	"rest": {
		"resource_resolver": {value: "organization_from_credential: the verified credential's organization (claims.OrgID); no request parameter names the organization", proofs: []claimProof{
			{file: "rest_error_response.go", decl: "authenticateRESTRequest", needles: []string{"return authctx.Claims{OrgID: claims.OrgID}, true"}, anyOf: []string{}},
		}},
		"disclosure_behavior": {value: "401 for a missing or failing credential (authenticateRESTRequest); 403 Organization context required for a valid edge or user access token with no organization unless the caller is a superuser, who passes; 503 when the users-row store is unavailable and 500 otherwise (policy.WriteAuthFailure)", proofs: []claimProof{
			{file: "rest_error_response.go", decl: "authenticateRESTRequest", needles: []string{"writeRESTUnauthorized(", "policy.WriteDetail(w, http.StatusForbidden, \"Organization context required\", nil)", "policy.WriteAuthFailure(", "authenticateRESTRouted("}, anyOf: []string{}},
			{file: "rest_access.go", decl: "authenticateRESTRouted", needles: []string{"writeRESTUnauthorized(", "policy.WriteDetail(w, http.StatusForbidden, \"Organization context required\", nil)", "policy.WriteAuthFailure("}, anyOf: []string{}},
			{file: "rest_error_response.go", decl: "writeRESTUnauthorized", needles: []string{"http.StatusUnauthorized"}, anyOf: []string{}},
			{file: "../../api/policy/principal.go", decl: "WriteAuthFailure", needles: []string{"http.StatusServiceUnavailable", "WriteInternal(w)"}, anyOf: []string{}},
			{file: "edge_users_test.go", decl: "TestOrglessEdgeTokenNeverReachesAPlantedRoute", needles: []string{"\"orgless superuser\""}, anyOf: []string{}},
			{file: "rest_access_test.go", decl: "TestQueryAPIRESTOrgMissingIs403ForNonSuperuser", needles: []string{"\"user token\"", "http.StatusForbidden"}, anyOf: []string{}},
		}},
		"primary_validator": {value: "authenticateRESTRequest: bearer edge access token (HS256, live users row), user access token (EdDSA, typ at+jwt, audience dev-health-api; live users row, session and organization membership; only with AUTH_ACCESS_TOKEN_ISSUER set) or EdDSA envelope; a missing or failing credential answers 401; a valid edge or user access token with no organization answers 403 Organization context required unless the caller is a superuser, who passes; a failed users-row read is answered by policy.WriteAuthFailure.", proofs: []claimProof{
			{file: "rest_error_response.go", decl: "authenticateRESTRequest", needles: []string{"writeRESTUnauthorized(", "policy.WriteDetail(w, http.StatusForbidden, \"Organization context required\", nil)", "policy.WriteAuthFailure(", "authenticateRESTRouted("}, anyOf: []string{}},
			{file: "rest_access.go", decl: "authenticateRESTRouted", needles: []string{"edgeVerifier.VerifyRouted(ctx, routeProfile(r), token)", "policy.WriteDetail(w, http.StatusForbidden, \"Organization context required\", nil)", "policy.WriteAuthFailure("}, anyOf: []string{}},
			{file: "../principal/edge_access.go", decl: "AccessClassOf", needles: []string{"header.Typ == UserAccessTokenTyp"}, anyOf: []string{}},
			{file: "access_verify.go", decl: "accessFromEnv", needles: []string{"accessTokenIssuer(getenv)"}, anyOf: []string{}},
			{file: "rest_error_response.go", decl: "writeRESTUnauthorized", needles: []string{"http.StatusUnauthorized"}, anyOf: []string{}},
			{file: "../../api/policy/principal.go", decl: "WriteAuthFailure", needles: []string{"http.StatusServiceUnavailable", "WriteInternal(w)"}, anyOf: []string{}},
			{file: "edge_users_test.go", decl: "TestOrglessEdgeTokenNeverReachesAPlantedRoute", needles: []string{"\"orgless superuser\""}, anyOf: []string{}},
			{file: "rest_access_test.go", decl: "TestQueryAPIRESTUserTokenPath", needles: []string{"\"revoked session\"", "\"not a member of the org\""}, anyOf: []string{}},
		}},
	},
	"gql": {
		"resource_resolver": {value: "organization_from_header_or_credential: the X-Org-Id header when the caller may use it (a superuser, the caller's own organization, or a member of that organization), else the credential's organization (scope.OrgScope)", proofs: []claimProof{
			{file: "../../api/policy/scope.go", decl: "Scope.OrgScope", needles: []string{"headerOrgID", "s.mayUseOrg(r.Context(), user, headerOrgID)", "resolved = headerOrgID", "case user != nil && user.OrgID != \"\":"}, anyOf: []string{}},
			{file: "../../api/policy/scope.go", decl: "Scope.mayUseOrg", needles: []string{"user.IsSuperuser || orgID == user.OrgID", "s.auth.IsMember(ctx, user.UserID, orgID)"}, anyOf: []string{}},
		}},
		"impersonation_policy": {value: "subject_to_impersonation_override", proofs: []claimProof{
			{file: "graphql_edge_route.go", decl: "graphQLEdgeChain", needles: []string{"scope.Impersonation(handler)"}, anyOf: []string{}},
		}},
		"disclosure_behavior": {value: "401 Authentication required (authenticateGraphQLEdge); 403 for an X-Org-Id the caller may not use (scope.OrgScope); 404 for a browser GET; 404 UNREGISTERED_DOCUMENT for an authenticated unregistered document; 500 when an identity read (the users row, the membership or the impersonation session) fails: the request fails closed and is not served (policy.Scope middleware -> WriteInternal; authenticateGraphQLEdge answers the same 500 on edgeUnavailable)", proofs: []claimProof{
			{file: "graphql_edge_route.go", decl: "authenticateGraphQLEdge", needles: []string{"policy.WriteDetail(w, http.StatusUnauthorized, \"Authentication required\", nil)"}, anyOf: []string{}},
			{file: "graphql_edge_route.go", decl: "authenticateGraphQLEdge", needles: []string{"case edgeUnavailable:", "policy.WriteInternal(w)"}, anyOf: []string{}},
			{file: "../../api/policy/scope.go", decl: "Scope.Impersonation", needles: []string{"session lookup failed; refusing the request", "WriteInternal(w)"}, anyOf: []string{}},
			{file: "../../api/policy/callerlookup.go", decl: "writeCallerLookupFailure", needles: []string{"WriteInternal(w)"}, anyOf: []string{}},
			{file: "../../api/policy/scope.go", decl: "Scope.OrgScope", needles: []string{"writeOrgDenied(w)"}, anyOf: []string{}},
			{file: "../../api/policy/scope.go", decl: "writeOrgDenied", needles: []string{"http.StatusForbidden"}, anyOf: []string{}},
			{file: "graphql_edge_route.go", decl: "refuseIDE", needles: []string{"plainRefusal(http.StatusNotFound, \"Not Found\")"}, anyOf: []string{}},
			{file: "graphql_edge_route.go", decl: "refuseGraphQLEdgeUnregistered", needles: []string{"writeGraphQLError(w, http.StatusNotFound, \"This GraphQL document is not registered.\", \"UNREGISTERED_DOCUMENT\")"}, anyOf: []string{}},
		}},
	},
	"query": {
		"resource_resolver": {value: "organization_from_credential: claims.OrgID returned by authenticateInternalRequest", proofs: []claimProof{
			{file: "internal_auth.go", decl: "authenticateInternalRequest", needles: []string{"OrgID: claims.OrgID"}, anyOf: []string{}},
		}},
		"impersonation_policy": {value: "subject_to_impersonation_override", proofs: []claimProof{
			{file: "internal_auth.go", decl: "authenticateInternalRequest", needles: []string{"ImpersonationActive: claims.ImpersonationActive"}, anyOf: []string{}},
			{file: "internal_auth.go", decl: "checkEdgeCarrier", needles: []string{"impersonationActive := session != nil"}, anyOf: []string{}},
		}},
		"disclosure_behavior": {value: "401 for a missing or failing credential (authenticateInternalRequest); 404 for an authenticated unregistered document (shared dispatcher)", proofs: []claimProof{
			{file: "internal_auth.go", decl: "authenticateInternalRequest", needles: []string{"http.Error(w, \"unauthorized\", http.StatusUnauthorized)"}, anyOf: []string{}},
			{file: "query_route.go", decl: "newDocumentDispatchHandler", needles: []string{"operationForDocument(query, operationByDigest)"}, anyOf: []string{}},
			{file: "internal_auth_test.go", decl: "TestQueryStillAnswers404ForAnUnregisteredDocumentOnceAuthenticated", needles: []string{"http.StatusNotFound"}, anyOf: []string{}},
		}},
	},
	"proofw": {
		"resource_resolver": {value: "organization_from_credential: claims.OrgID returned by authenticateInternalRequest; the organization must be on the go_api_proof_orgs allowlist", proofs: []claimProof{
			{file: "internal_auth.go", decl: "authenticateInternalRequest", needles: []string{"OrgID: claims.OrgID"}, anyOf: []string{}},
			{file: "query_route.go", decl: "newProofOrgAllowed", needles: []string{"postgresstore.ProofOrgAllowed(ctx, pool, orgID)"}, anyOf: []string{}},
			{file: "query_route.go", decl: "newDocumentDispatchHandler", needles: []string{"orgAllowed(r.Context(), claims.OrgID)"}, anyOf: []string{}},
		}},
		"impersonation_policy": {value: "subject_to_impersonation_override", proofs: []claimProof{
			{file: "internal_auth.go", decl: "authenticateInternalRequest", needles: []string{"ImpersonationActive: claims.ImpersonationActive"}, anyOf: []string{}},
			{file: "internal_auth.go", decl: "checkEdgeCarrier", needles: []string{"impersonationActive := session != nil"}, anyOf: []string{}},
		}},
		"disclosure_behavior": {value: "401 for a missing or failing credential (authenticateInternalRequest); 403 for an organization not on the go_api_proof_orgs allowlist", proofs: []claimProof{
			{file: "internal_auth.go", decl: "authenticateInternalRequest", needles: []string{"http.Error(w, \"unauthorized\", http.StatusUnauthorized)"}, anyOf: []string{}},
			{file: "query_route.go", decl: "newDocumentDispatchHandler", needles: []string{"http.Error(w, \"this org is not enabled for proof-write\", http.StatusForbidden)"}, anyOf: []string{}},
			{file: "proof_write_dispatch_test.go", decl: "TestProofWriteRefusesAnOrgNotOnTheAllowlist", needles: []string{"http.StatusForbidden"}, anyOf: []string{}},
		}},
	},
	"build": {
		"resource_resolver": {value: "none: the response is the process's build identity; the credential's organization is not read", proofs: []claimProof{
			{file: "buildinfo_route.go", decl: "newBuildInfoHandler", needles: []string{"if _, ok := authenticateInternalRequest(w, r, verifier, nil, nil); !ok {", "json.NewEncoder(w).Encode(json.RawMessage(body))"}, anyOf: []string{}},
		}},
		"impersonation_policy": {value: "exempt_no_org_context", proofs: []claimProof{
			{file: "buildinfo_route.go", decl: "newBuildInfoHandler", needles: []string{"if _, ok := authenticateInternalRequest(w, r, verifier, nil, nil); !ok {"}, anyOf: []string{}},
		}},
		"disclosure_behavior": {value: "401 for a missing or failing credential (authenticateInternalRequest); any verified credential receives the same process build identity", proofs: []claimProof{
			{file: "internal_auth.go", decl: "authenticateInternalRequest", needles: []string{"http.Error(w, \"unauthorized\", http.StatusUnauthorized)"}, anyOf: []string{}},
			{file: "buildinfo_route.go", decl: "newBuildInfoHandler", needles: []string{"if _, ok := authenticateInternalRequest(w, r, verifier, nil, nil); !ok {", "json.NewEncoder(w).Encode(json.RawMessage(body))"}, anyOf: []string{}},
		}},
	},
	"mcp": {
		"resource_resolver": {value: "organization_from_credential: the organization of the internal identity headers (mcpAuthenticate); a bearer token is refused", proofs: []claimProof{
			{file: "mcp_route.go", decl: "mcpAuthenticate", needles: []string{"return authctx.Claims{OrgID: claims.OrgID}, 0, \"\"", "http.StatusUnauthorized, mcpReasonAuthorization"}, anyOf: []string{}},
		}},
		"disclosure_behavior": {value: "401 for a missing or invalid identity; 403 for an identity carrying superuser, impersonation or an operator role; 403 for a root field outside the allowlist or an orgId argument that does not name the caller's organization; 404 for a root field not enabled for the MCP caller class", proofs: []claimProof{
			{file: "mcp_route.go", decl: "mcpAuthenticate", needles: []string{"http.StatusUnauthorized, mcpReasonNoCarrier", "http.StatusForbidden, mcpReasonElevatedClaim"}, anyOf: []string{}},
			{file: "mcp_route.go", decl: "mcpHandler.ServeHTTP", needles: []string{"refuse(http.StatusForbidden, mcpReasonRootField", "refuse(http.StatusForbidden, orgReason", "refuse(http.StatusNotFound, mcpReasonRootFieldNotEnabled"}, anyOf: []string{}},
		}},
	},
}
