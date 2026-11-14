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

var restResolverClaim = rowClaim{value: "organization_from_credential: the verified credential's organization (claims.OrgID); no request parameter names the organization", proofs: []claimProof{
	{file: "rest_access.go", decl: "authenticateRESTRouted", needles: []string{"return authctx.Claims{OrgID: claims.OrgID, Scope: scope}, true"}, anyOf: []string{}},
}}

var restDisclosureClaim = rowClaim{value: "401 for a missing or failing credential (authenticateRESTRequest); 403 Organization context required for a valid edge or user access token with no organization unless the caller is a superuser, who passes; 503 when the users-row store is unavailable and 500 otherwise (policy.WriteAuthFailure)", proofs: []claimProof{
	{file: "rest_error_response.go", decl: "authenticateRESTRequest", needles: []string{"writeRESTUnauthorized(", "authenticateRESTRouted("}, anyOf: []string{}},
	{file: "rest_access.go", decl: "authenticateRESTRouted", needles: []string{"writeRESTUnauthorized(", "policy.WriteDetail(w, http.StatusForbidden, \"Organization context required\", nil)", "policy.WriteAuthFailure("}, anyOf: []string{}},
	{file: "rest_error_response.go", decl: "writeRESTUnauthorized", needles: []string{"http.StatusUnauthorized"}, anyOf: []string{}},
	{file: "../../api/policy/principal.go", decl: "WriteAuthFailure", needles: []string{"http.StatusServiceUnavailable", "WriteInternal(w)"}, anyOf: []string{}},
	{file: "rest_access_test.go", decl: "TestQueryAPIRESTOrgMissingIs403ForNonSuperuser", needles: []string{"\"user token\"", "http.StatusForbidden", "superuser %s with no org"}, anyOf: []string{}},
}}

var restValidatorClaim = rowClaim{value: "authenticateRESTRequest: bearer user access token (EdDSA, typ at+jwt, audience dev-health-api; live users row, session and organization membership; only with AUTH_ACCESS_TOKEN_ISSUER set); an HS256 edge access token is refused (alg_not_allowed); any other bearer, the retired effective-principal envelope included, is refused as an unaccepted class; a missing or failing credential answers 401; a valid user access token with no organization answers 403 Organization context required unless the caller is a superuser, who passes; a failed users-row read is answered by policy.WriteAuthFailure.", proofs: []claimProof{
	{file: "rest_error_response.go", decl: "authenticateRESTRequest", needles: []string{"writeRESTUnauthorized(", "authenticateRESTRouted("}, anyOf: []string{}},
	{file: "rest_access.go", decl: "authenticateRESTRouted", needles: []string{"edgeVerifier.VerifyRouted(ctx, routeProfile(r), token)", "policy.WriteDetail(w, http.StatusForbidden, \"Organization context required\", nil)", "policy.WriteAuthFailure("}, anyOf: []string{}},
	{file: "../principal/edge_access.go", decl: "AccessClassOf", needles: []string{"header.Typ == UserAccessTokenTyp"}, anyOf: []string{}},
	{file: "../../auth/accessverify/accessverify.go", decl: "Validator.Validate", needles: []string{"default:\n\t\treturn nil, v.refuse(ctx, row.Service, row.Profile, ReasonAlgNotAllowed)"}, anyOf: []string{}},
	{file: "access_verify.go", decl: "accessFromEnv", needles: []string{"accessTokenIssuer(getenv)"}, anyOf: []string{}},
	{file: "rest_error_response.go", decl: "writeRESTUnauthorized", needles: []string{"http.StatusUnauthorized"}, anyOf: []string{}},
	{file: "../../api/policy/principal.go", decl: "WriteAuthFailure", needles: []string{"http.StatusServiceUnavailable", "WriteInternal(w)"}, anyOf: []string{}},
	{file: "rest_access_test.go", decl: "TestQueryAPIRESTUserTokenPath", needles: []string{"\"revoked session\"", "\"not a member of the org\""}, anyOf: []string{}},
	{file: "rest_access_test.go", decl: "TestQueryAPIRESTOrgMissingIs403ForNonSuperuser", needles: []string{"http.StatusForbidden", "superuser %s with no org"}, anyOf: []string{}},
}}

var stateCellsProof = claimProof{file: "endpoint_profile_live_test.go", decl: "userStateCells", needles: []string{"\"users row deactivated after the first request\"", "\"session revoked after the first request\"", "\"token version moved after the first request\"", "\"membership removed after the first request\"", "http.StatusUnauthorized"}}

var stackStateCellsProof = claimProof{file: "endpoint_profile_live_test.go", decl: "stackStateCellsAs", needles: []string{"range userStateCells", "cell.change(store)", "cell.after"}}

var internalCacheProofs = []claimProof{
	stateCellsProof, stackStateCellsProof,
	{file: "endpoint_profile_live_test.go", decl: "driveInternal", needles: []string{"stackStateCells(t, o, listener+\": \", q)", "\"internal headers, every store read failing\""}},
	{file: "internal_auth.go", decl: "authenticateInternalRequest", needles: []string{"internalidentity.FromHeader(r.Header)"}},
}

var restCacheClaim = rowClaim{value: "none: per-request read: a user access token's users row, session, token version and organization membership are read on every request and nothing is cached between requests (a change between two requests answers the second); an enforced row asks the PDP on every request, with no allow cache", proofs: []claimProof{
	stateCellsProof,
	{file: "endpoint_profile_live_test.go", decl: "restStateCells", needles: []string{"range userStateCells", "cell.change(f.store)"}},
	{file: "../pdp/uncached_test.go", decl: "TestPDPGateHoldsNoDecisionCache", needles: []string{"g.decider.(*pep.Client)", "each read asks the PDP"}},
}}

var restImpersonationClaim = rowClaim{value: "subject_to_impersonation_override", proofs: []claimProof{
	{file: "endpoint_profile_live_test.go", decl: "restImpersonationCells", needles: []string{"\"act\"", "liveTargetOrg", "o.impersonationPolicy(true)"}},
	{file: "rest_access.go", decl: "authenticateRESTRouted", needles: []string{"return authctx.Claims{OrgID: claims.OrgID, Scope: scope}, true"}},
}}

var restIDDisclosureClaim = rowClaim{value: restDisclosureClaim.value + "; 404 for a path id with no row in the caller's organization, the same answer as an id that exists nowhere (the read binds the credential's organization)", proofs: append(append([]claimProof(nil), restDisclosureClaim.proofs...),
	claimProof{file: "endpoint_profile_live_test.go", decl: "restIDCells", needles: []string{"\"a person id another organization holds\"", "\"a person id nothing holds\"", "foreign.body != unknown.body", "bindingValue(client.bindings[i], \"org_id\")"}},
	claimProof{file: "../people/resolve.go", decl: "resolvePersonIdentity", needles: []string{"AND org_id = {org_id:String}", "{Name: \"org_id\", Value: orgID}"}},
	claimProof{file: "workunit_explain_route.go", decl: "newWorkUnitExplainHandler", needles: []string{"OrgID:              org,", "writeWorkUnitExplainNotFound(w, r, org, workUnitID)"}},
)}

var queryCacheClaim = rowClaim{value: "none: per-request read: a user access token's users row, session, token version and organization membership, and the impersonation session, are read on every request and nothing is cached between requests (a change between two requests answers the second); the internal identity headers read no state", proofs: internalCacheProofs}

var rowClaims = map[string]map[string]rowClaim{
	"rest": {
		"resource_resolver":            restResolverClaim,
		"disclosure_behavior":          restDisclosureClaim,
		"primary_validator":            restValidatorClaim,
		"current_state_cache_behavior": restCacheClaim,
		"impersonation_policy":         restImpersonationClaim,
	},
	"rest-id": {
		"resource_resolver":            restResolverClaim,
		"disclosure_behavior":          restIDDisclosureClaim,
		"primary_validator":            restValidatorClaim,
		"current_state_cache_behavior": restCacheClaim,
		"impersonation_policy":         restImpersonationClaim,
	},
	"gql": {
		"current_state_cache_behavior": {value: "none: per-request read: the users row, session, token version and organization membership, the membership of an X-Org-Id organization and the impersonation session are read on every request and nothing is cached between requests (a change between two requests answers the second)", proofs: []claimProof{
			stateCellsProof, stackStateCellsProof,
			{file: "endpoint_profile_live_test.go", decl: "driveGraphQL", needles: []string{"stackStateCells(t, o, listener+\": \", request(iaDocument, nil, \"\"))"}},
		}},
		"resource_resolver": {value: "organization_from_header_or_credential: the X-Org-Id header when the caller may use it (a superuser, the caller's own organization, or a member of that organization), else the credential's organization (scope.OrgScope)", proofs: []claimProof{
			{file: "../../api/policy/scope.go", decl: "Scope.OrgScope", needles: []string{"headerOrgID", "s.mayUseOrg(r.Context(), user, headerOrgID)", "resolved = headerOrgID", "case user != nil && user.OrgID != \"\":"}, anyOf: []string{}},
			{file: "../../api/policy/scope.go", decl: "Scope.mayUseOrg", needles: []string{"user.Holds(ActionAdminOrgs) || orgID == user.OrgID", "s.auth.IsMember(ctx, user.UserID, orgID)"}, anyOf: []string{}},
		}},
		"impersonation_policy": {value: "subject_to_impersonation_override", proofs: []claimProof{
			{file: "graphql_edge_route.go", decl: "graphQLEdgeChain", needles: []string{"scope.Impersonation(handler)"}, anyOf: []string{}},
		}},
		"disclosure_behavior": {value: "401 Authentication required (authenticateGraphQLEdge); 403 for an X-Org-Id the caller may not use (scope.OrgScope); 403 Not allowed in an impersonation session for the legacy impersonation caller (the admin's own bearer beside a live go-api impersonation row) while AUTH_PROXY_IMPERSONATION is on (scope.Impersonation, RefuseLegacyImpersonationReading), and for a caller that no longer holds admin.impersonate on a POST of a mutation document only, by the document's operation type (graphQLEdgeReads; its query document is served as itself and does not read the row, and a GET reads by its method); 404 for a browser GET; 404 UNREGISTERED_DOCUMENT for an authenticated unregistered document; 500 when an identity read (the users row or the membership) fails for any reason other than policy.ErrUnavailable: the request fails closed and is not served (policy.Scope middleware -> WriteInternal; authenticateGraphQLEdge answers the same 500 on edgeUnavailable); 503 Database temporarily unavailable when the caller lookup is policy.ErrUnavailable: the users store is unavailable, or the token key source is unreadable on the user access token path (writeCallerLookupFailure -> policy.WriteAuthFailure), and when the impersonation session read fails for any reason: the request fails closed and is not served (Scope.Impersonation -> WriteAuthFailure(w, \"\", ErrUnavailable))", proofs: []claimProof{
			{file: "graphql_edge_route.go", decl: "authenticateGraphQLEdge", needles: []string{"policy.WriteDetail(w, http.StatusUnauthorized, \"Authentication required\", nil)"}, anyOf: []string{}},
			{file: "graphql_edge_route.go", decl: "graphQLEdgeChain", needles: []string{"scope.RefuseLegacyImpersonationReading(deps.refuseLegacyImpersonation, graphQLEdgeReads(deps.maxBytes))"}, anyOf: []string{}},
			{file: "legacy_impersonation.go", decl: "graphQLEdgeReads", needles: []string{"digest.DocumentKind(text)", "kind == digest.KindQuery"}, anyOf: []string{}},
			{file: "../../api/policy/scope.go", decl: "Scope.Impersonation", needles: []string{"s.refuseLegacy.reads(r)"}, anyOf: []string{}},
			{file: "legacy_impersonation_refused_test.go", decl: "TestGraphQLRevokedHolderIsRefusedByOperationType", needles: []string{"http.StatusForbidden", "servedAsItself"}, anyOf: []string{}},
			{file: "../../api/policy/scope.go", decl: "Scope.Impersonation", needles: []string{"s.refusesLegacyCaller(r)", "WriteDetail(w, http.StatusForbidden, msgImpersonationReadOnly, nil)"}, anyOf: []string{}},
			{file: "legacy_impersonation_refused_test.go", decl: "TestGraphQLRefusesTheLegacyImpersonationCaller", needles: []string{"http.StatusForbidden"}, anyOf: []string{}},
			{file: "graphql_edge_route.go", decl: "authenticateGraphQLEdge", needles: []string{"case edgeUnavailable:", "policy.WriteInternal(w)"}, anyOf: []string{}},
			{file: "../../api/policy/scope.go", decl: "Scope.Impersonation", needles: []string{"session lookup failed; refusing the request", "WriteAuthFailure(w, \"\", ErrUnavailable)"}, anyOf: []string{}},
			{file: "../../api/policy/callerlookup.go", decl: "writeCallerLookupFailure", needles: []string{"WriteInternal(w)"}, anyOf: []string{}},
			{file: "../../api/policy/callerlookup.go", decl: "writeCallerLookupFailure", needles: []string{"errors.Is(err, ErrUnavailable)", "WriteAuthFailure(w, \"\", ErrUnavailable)"}, anyOf: []string{}},
			{file: "../../api/policy/scope.go", decl: "Scope.OrgScope", needles: []string{"writeOrgDenied(w)"}, anyOf: []string{}},
			{file: "../../api/policy/scope.go", decl: "writeOrgDenied", needles: []string{"http.StatusForbidden"}, anyOf: []string{}},
			{file: "graphql_edge_route.go", decl: "refuseIDE", needles: []string{"plainRefusal(http.StatusNotFound, \"Not Found\")"}, anyOf: []string{}},
			{file: "graphql_edge_route.go", decl: "refuseGraphQLEdgeUnregistered", needles: []string{"writeGraphQLError(w, http.StatusNotFound, \"This GraphQL document is not registered.\", \"UNREGISTERED_DOCUMENT\")"}, anyOf: []string{}},
		}},
	},
	"query": {
		"current_state_cache_behavior": queryCacheClaim,
		"resource_resolver": {value: "organization_from_credential: claims.OrgID returned by authenticateInternalRequest", proofs: []claimProof{
			{file: "internal_auth.go", decl: "authenticateInternalRequest", needles: []string{"return withLegacyScope(claims), true"}, anyOf: []string{}},
		}},
		"impersonation_policy": {value: "subject_to_impersonation_override", proofs: []claimProof{
			{file: "../internalidentity/internalidentity.go", decl: "FromHeader", needles: []string{"claims.ImpersonationActive = impersonating"}, anyOf: []string{}},
			{file: "internal_auth.go", decl: "checkEdgeCarrier", needles: []string{"impersonationActive := session != nil"}, anyOf: []string{}},
		}},
		"disclosure_behavior": {value: "401 for a missing or refused credential (authenticateInternalRequest); 503 Database temporarily unavailable when a live identity read of a user access token cannot be made (the users row, the session, the organization membership, the token key source or the impersonation session), for any store error: the request fails closed and is not served (authenticateEdgeCarrier -> policy.WriteAuthFailure(w, \"\", policy.ErrUnavailable), go-api's answer for a session read it could not make); a store error other than policy.ErrUnavailable is this 503 too, where go-api's caller and membership reads and /graphql answer the generic Internal Server Error; 403 Not allowed in an impersonation session for the legacy impersonation caller (the admin's own bearer beside a live go-api impersonation row) while AUTH_PROXY_IMPERSONATION is on (checkEdgeCarrier, policy.WriteImpersonationRefused); 403 Not allowed in an impersonation session for a non-query document (a mutation, a subscription, a text that does not parse) of a caller without admin.impersonate beside a live go-api impersonation row while AUTH_PROXY_IMPERSONATION is on (liveScope, newDocumentDispatchHandler), and the same 503 for that document when the row cannot be read; its queries are served as itself; 404 for an authenticated unregistered document (shared dispatcher)", proofs: []claimProof{
			{file: "internal_auth.go", decl: "liveScope", needles: []string{"markNonHolder(ctx, user.ID)"}, anyOf: []string{}},
			{file: "query_route.go", decl: "newDocumentDispatchHandler", needles: []string{"nonHolderOf(r.Context())", "!legacyDocumentReads(query)", "edgeStore.ActiveImpersonation(r.Context(), id)", "policy.WriteImpersonationRefused(w)"}, anyOf: []string{}},
			{file: "legacy_impersonation_nonholder_test.go", decl: "TestQueryRefusesANonHoldersNonQueryDocumentBesideALiveRowUnderTheSwitch", needles: []string{"http.StatusForbidden"}, anyOf: []string{}},
			{file: "legacy_impersonation_nonholder_test.go", decl: "TestQueryFailsClosedForANonHoldersWriteWhoseRowReadFailsUnderTheSwitch", needles: []string{"http.StatusServiceUnavailable", "restUnavailable"}, anyOf: []string{}},
			{file: "internal_auth.go", decl: "authenticateEdgeCarrier", needles: []string{"case edgeUnavailable:", "policy.WriteAuthFailure(w, \"\", policy.ErrUnavailable)"}, anyOf: []string{}},
			{file: "internal_auth.go", decl: "checkEdgeCarrier", needles: []string{"policy.IsRefusal(err)"}, anyOf: []string{}},
			{file: "query_route.go", decl: "newDocumentDispatchHandler", needles: []string{"policy.WriteAuthFailure(w, \"\", policy.ErrUnavailable)"}, anyOf: []string{}},
			{file: "query_store_unavailable_test.go", decl: "TestQueryFamilyAnswersAFailedLiveReadAsUnavailable", needles: []string{"unavailableProblem(got)", "legacyQueryRoutes"}, anyOf: []string{}},
			{file: "query_store_unavailable_test.go", decl: "TestGraphQLAnswersAnOtherStoreErrorAtTheUsersReadAsBefore", needles: []string{"http.StatusInternalServerError", "unavailableProblem("}, anyOf: []string{}},
			{file: "internal_auth.go", decl: "liveScope", needles: []string{"user.Holds(policy.ActionAdminImpersonate)", "errLegacyImpersonationRefused"}, anyOf: []string{}},
			{file: "legacy_impersonation_holder_test.go", decl: "TestQueryRefusesAnActionOnlyHoldersWriteBesideALiveRow", needles: []string{"http.StatusForbidden"}, anyOf: []string{}},
			{file: "internal_auth.go", decl: "authenticateInternalRequest", needles: []string{"http.Error(w, \"unauthorized\", http.StatusUnauthorized)"}, anyOf: []string{}},
			{file: "query_route.go", decl: "newDocumentDispatchHandler", needles: []string{"operationForDocument(query, operationByDigest)"}, anyOf: []string{}},
			{file: "internal_auth.go", decl: "liveScope", needles: []string{"legacyImpersonationRefused(ctx)"}, anyOf: []string{}},
			{file: "internal_auth.go", decl: "authenticateEdgeCarrier", needles: []string{"policy.WriteImpersonationRefused(w)"}, anyOf: []string{}},
			{file: "../../api/policy/impersonation_refused.go", decl: "WriteImpersonationRefused", needles: []string{"http.StatusForbidden", "msgImpersonationReadOnly"}, anyOf: []string{}},
			{file: "legacy_impersonation_query_test.go", decl: "TestQueryRefusesTheLegacyImpersonationCaller", needles: []string{"http.StatusForbidden"}, anyOf: []string{}},
			{file: "internal_auth_test.go", decl: "TestQueryStillAnswers404ForAnUnregisteredDocumentOnceAuthenticated", needles: []string{"http.StatusNotFound"}, anyOf: []string{}},
		}},
	},
	"proofw": {
		"current_state_cache_behavior": {value: queryCacheClaim.value + "; the go_api_proof_orgs allowlist is read on every request", proofs: append(append([]claimProof(nil), internalCacheProofs...),
			claimProof{file: "query_route.go", decl: "newProofOrgAllowed", needles: []string{"postgresstore.ProofOrgAllowed(ctx, pool, orgID)"}})},
		"resource_resolver": {value: "organization_from_credential: claims.OrgID returned by authenticateInternalRequest; the organization must be on the go_api_proof_orgs allowlist", proofs: []claimProof{
			{file: "internal_auth.go", decl: "authenticateInternalRequest", needles: []string{"return withLegacyScope(claims), true"}, anyOf: []string{}},
			{file: "query_route.go", decl: "newProofOrgAllowed", needles: []string{"postgresstore.ProofOrgAllowed(ctx, pool, orgID)"}, anyOf: []string{}},
			{file: "query_route.go", decl: "newDocumentDispatchHandler", needles: []string{"orgAllowed(r.Context(), claims.OrgID)"}, anyOf: []string{}},
		}},
		"impersonation_policy": {value: "subject_to_impersonation_override", proofs: []claimProof{
			{file: "../internalidentity/internalidentity.go", decl: "FromHeader", needles: []string{"claims.ImpersonationActive = impersonating"}, anyOf: []string{}},
			{file: "internal_auth.go", decl: "checkEdgeCarrier", needles: []string{"impersonationActive := session != nil"}, anyOf: []string{}},
		}},
		"disclosure_behavior": {value: "401 for a missing or refused credential (authenticateInternalRequest); 503 Database temporarily unavailable when a live identity read of a user access token cannot be made (the users row, the session, the organization membership, the token key source or the impersonation session), for any store error: the request fails closed and is not served (authenticateEdgeCarrier -> policy.WriteAuthFailure(w, \"\", policy.ErrUnavailable), go-api's answer for a session read it could not make); a store error other than policy.ErrUnavailable is this 503 too, where go-api's caller and membership reads and /graphql answer the generic Internal Server Error; 403 Not allowed in an impersonation session for the legacy impersonation caller (the admin's own bearer beside a live go-api impersonation row) while AUTH_PROXY_IMPERSONATION is on (checkEdgeCarrier, policy.WriteImpersonationRefused); 403 Not allowed in an impersonation session for a non-query document (a mutation, a subscription, a text that does not parse) of a caller without admin.impersonate beside a live go-api impersonation row while AUTH_PROXY_IMPERSONATION is on (liveScope, newDocumentDispatchHandler), and the same 503 for that document when the row cannot be read; its queries are served as itself; 403 for an organization not on the go_api_proof_orgs allowlist", proofs: []claimProof{
			{file: "internal_auth.go", decl: "liveScope", needles: []string{"markNonHolder(ctx, user.ID)"}, anyOf: []string{}},
			{file: "query_route.go", decl: "newDocumentDispatchHandler", needles: []string{"nonHolderOf(r.Context())", "!legacyDocumentReads(query)", "edgeStore.ActiveImpersonation(r.Context(), id)", "policy.WriteImpersonationRefused(w)"}, anyOf: []string{}},
			{file: "legacy_impersonation_nonholder_test.go", decl: "TestQueryRefusesANonHoldersNonQueryDocumentBesideALiveRowUnderTheSwitch", needles: []string{"http.StatusForbidden"}, anyOf: []string{}},
			{file: "legacy_impersonation_nonholder_test.go", decl: "TestQueryFailsClosedForANonHoldersWriteWhoseRowReadFailsUnderTheSwitch", needles: []string{"http.StatusServiceUnavailable", "restUnavailable"}, anyOf: []string{}},
			{file: "internal_auth.go", decl: "authenticateEdgeCarrier", needles: []string{"case edgeUnavailable:", "policy.WriteAuthFailure(w, \"\", policy.ErrUnavailable)"}, anyOf: []string{}},
			{file: "internal_auth.go", decl: "checkEdgeCarrier", needles: []string{"policy.IsRefusal(err)"}, anyOf: []string{}},
			{file: "query_route.go", decl: "newDocumentDispatchHandler", needles: []string{"policy.WriteAuthFailure(w, \"\", policy.ErrUnavailable)"}, anyOf: []string{}},
			{file: "query_store_unavailable_test.go", decl: "TestQueryFamilyAnswersAFailedLiveReadAsUnavailable", needles: []string{"unavailableProblem(got)", "legacyQueryRoutes"}, anyOf: []string{}},
			{file: "query_store_unavailable_test.go", decl: "TestGraphQLAnswersAnOtherStoreErrorAtTheUsersReadAsBefore", needles: []string{"http.StatusInternalServerError", "unavailableProblem("}, anyOf: []string{}},
			{file: "internal_auth.go", decl: "liveScope", needles: []string{"user.Holds(policy.ActionAdminImpersonate)", "errLegacyImpersonationRefused"}, anyOf: []string{}},
			{file: "legacy_impersonation_holder_test.go", decl: "TestQueryRefusesAnActionOnlyHoldersWriteBesideALiveRow", needles: []string{"http.StatusForbidden"}, anyOf: []string{}},
			{file: "internal_auth.go", decl: "authenticateInternalRequest", needles: []string{"http.Error(w, \"unauthorized\", http.StatusUnauthorized)"}, anyOf: []string{}},
			{file: "query_route.go", decl: "newDocumentDispatchHandler", needles: []string{"http.Error(w, \"this org is not enabled for proof-write\", http.StatusForbidden)"}, anyOf: []string{}},
			{file: "internal_auth.go", decl: "liveScope", needles: []string{"legacyImpersonationRefused(ctx)"}, anyOf: []string{}},
			{file: "internal_auth.go", decl: "authenticateEdgeCarrier", needles: []string{"policy.WriteImpersonationRefused(w)"}, anyOf: []string{}},
			{file: "../../api/policy/impersonation_refused.go", decl: "WriteImpersonationRefused", needles: []string{"http.StatusForbidden", "msgImpersonationReadOnly"}, anyOf: []string{}},
			{file: "legacy_impersonation_query_test.go", decl: "TestProofWriteDoorRefusesTheLegacyImpersonationCaller", needles: []string{"http.StatusForbidden"}, anyOf: []string{}},
			{file: "proof_write_dispatch_test.go", decl: "TestProofWriteRefusesAnOrgNotOnTheAllowlist", needles: []string{"http.StatusForbidden"}, anyOf: []string{}},
		}},
	},
	"build": {
		"current_state_cache_behavior": {value: "none: per-request read: a user access token's users row, session, token version and organization membership, and the impersonation session, are read on every request and nothing is cached between requests (a change between two requests answers the second); the internal identity headers read no state; their claims are not read", proofs: []claimProof{
			stateCellsProof,
			{file: "buildinfo_route.go", decl: "authenticateBuildInfo", needles: []string{"authenticateInternalRequest(w, r, verifier, nil, nil)", "authenticateEdgeCarrier(w, r, edgeAuth, edgeStore, token)"}},
			{file: "endpoint_profile_live_test.go", decl: "driveBuildInfo", needles: []string{"range []func(*accessStore){nil, noStateRead}", "got.body != identity", "stackStateCellsAs(t, o, listener+\": platform admin, \", q, asSuperuser)"}},
		}},
		"resource_resolver": {value: "none: the response is the process's build identity; the credential's organization is not read", proofs: []claimProof{
			{file: "buildinfo_route.go", decl: "newBuildInfoHandler", needles: []string{"if !authenticateBuildInfo(w, r, verifier, edgeAuth, edgeStore) {", "json.NewEncoder(w).Encode(json.RawMessage(body))"}, anyOf: []string{}},
		}},
		"impersonation_policy": {value: "exempt_no_org_context", proofs: []claimProof{
			{file: "buildinfo_route.go", decl: "newBuildInfoHandler", needles: []string{"if !authenticateBuildInfo(w, r, verifier, edgeAuth, edgeStore) {"}, anyOf: []string{}},
			{file: "buildinfo_route.go", decl: "authenticateBuildInfo", needles: []string{"claims.ImpersonationActive || claims.Delegated"}},
		}},
		"disclosure_behavior": {value: "401 for a missing or refused credential (authenticateInternalRequest), and for a user access token whose live users row is not a platform admin (admin.orgs), that has an active impersonation session or that is delegated (authenticateBuildInfo); 503 Database temporarily unavailable when a live identity read of a user access token cannot be made, for any store error (authenticateEdgeCarrier -> policy.WriteAuthFailure(w, \"\", policy.ErrUnavailable)); any accepted credential receives the same process build identity", proofs: []claimProof{
			{file: "internal_auth.go", decl: "authenticateEdgeCarrier", needles: []string{"case edgeUnavailable:", "policy.WriteAuthFailure(w, \"\", policy.ErrUnavailable)"}, anyOf: []string{}},
			{file: "query_store_unavailable_test.go", decl: "TestBuildInfoAnswersAFailedLiveReadAsUnavailable", needles: []string{"\"/buildinfo\"", "unavailableProblem("}, anyOf: []string{}},
			{file: "endpoint_profile_live_test.go", decl: "driveBuildInfo", needles: []string{"\": platform admin, users store unavailable\"", "http.StatusServiceUnavailable"}},
			{file: "internal_auth.go", decl: "authenticateInternalRequest", needles: []string{"http.Error(w, \"unauthorized\", http.StatusUnauthorized)"}, anyOf: []string{}},
			{file: "buildinfo_route.go", decl: "authenticateBuildInfo", needles: []string{"!claims.Holds(rule.ActionAdminOrgs) || claims.ImpersonationActive || claims.Delegated", "refuseInternal(w, r, \"buildinfo_not_platform_admin\", \"edge\")"}},
			{file: "endpoint_profile_live_test.go", decl: "buildInfoUserTokenCells", needles: []string{"\"platform-admin user token\"", "\"delegated platform-admin user token\"", "\"viewer\", \"member\", \"admin\", \"owner\""}},
			{file: "buildinfo_route.go", decl: "newBuildInfoHandler", needles: []string{"if !authenticateBuildInfo(w, r, verifier, edgeAuth, edgeStore) {", "json.NewEncoder(w).Encode(json.RawMessage(body))"}, anyOf: []string{}},
		}},
	},
	"mcp": {
		"impersonation_policy": {value: "not_applicable_non_main_app", proofs: []claimProof{
			{file: "mcp_route.go", decl: "mcpAuthenticate", needles: []string{"http.StatusForbidden, mcpReasonElevatedClaim"}},
			{file: "endpoint_profile_live_test.go", decl: "driveMCP", needles: []string{"mcpHeaders(mcpTestOrg, \"member\", \"false\", \"true\")", "reason != mcpReasonElevatedClaim", "\"not_applicable_non_main_app\""}},
		}},
		"current_state_cache_behavior": {value: "none: no state read: the internal identity headers state the identity on every request; nothing is read or cached", proofs: []claimProof{
			{file: "mcp_route.go", decl: "mcpAuthenticate", needles: []string{"internalidentity.FromHeader(r.Header)"}},
			{file: "endpoint_profile_live_test.go", decl: "driveMCP", needles: []string{"\"a valid identity again after a refused one\""}},
		}},
		"resource_resolver": {value: "organization_from_credential: the organization of the internal identity headers (mcpAuthenticate); a bearer token is refused", proofs: []claimProof{
			{file: "mcp_route.go", decl: "mcpAuthenticate", needles: []string{"return legacyClaims(claims.OrgID), 0, \"\"", "http.StatusUnauthorized, mcpReasonAuthorization"}, anyOf: []string{}},
		}},
		"disclosure_behavior": {value: "401 for a missing or invalid identity; 403 for an identity carrying superuser, impersonation or an operator role; 403 for a root field outside the allowlist or an orgId argument that does not name the caller's organization; 404 for a root field not enabled for the MCP caller class", proofs: []claimProof{
			{file: "mcp_route.go", decl: "mcpAuthenticate", needles: []string{"http.StatusUnauthorized, mcpReasonNoCarrier", "http.StatusForbidden, mcpReasonElevatedClaim"}, anyOf: []string{}},
			{file: "mcp_route.go", decl: "mcpHandler.ServeHTTP", needles: []string{"refuse(http.StatusForbidden, mcpReasonRootField", "refuse(http.StatusForbidden, orgReason", "refuse(http.StatusNotFound, mcpReasonRootFieldNotEnabled"}, anyOf: []string{}},
		}},
	},
}
