package authapi

import "net/http"

// PatternAuthorizeRepositories is the repository listing route of the policy decision point: for the principal a
// reference names and one organization, the repositories it may read through acr.context.read or acr.evidence.read.
// It mounts only when the endpoint-profile document holds its row.
const PatternAuthorizeRepositories = "/v1/authorize:repositories"

const (
	opAuthorizeRepositories = "authorize_repositories"

	pdpAuthorizeRepositoriesCallerLimitID = "auth_svc_pdp_authorize_repositories_caller"
)

// DecodeRepositoryListingRequest decodes the body of the listing route: one authorization_request.v1 held to
// DecodeAuthorizationRequest's rules, whose resource is the whole organization's repositories (type repository, key
// "*", organization_id set). Any other resource is refused: the route lists, it never decides one repository.
func DecodeRepositoryListingRequest(body []byte) ([]AuthorizationQuestion, error) {
	q, err := DecodeAuthorizationRequest(body)
	if err != nil {
		return nil, err
	}
	if q.ResourceType != "repository" || q.ResourceKey != "*" || q.OrganizationID == "" {
		return nil, invalidAuthorization("/resource is not a repository listing (type repository, key *, organization_id)")
	}
	return []AuthorizationQuestion{q}, nil
}

// authorizeRepositories answers one authorization_decision.v1, Evaluator.ReadableRepositories: the set of repositories
// the per-key read decisions allow, so an allow carries repository_scope organization (every repository) or the
// repository_allowlist of the principal's own keys, and more keys than the allowlist bound is the deny
// repository_allowlist_too_large. A state failure is the call's 503, never a list.
func (h authorizeHandlers) authorizeRepositories(w http.ResponseWriter, r *http.Request) {
	decisions, ok := h.decide(w, r, opAuthorizeRepositories, maxAuthorizeBody, DecodeRepositoryListingRequest)
	if !ok {
		return
	}
	h.rec.record(r.Context(), opAuthorizeRepositories, "success", reasonAccepted, nil)
	writeJSON(w, http.StatusOK, decisions[0])
}
