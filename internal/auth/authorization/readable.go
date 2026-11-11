package authorization

// ListingDecisionAction is the action whose decision a repository listing answers: acr.credential.issue, the decision
// that names the repositories a credential gets on the approval path today. The listing answers exactly that
// decision for every caller class (platform admin, impersonation, service account, membership default, grants), so a
// reader is never given more or less than the issue path gives it (acp ruling on CHAOS-13542).
const ListingDecisionAction = "acr.credential.issue"

// ReadableRepositories answers which repositories s may read in one organization through a repository action
// (acr.context.read, acr.evidence.read). r is the listing question: ResourceKind repository, ResourceKey "*" and the
// organization; any other shape is invalid_request, an unknown action unknown_action, an action that is not
// repository-kind resource_mismatch.
//
// The answer is Evaluate's decision of ListingDecisionAction for the organization, unchanged: an allow carries
// repository_scope organization (every repository) or the repository_allowlist of repositoryScope (sorted, unique,
// canonical keys; an id-bearing grant lists the live path of its id; [] is none); more than MaxRepositoryAllowlist
// keys is the deny repository_allowlist_too_large, never a truncated list. Nothing reads a repository catalog.
func (e *Evaluator) ReadableRepositories(r Request, s Snapshot) Decision {
	if e == nil || e.cat == nil || isNilCatalog(e.cat) {
		return Decision{Reason: ReasonInvalidRequest, CacheClass: CacheNone}
	}
	rev := e.cat.Version()
	deny := func(why Reason) Decision { return Decision{Reason: why, PolicyRevision: rev, CacheClass: CacheNone} }
	if s == nil || r.ResourceKind != resourceRepository || r.ResourceKey != wildcardKey {
		return deny(ReasonInvalidRequest)
	}
	r, ok := NormalizeRequest(r)
	if !ok {
		return deny(ReasonInvalidRequest)
	}
	act, err := e.cat.Action(r.Action)
	if err != nil {
		return deny(ReasonUnknownAction)
	}
	if act.ResourceKind != resourceRepository {
		return deny(ReasonResourceMismatch)
	}
	return e.Evaluate(Request{Action: ListingDecisionAction, ResourceKind: resourceOrganization, OrganizationID: r.OrganizationID}, s)
}
