package authorization

import "github.com/full-chaos/dev-health-ops/internal/auth/policycatalog"

// ReadableRepositories answers which repositories s may read in one organization through a repository action
// (acr.context.read, acr.evidence.read): the whole-set form of the decision evaluateRepository makes for one key. r is
// the listing question: ResourceKind repository, ResourceKey "*" and the organization; any other shape is
// invalid_request.
//
// The standing checks are the per-key decision's (repositoryStanding: an active organization, an active membership or
// service-account row, a clock, a grant read), and the set is repositoryScope's, the rule credential issue lists its
// repository_allowlist with: an allow carries repository_scope organization when every repository is readable (the
// membership default with no grant row, or a live wildcard grant), else the sorted, unique, canonical keys of the live
// grants (an id-bearing grant lists the live path of its id; [] is none). More than MaxRepositoryAllowlist keys deny
// repository_allowlist_too_large; a list is never truncated. Nothing reads a repository catalog: an org-wide answer is
// the marker, never a list.
//
// A delegated session follows the decision point's delegation rules (delegationDenial, delegatedAllow). Its allow
// covers the subject's own keys with the session's assurance level; an org-wide answer is denied
// impersonation_denied, because the org-wide marker never reaches a delegated session and a delegated "*" is refused.
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
	if ds, ok := s.(DelegationSnapshot); ok {
		if del, delegated := ds.Delegation(); delegated {
			if why := delegationDenial(r, del); why != "" {
				return deny(why)
			}
			d := e.readableBase(r, act, s, rev)
			if !d.Allowed {
				return d
			}
			if d.Obligations.RepositoryScope != "" {
				return deny(ReasonImpersonationDenied)
			}
			return e.delegatedAllow(r, del, d, rev)
		}
	}
	return e.readableBase(r, act, s, rev)
}

// readableBase is the listing of the effective subject, with the per-key decision's standing and repositoryScope's set.
func (e *Evaluator) readableBase(r Request, act policycatalog.Action, s Snapshot, rev int) Decision {
	deny := func(why Reason) Decision { return Decision{Reason: why, PolicyRevision: rev, CacheClass: CacheNone} }
	if s.PrincipalStatus() != StatusActive {
		return deny(ReasonPrincipalInactive)
	}
	gs, ok := s.(GrantSnapshot)
	if !ok {
		return deny(ReasonGrantAbsent)
	}
	if why := workloadDenial(gs, r.Action); why != "" {
		return deny(why)
	}
	memberRole, isSA, rows, now, why := repositoryStanding(r, gs)
	if why != "" {
		return deny(why)
	}
	reason := ReasonAllowedByRepositoryGrant
	if len(rows) == 0 {
		if isSA {
			return deny(ReasonGrantAbsent)
		}
		if why := e.membershipDefaultDenial(memberRole, act); why != "" {
			return deny(why)
		}
		reason = ReasonAllowedByMembershipDefault
	}
	keys, unrestricted, why := e.repositoryScope(s, r.OrganizationID, memberRole, act, rows, now)
	if why != "" {
		return deny(why)
	}
	d := repositoryAllow(reason, act, rev, now)
	o := &Obligations{TenantScope: r.OrganizationID, Redaction: e.redactionFor(memberRole, act.ID)}
	if unrestricted {
		o.RepositoryScope = RepositoryScopeOrganization
	} else {
		o.RepositoryRestricted, o.RepositoryAllowlist = true, keys
	}
	d.Obligations = o
	return d
}
