package authorization

import (
	"sort"

	"github.com/full-chaos/dev-health-ops/internal/auth/policycatalog"
)

// repositoryScope is the repositories a membership of role may read through act, from the grant rows of its
// (organization, principal) pair. It is the same rule evaluateRepository applies to one key, stated for the whole
// set: a pair with no row of any state reads every repository when its membership role holds the action (the
// membership default); a pair with rows reads the keys of its live grants whose role holds the action, and a live
// wildcard grant reads every repository.
//
// unrestricted is true when every repository is readable: the decision then carries repository_scope organization
// and no allowlist, because the decision point cannot list the repositories of an organization and no wildcard is
// ever emitted. Otherwise keys
// is the sorted, unique list, empty for none. why is ReasonRepositoryAllowlistTooLarge when the list would pass
// MaxRepositoryAllowlist: it is never truncated.
func (e *Evaluator) repositoryScope(s Snapshot, orgID, role string, act policycatalog.Action, rows []Grant, now int64) (keys []string, unrestricted bool, why Reason) {
	return e.scopeOf(s, orgID, role, act, rows, now, false)
}

// liveRepositoryScope is repositoryScope for the read listing (CHAOS-13618): a set over MaxRepositoryAllowlist is
// counted again with only the keys that are the live path of a repository identity row (livePath). 256 or fewer of
// them is the list; still more is ReasonRepositoryAllowlistTooLarge. A set within the bound is repositoryScope's.
func (e *Evaluator) liveRepositoryScope(s Snapshot, orgID, role string, act policycatalog.Action, rows []Grant, now int64) (keys []string, unrestricted bool, why Reason) {
	return e.scopeOf(s, orgID, role, act, rows, now, true)
}

func (e *Evaluator) scopeOf(s Snapshot, orgID, role string, act policycatalog.Action, rows []Grant, now int64, recount bool) (keys []string, unrestricted bool, why Reason) {
	if len(rows) == 0 {
		return nil, e.membershipDefaultDenial(role, act) == "", ""
	}
	seen := map[string]bool{}
	for _, g := range rows {
		if g.ResourceKind != resourceRepository || !e.roleHolds(g.Role, act) || g.Revoked ||
			(g.ExpiresAtUnixNano != 0 && now >= g.ExpiresAtUnixNano) {
			continue
		}
		if g.ResourceKey == wildcardKey {
			return nil, true, ""
		}
		if g.RepoID == "" {
			seen[g.ResourceKey] = true
			continue
		}
		// An id-bearing grant lists the live path of its id, never its stored path: after a rename the stored path
		// is stale, and a new repository may hold it. An id with no single live path lists nothing.
		if path, ok := idPath(s, orgID, g.RepoID); ok {
			seen[path] = true
		}
	}
	if recount && len(seen) > MaxRepositoryAllowlist {
		for key := range seen {
			if !livePath(s, orgID, key) {
				delete(seen, key)
			}
		}
	}
	if len(seen) > MaxRepositoryAllowlist {
		return nil, false, ReasonRepositoryAllowlistTooLarge
	}
	keys = make([]string, 0, len(seen))
	for k := range seen {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys, false, ""
}

// organizationAllow is the allow of an organization-scope action: its obligations are the tenant scope, the
// repositories the principal may read when the action reads repository-bound data, and the redaction rule of the
// membership role. A snapshot that cannot report grants cannot narrow a filtered action: it denies grant_absent,
// as it does for a repository action.
func (e *Evaluator) organizationAllow(r Request, act policycatalog.Action, s Snapshot, role string, rev int) Decision {
	deny := func(why Reason) Decision { return Decision{Reason: why, PolicyRevision: rev, CacheClass: CacheNone} }
	o := &Obligations{TenantScope: r.OrganizationID, Redaction: e.redactionFor(role, act.ID)}
	if act.RepositoryScope == policycatalog.RepositoryScopeFiltered {
		gs, ok := s.(GrantSnapshot)
		if !ok {
			return deny(ReasonGrantAbsent)
		}
		now := gs.NowUnixNano()
		if now <= 0 {
			return deny(ReasonClockUnavailable)
		}
		all, err := gs.Grants(r.OrganizationID)
		if err != nil {
			return deny(ReasonGrantReadFailed)
		}
		var rows []Grant
		for _, g := range all {
			if g.OrgID == r.OrganizationID {
				rows = append(rows, g)
			}
		}
		keys, unrestricted, why := e.repositoryScope(s, r.OrganizationID, role, act, rows, now)
		if why != "" {
			return deny(why)
		}
		if unrestricted {
			o.RepositoryScope = RepositoryScopeOrganization
		} else {
			o.RepositoryRestricted, o.RepositoryAllowlist = true, keys
		}
	}
	return Decision{Allowed: true, Reason: ReasonAllowedByRole, PolicyRevision: rev, Obligations: o}
}

// withRepositoryObligations gives a repository allow its obligations: the tenant scope, the redaction rule of the
// membership role (empty for a service account), and the repositories it covers. An allow that is org-wide in fact
// (repositoryScope's unrestricted: the membership default with no grant row, or a live wildcard grant) carries
// repository_scope organization and no allowlist; any other allow carries the one repository key of the request.
func (e *Evaluator) withRepositoryObligations(d Decision, r Request, role string, unrestricted bool) Decision {
	o := &Obligations{TenantScope: r.OrganizationID, Redaction: e.redactionFor(role, r.Action)}
	if unrestricted {
		o.RepositoryScope = RepositoryScopeOrganization
	} else {
		o.RepositoryRestricted, o.RepositoryAllowlist = true, []string{r.ResourceKey}
	}
	d.Obligations = o
	return d
}

func (e *Evaluator) redactionFor(role, action string) []string {
	if role == "" {
		return nil
	}
	return append([]string(nil), e.redaction[redactionKey{role, action}]...)
}
