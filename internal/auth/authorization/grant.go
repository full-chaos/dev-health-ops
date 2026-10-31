package authorization

import "github.com/full-chaos/dev-health-ops/internal/auth/policycatalog"

// Grant is one resource_grants row as read from state. Grants are
// allow-only: the table has no deny effect. A revoked row stays as history.
type Grant struct {
	OrgID        string
	Role         string
	ResourceKind string
	ResourceKey  string
	// ExpiresAtUnixNano is the expiry in Unix nanoseconds; 0 is no expiry.
	// A grant is expired at its expiry instant and after it.
	ExpiresAtUnixNano int64
	Revoked           bool
}

// ServiceAccount is a service-account principal's row in one organization.
type ServiceAccount struct {
	OrgID  string
	Status Status
}

// WorkloadLimits narrows a workload principal. Audience is the audience the
// verified workload token carries, read from the verified principal
// reference; a request never supplies it. Audiences and AllowedActions are
// the registry row: the audiences a token for the workload may carry and
// the actions it may be granted. Neither list has a wildcard.
type WorkloadLimits struct {
	Audience       string
	Audiences      []string
	AllowedActions []string
}

// GrantSnapshot is a Snapshot that also holds repository grants, the
// service-account row, workload limits and a clock. A Snapshot that is not
// a GrantSnapshot denies every repository action. The clock is the
// snapshot's: this package never reads the wall clock.
type GrantSnapshot interface {
	Snapshot
	// Grants returns the principal's grant rows; rows of another
	// organization are ignored by the evaluator whatever is asked for.
	Grants(orgID string) []Grant
	// Workload reports the limits of a workload principal; false for any
	// other principal.
	Workload() (WorkloadLimits, bool)
	// ServiceAccount reports the principal's service-account row in the
	// organization; false when the principal is not a service account there.
	ServiceAccount(orgID string) (ServiceAccount, bool)
	// NowUnixNano is the decision time in Unix nanoseconds; 0 or less means
	// no clock.
	NowUnixNano() int64
}

func isNilCatalog(c catalogView) bool {
	p, ok := c.(*policycatalog.Catalog)
	return ok && p == nil
}

func listHas(list []string, v string) bool {
	for _, x := range list {
		if x == v {
			return true
		}
	}
	return false
}

// workloadDenial narrows a workload principal: its token audience must be
// one its registration lists, and the action one it may be granted.
func workloadDenial(s GrantSnapshot, action string) Reason {
	w, ok := s.Workload()
	if !ok {
		return ""
	}
	if w.Audience == "" || !listHas(w.Audiences, w.Audience) {
		return ReasonWorkloadAudienceDenied
	}
	if !listHas(w.AllowedActions, action) {
		return ReasonWorkloadActionDenied
	}
	return ""
}

// evaluateRepository allows a repository action only for a principal with
// standing in the organization (membership, or the service-account row)
// AND a live grant of that organization whose role holds the action. The
// organization role alone never allows it.
func (e *Evaluator) evaluateRepository(r Request, act policycatalog.Action, s Snapshot, rev int) Decision {
	deny := func(why Reason) Decision { return Decision{Reason: why, PolicyRevision: rev} }
	gs, ok := s.(GrantSnapshot)
	if !ok {
		return deny(ReasonGrantAbsent)
	}
	if gs.OrganizationStatus(r.OrganizationID) != StatusActive {
		return deny(ReasonOrganizationInactive)
	}
	if sa, isSA := gs.ServiceAccount(r.OrganizationID); isSA {
		if sa.OrgID != r.OrganizationID {
			return deny(ReasonOrgMismatch)
		}
		if sa.Status != StatusActive {
			return deny(ReasonServiceAccountInactive)
		}
	} else {
		m, found := gs.Membership(r.OrganizationID)
		if !found {
			return deny(ReasonNoMembership)
		}
		if m.OrgID != r.OrganizationID {
			return deny(ReasonOrgMismatch)
		}
		if m.Status != StatusActive {
			return deny(ReasonMembershipInactive)
		}
	}
	now := gs.NowUnixNano()
	if now <= 0 {
		return deny(ReasonClockUnavailable)
	}

	var sawGrant, sawForeign, sawRevoked, sawExpired bool
	for _, g := range gs.Grants(r.OrganizationID) {
		if g.ResourceKind != resourceRepository {
			continue
		}
		keyMatches := g.ResourceKey == r.ResourceKey || g.ResourceKey == wildcardKey
		if g.OrgID != r.OrganizationID {
			sawForeign = sawForeign || keyMatches
			continue
		}
		sawGrant = true
		if !keyMatches {
			continue
		}
		if !e.roleHolds(g.Role, act) {
			continue
		}
		if g.Revoked {
			sawRevoked = true
			continue
		}
		if g.ExpiresAtUnixNano != 0 && now >= g.ExpiresAtUnixNano {
			sawExpired = true
			continue
		}
		return Decision{Allowed: true, Reason: ReasonAllowedByRepositoryGrant, PolicyRevision: rev}
	}
	switch {
	case sawExpired:
		return deny(ReasonGrantExpired)
	case sawRevoked:
		return deny(ReasonGrantRevoked)
	case sawGrant:
		return deny(ReasonRepositoryNotGranted)
	case sawForeign:
		return deny(ReasonResourceOrgMismatch)
	}
	return deny(ReasonGrantAbsent)
}

func (e *Evaluator) roleHolds(roleID string, act policycatalog.Action) bool {
	role, err := e.cat.Role(roleID)
	return err == nil && role.Scope == act.Scope && e.cat.Allows(roleID, act.ID)
}
