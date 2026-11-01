package authorization

import (
	"math"

	"github.com/full-chaos/dev-health-ops/internal/auth/policycatalog"
)

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
	// Grants returns EVERY grant row of the (organization, principal) pair, in
	// any state (live, revoked, expired, ended), or an error. A read failure
	// must be an error: an implementation that reports zero rows on failure
	// would grant the membership default. Rows of another organization are
	// ignored by the evaluator whatever is asked for.
	Grants(orgID string) ([]Grant, error)
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

// RepositoryAllowBoundNanos is how long a repository allow stays usable, by
// either path (live grant or membership default): 30 seconds, the TRD
// section 14 ceiling for a repository read. It is owned here; no
// environment, config or snapshot value changes it.
const RepositoryAllowBoundNanos int64 = 30_000_000_000

// repositoryAllowExpiry is the instant a repository allow made at now stops
// being usable; it saturates and never wraps.
func repositoryAllowExpiry(now int64) int64 {
	if now > math.MaxInt64-RepositoryAllowBoundNanos {
		return math.MaxInt64
	}
	return now + RepositoryAllowBoundNanos
}

// membershipDefaultRole is the role an organization membership holds for
// repository actions when it has no grant row: owner reads as admin.
func membershipDefaultRole(role string) string {
	if role == "owner" {
		return "admin"
	}
	return role
}

// membershipDefaultDenial is "" when the membership role holds the action by
// default, else the reason it does not.
func (e *Evaluator) membershipDefaultDenial(role string, act policycatalog.Action) Reason {
	roleID := membershipDefaultRole(role)
	r, err := e.cat.Role(roleID)
	switch {
	case err != nil:
		return ReasonRoleUnknown
	case r.Scope != act.Scope:
		return ReasonRoleScopeMismatch
	case !e.cat.Allows(roleID, act.ID):
		return ReasonRoleLacksAction
	}
	return ""
}

// HoldsByMembershipDefault is the one statement of the membership default
// (CHAOS-10838, D6606): an active membership whose (organization, principal)
// pair has NO grant row of any state holds a repository action when its
// membership role holds it; owner reads as admin. rows are every row of the
// pair, so one row of any state, for any repository, ends the default. It
// answers false for an action that is not repository-scoped, an unknown
// action or role, and a nil evaluator. Service accounts never use it.
func (e *Evaluator) HoldsByMembershipDefault(membershipRole, action string, rows []Grant) bool {
	if e == nil || e.cat == nil || isNilCatalog(e.cat) || len(rows) != 0 {
		return false
	}
	act, err := e.cat.Action(action)
	if err != nil || act.ResourceKind != resourceRepository {
		return false
	}
	return e.membershipDefaultDenial(membershipRole, act) == ""
}

// evaluateRepository allows a repository action only for a principal with
// standing in the organization (membership, or the service-account row) and
// either a live grant of that organization whose role holds the action, or
// (a membership with no grant row at all) the membership default. A pair
// with any row follows its live grants only.
func (e *Evaluator) evaluateRepository(r Request, act policycatalog.Action, s Snapshot, rev int) Decision {
	deny := func(why Reason) Decision { return Decision{Reason: why, PolicyRevision: rev, CacheClass: CacheNone} }
	gs, ok := s.(GrantSnapshot)
	if !ok {
		return deny(ReasonGrantAbsent)
	}
	if gs.OrganizationStatus(r.OrganizationID) != StatusActive {
		return deny(ReasonOrganizationInactive)
	}
	var memberRole string
	isSA := false
	if sa, found := gs.ServiceAccount(r.OrganizationID); found {
		isSA = true
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
		memberRole = m.Role
	}
	now := gs.NowUnixNano()
	if now <= 0 {
		return deny(ReasonClockUnavailable)
	}
	all, err := gs.Grants(r.OrganizationID)
	if err != nil {
		return deny(ReasonGrantReadFailed)
	}
	rows := make([]Grant, 0, len(all))
	for _, g := range all {
		if g.OrgID == r.OrganizationID {
			rows = append(rows, g)
		}
	}

	if len(rows) == 0 {
		if isSA {
			return deny(ReasonGrantAbsent)
		}
		if why := e.membershipDefaultDenial(memberRole, act); why != "" {
			return deny(why)
		}
		return repositoryAllow(ReasonAllowedByMembershipDefault, act, rev, now)
	}

	var sawRevoked, sawExpired bool
	for _, g := range rows {
		if g.ResourceKind != resourceRepository {
			continue
		}
		if g.ResourceKey != r.ResourceKey && g.ResourceKey != wildcardKey {
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
		return repositoryAllow(ReasonAllowedByRepositoryGrant, act, rev, now)
	}
	switch {
	case sawExpired:
		return deny(ReasonGrantExpired)
	case sawRevoked:
		return deny(ReasonGrantRevoked)
	}
	return deny(ReasonRepositoryNotGranted)
}

// repositoryAllow is the one allow decision of both repository paths: a
// repository read is resource_revision_bound and ends 30 s after the clock
// (TRD section 14); an action whose catalog class is none is never cached.
func repositoryAllow(why Reason, act policycatalog.Action, rev int, now int64) Decision {
	if act.CacheClass == policycatalog.CacheNone {
		return Decision{Allowed: true, Reason: why, PolicyRevision: rev, CacheClass: CacheNone}
	}
	return Decision{Allowed: true, Reason: why, PolicyRevision: rev,
		CacheClass: CacheResourceRevisionBound, ExpiresAtUnixNano: repositoryAllowExpiry(now)}
}

func (e *Evaluator) roleHolds(roleID string, act policycatalog.Action) bool {
	role, err := e.cat.Role(roleID)
	return err == nil && role.Scope == act.Scope && e.cat.Allows(roleID, act.ID)
}
