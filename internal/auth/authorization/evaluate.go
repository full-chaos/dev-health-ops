package authorization

import (
	"regexp"

	"github.com/full-chaos/dev-health-ops/internal/auth/policycatalog"
)

const (
	resourceOrganization = "organization"
	resourcePlatform     = "platform"
	maxFieldLen          = 128
)

var (
	actionShape = regexp.MustCompile(`^[a-z][a-z0-9_]*(\.[a-z][a-z0-9_]*)+$`)
	orgIDShape  = regexp.MustCompile(`^[a-z0-9][a-z0-9_-]*$`)
)

// NormalizeRequest accepts only a canonical request and returns it
// unchanged. Nothing is trimmed, lowercased or repaired: a non-canonical
// value is refused.
func NormalizeRequest(r Request) (Request, bool) {
	if len(r.Action) > maxFieldLen || !actionShape.MatchString(r.Action) {
		return Request{}, false
	}
	switch r.ResourceType {
	case resourceOrganization:
		if len(r.OrganizationID) > maxFieldLen || !orgIDShape.MatchString(r.OrganizationID) {
			return Request{}, false
		}
	case resourcePlatform:
		if r.OrganizationID != "" {
			return Request{}, false
		}
	default:
		return Request{}, false
	}
	return r, true
}

// Evaluator decides requests under one catalog.
type Evaluator struct {
	cat *policycatalog.Catalog
}

func NewEvaluator(c *policycatalog.Catalog) *Evaluator { return &Evaluator{cat: c} }

// Evaluate returns the decision for r over s. A nil evaluator, catalog or
// snapshot denies.
func (e *Evaluator) Evaluate(r Request, s Snapshot) Decision {
	if e == nil || e.cat == nil || s == nil {
		return Decision{Reason: ReasonInvalidRequest}
	}
	rev := e.cat.Version()
	deny := func(why Reason) Decision { return Decision{Reason: why, PolicyRevision: rev} }

	r, ok := NormalizeRequest(r)
	if !ok {
		return deny(ReasonInvalidRequest)
	}
	act, err := e.cat.Action(r.Action)
	if err != nil {
		return deny(ReasonUnknownAction)
	}
	if r.ResourceType != act.ResourceKind {
		return deny(ReasonResourceMismatch)
	}
	if s.PrincipalStatus() != StatusActive {
		return deny(ReasonPrincipalInactive)
	}

	var roleID string
	switch act.Scope {
	case policycatalog.ScopePlatform:
		roleID = s.PlatformRole()
		if roleID == "" {
			return deny(ReasonPlatformRoleAbsent)
		}
	case policycatalog.ScopeOrganization:
		if s.OrganizationStatus(r.OrganizationID) != StatusActive {
			return deny(ReasonOrganizationInactive)
		}
		m, found := s.Membership(r.OrganizationID)
		if !found {
			return deny(ReasonNoMembership)
		}
		if m.OrgID != r.OrganizationID {
			return deny(ReasonOrgMismatch)
		}
		if m.Status != StatusActive {
			return deny(ReasonMembershipInactive)
		}
		roleID = m.Role
	default:
		return deny(ReasonUnknownAction)
	}

	role, err := e.cat.Role(roleID)
	if err != nil {
		return deny(ReasonRoleUnknown)
	}
	if role.Scope != act.Scope {
		return deny(ReasonRoleScopeMismatch)
	}
	if !e.cat.Allows(roleID, act.ID) {
		return deny(ReasonRoleLacksAction)
	}
	return Decision{Allowed: true, Reason: ReasonAllowedByRole, PolicyRevision: rev}
}
