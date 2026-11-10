package authorization

import (
	"regexp"

	"github.com/full-chaos/dev-health-ops/internal/auth/policycatalog"
	"github.com/full-chaos/dev-health-ops/internal/auth/repokey"
)

const (
	resourceOrganization = "organization"
	resourcePlatform     = "platform"
	resourceRepository   = "repository"
	wildcardKey          = "*"
	platformAdminAction  = "admin.orgs"
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
	switch r.ResourceKind {
	case resourceOrganization:
		if r.ResourceKey != "" || len(r.OrganizationID) > maxFieldLen || !orgIDShape.MatchString(r.OrganizationID) {
			return Request{}, false
		}
	case resourceRepository:
		if !repokey.Valid(r.ResourceKey) || len(r.OrganizationID) > maxFieldLen || !orgIDShape.MatchString(r.OrganizationID) {
			return Request{}, false
		}
	case resourcePlatform:
		if r.OrganizationID != "" || r.ResourceKey != "" {
			return Request{}, false
		}
	default:
		return Request{}, false
	}
	return r, true
}

// Evaluator decides requests under one catalog.
type Evaluator struct {
	cat catalogView
	// redaction maps role and action to the field classes an allow of the pair must have removed
	// (vocabulary/redaction-classes.v1.json rules).
	redaction map[redactionKey][]string
}

type redactionKey struct{ role, action string }

// catalogView is the read side of a catalog the evaluator needs; the
// embedded *policycatalog.Catalog is the only one outside tests.
type catalogView interface {
	Version() int
	Action(id string) (policycatalog.Action, error)
	Role(id string) (policycatalog.Role, error)
	Allows(role, action string) bool
}

// NewEvaluator decides under the embedded catalog only; a caller cannot supply one.
func NewEvaluator() *Evaluator {
	e := newEvaluator(policycatalog.Default())
	e.redaction = embeddedRedactionRules(e.cat)
	return e
}

func newEvaluator(c catalogView) *Evaluator { return &Evaluator{cat: c} }

func (e *Evaluator) evaluateBase(r Request, s Snapshot) Decision {
	if e == nil || e.cat == nil || isNilCatalog(e.cat) {
		return Decision{Reason: ReasonInvalidRequest, CacheClass: CacheNone}
	}
	rev := e.cat.Version()
	deny := func(why Reason) Decision { return Decision{Reason: why, PolicyRevision: rev, CacheClass: CacheNone} }
	if s == nil {
		return deny(ReasonInvalidRequest)
	}

	r, ok := NormalizeRequest(r)
	if !ok {
		return deny(ReasonInvalidRequest)
	}
	// A repository decision names one repository; the wildcard key stays a valid request shape (the wire
	// accepts it) and is refused here, so no allowlist ever holds it.
	if r.ResourceKind == resourceRepository && r.ResourceKey == wildcardKey {
		return deny(ReasonInvalidRequest)
	}
	act, err := e.cat.Action(r.Action)
	if err != nil {
		return deny(ReasonUnknownAction)
	}
	if r.ResourceKind != act.ResourceKind {
		return deny(ReasonResourceMismatch)
	}
	if s.PrincipalStatus() != StatusActive {
		return deny(ReasonPrincipalInactive)
	}
	if gs, ok := s.(GrantSnapshot); ok {
		if why := workloadDenial(gs, r.Action); why != "" {
			return deny(why)
		}
	}
	if act.ResourceKind == resourceRepository {
		return e.evaluateRepository(r, act, s, rev)
	}

	var roleID string
	switch act.Scope {
	case policycatalog.ScopePlatform:
		roleID = s.PlatformRole()
		if roleID == "" {
			return deny(ReasonPlatformRoleAbsent)
		}
	case policycatalog.ScopeOrganization:
		// The legacy paths admit a holder of admin.orgs before they read the membership row, in an active,
		// suspended or unknown organization (no organization row); a removed organization still denies.
		orgStatus := s.OrganizationStatus(r.OrganizationID)
		if (orgStatus == StatusActive || orgStatus == StatusSuspended || orgStatus == StatusUnknown) && act.PlatformAdmin &&
			e.cat.Allows(s.PlatformRole(), platformAdminAction) {
			o := &Obligations{TenantScope: r.OrganizationID}
			// A filtered action's allow names the repositories it covers; a platform admin reads them org-wide.
			if act.RepositoryScope == policycatalog.RepositoryScopeFiltered {
				o.RepositoryScope = RepositoryScopeOrganization
			}
			return Decision{Allowed: true, Reason: ReasonAllowedByPlatformAdmin, PolicyRevision: rev, Obligations: o}
		}
		if orgStatus != StatusActive {
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
			return deny(ReasonNoMembership)
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
	if act.Scope == policycatalog.ScopePlatform {
		return Decision{Allowed: true, Reason: ReasonAllowedByRole, PolicyRevision: rev}
	}
	return e.organizationAllow(r, act, s, roleID, rev)
}
