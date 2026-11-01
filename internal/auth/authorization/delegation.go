package authorization

import (
	"strings"

	"github.com/full-chaos/dev-health-ops/internal/auth/policycatalog"
)

const platformOwnerRole = "platform_owner"

// Hop is one delegation of an actor chain.
type Hop struct {
	ActorPrincipalID string
	// PermittedActions caps the delegated session; empty permits nothing.
	PermittedActions []string
}

// Delegation is the actor chain of a delegated session. The Snapshot methods
// stay the effective subject; the real actor appears only here.
type Delegation struct {
	Chain []Hop
	// ActorPlatformRole is the real actor's platform role from a live read,
	// never from a token claim.
	ActorPlatformRole    string
	TargetOrganizationID string
	// Assurance is aal1, aal2 or aal3 (principal.v1).
	Assurance string
}

// DelegationSnapshot is a Snapshot of a possibly delegated session. A
// snapshot that does not implement it is not delegated.
type DelegationSnapshot interface {
	Snapshot
	Delegation() (Delegation, bool)
}

var assuranceLevels = map[string]bool{"aal1": true, "aal2": true, "aal3": true}

var floorExact = map[string]bool{
	"admin.impersonate": true,
	"members.invite":    true,
	"members.manage":    true,
	"billing.manage":    true,
	"org.delete":        true,
}

// impersonationFloor is the approved deny list under impersonation. It holds
// whatever the catalog flag says; the flag denies by default as well.
func impersonationFloor(action string) bool {
	if floorExact[action] {
		return true
	}
	ns, _, ok := strings.Cut(action, ".")
	if !ok {
		return false
	}
	switch ns {
	case "credential", "credentials", "token", "tokens":
		switch action[strings.LastIndex(action, ".")+1:] {
		case "create", "issue", "rotate", "revoke":
			return true
		}
	}
	return false
}

func hasAction(list []string, v string) bool {
	for _, x := range list {
		if x == v {
			return true
		}
	}
	return false
}

// Evaluate returns the decision for r over s. A nil evaluator, catalog or
// snapshot denies; a denial made under a catalog carries its version.
func (e *Evaluator) Evaluate(r Request, s Snapshot) Decision {
	ds, ok := s.(DelegationSnapshot)
	if !ok || e == nil || e.cat == nil || isNilCatalog(e.cat) {
		return e.evaluateBase(r, s)
	}
	del, delegated := ds.Delegation()
	if !delegated {
		return e.evaluateBase(r, s)
	}
	rev := e.cat.Version()
	deny := func(why Reason) Decision { return Decision{Reason: why, PolicyRevision: rev} }

	switch {
	case len(del.Chain) == 0:
		return deny(ReasonDelegationInvalid)
	case len(del.Chain) > 1:
		return deny(ReasonImpersonationDenied)
	case del.Chain[0].ActorPrincipalID == "", del.TargetOrganizationID == "", !assuranceLevels[del.Assurance]:
		return deny(ReasonDelegationInvalid)
	case r.ResourceKind != resourcePlatform && r.OrganizationID != del.TargetOrganizationID:
		return deny(ReasonDelegationInvalid)
	}

	d := e.evaluateBase(r, s)
	if !d.Allowed {
		return d
	}
	act, err := e.cat.Action(r.Action)
	if err != nil || act.Scope == policycatalog.ScopePlatform ||
		!hasAction(del.Chain[0].PermittedActions, r.Action) {
		return deny(ReasonImpersonationDenied)
	}
	if impersonationFloor(r.Action) || act.Impersonation != policycatalog.ImpersonationAllowed {
		if del.ActorPlatformRole != platformOwnerRole {
			return deny(ReasonImpersonationDenied)
		}
		d.Audit = &DelegationAudit{
			ActorPrincipalID: del.Chain[0].ActorPrincipalID, OrganizationID: r.OrganizationID,
			Action: r.Action, Reason: AuditReasonOwnerExempt,
		}
	}
	d.Obligations = &Obligations{TenantScope: r.OrganizationID, MinimumAssurance: del.Assurance}
	if r.ResourceKind == resourceRepository {
		d.Obligations.RepositoryAllowlist = []string{r.ResourceKey}
	}
	return d
}
