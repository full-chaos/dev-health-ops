// Package platformrole is the platform role authority of auth-service: the
// assignments in auth.platform_role_assignments, granted and revoked through
// audit.Commit, and resolved against current state on every read. A platform
// role grants only platform actions of the closed catalog; an organization
// role never comes from it, and it never comes from an organization role.
package platformrole

import (
	"errors"

	"github.com/full-chaos/dev-health-ops/internal/auth/platformrole/rule"
	"github.com/full-chaos/dev-health-ops/internal/auth/policycatalog"
	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
)

// PlatformAdmin is the catalog's platform administrator role.
const PlatformAdmin = rule.PlatformAdmin

// GrantAction is the platform action a granter or revoker must hold now.
const GrantAction = "admin.users"

// OwnedEnv is the platform-roles-owned switch: while it is not true, public
// users.is_superuser is still the authority and these writes are refused.
const OwnedEnv = rule.OwnedEnv

var (
	ErrNotPlatformRole   = errors.New("platformrole: the role is not a platform role of the catalog")
	ErrGranterNotHeld    = errors.New("platformrole: the acting principal does not hold the platform action now")
	ErrPrincipalMissing  = errors.New("platformrole: the principal does not exist")
	ErrNotHumanUser      = errors.New("platformrole: a platform role is held only by a user principal")
	ErrPrincipalOff      = errors.New("platformrole: the principal is disabled")
	ErrExpiryNotFuture   = errors.New("platformrole: expires_at is not in the future")
	ErrAlreadyHeld       = errors.New("platformrole: the principal already holds the role")
	ErrNotHeld           = errors.New("platformrole: the principal does not hold the role")
	ErrLastPlatformAdmin = errors.New("platformrole: the revoke would leave no active platform administrator")
	ErrStore             = errors.New("platformrole: store failure")
)

// OwnedFromLookup reads OwnedEnv: unset, empty or false is off, true is on,
// any other value is an error. Case and surrounding space are ignored, as for
// AUTH_USERS_OWNED. It is rule.OwnedFromLookup, the one parse.
func OwnedFromLookup(lookup secrets.LookupEnv) (bool, error) {
	return rule.OwnedFromLookup(lookup)
}

// Authority is the platform authority a principal holds now. The zero value
// holds nothing.
type Authority struct {
	roles []string
}

// Roles are the held platform roles, sorted.
func (a Authority) Roles() []string { return append([]string(nil), a.roles...) }

// Any reports whether any platform role is held.
func (a Authority) Any() bool { return len(a.roles) > 0 }

// Allows reports whether a held role allows action and action is a platform
// action of the catalog. An organization action is never allowed.
func (a Authority) Allows(action string) bool {
	catalog := policycatalog.Default()
	act, err := catalog.Action(action)
	if err != nil || act.Scope != policycatalog.ScopePlatform {
		return false
	}
	for _, role := range a.roles {
		if catalog.Allows(role, action) {
			return true
		}
	}
	return false
}

// IsPlatformRole reports whether role is a platform role of the catalog.
func IsPlatformRole(role string) bool {
	r, err := policycatalog.Default().Role(role)
	return err == nil && r.Scope == policycatalog.ScopePlatform
}

// authorityOf keeps the platform roles of the catalog among the stored
// role keys and reports how many it ignored.
func authorityOf(stored []string) (Authority, int) {
	var a Authority
	ignored := 0
	for _, role := range stored {
		if IsPlatformRole(role) {
			a.roles = append(a.roles, role)
		} else {
			ignored++
		}
	}
	return a, ignored
}
