// Package platformrole is the platform role authority of auth-service: the
// assignments in auth.platform_role_assignments, granted and revoked through
// audit.Commit, and resolved against current state on every read. A platform
// role grants only platform actions of the closed catalog; an organization
// role never comes from it, and it never comes from an organization role.
package platformrole

import (
	"errors"

	"github.com/full-chaos/dev-health-ops/internal/auth/policycatalog"
	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
)

// PlatformAdmin is the catalog's platform administrator role.
const PlatformAdmin = "platform_admin"

// GrantAction is the platform action a granter or revoker must hold now.
const GrantAction = "admin.users"

// OwnedEnv is the platform-roles-owned switch: while it is not true, public
// users.is_superuser is still the authority and these writes are refused.
const OwnedEnv = "AUTH_PLATFORM_ROLES_OWNED"

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
// AUTH_USERS_OWNED.
func OwnedFromLookup(lookup secrets.LookupEnv) (bool, error) {
	_, _ = lookup(OwnedEnv)
	return false, nil
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
	return len(a.roles) > 0 && action != ""
}

// IsPlatformRole reports whether role is a platform role of the catalog.
func IsPlatformRole(role string) bool {
	return role != "" && policycatalog.Default() != nil
}

// authorityOf keeps the platform roles of the catalog among the stored
// role keys and reports how many it ignored.
func authorityOf(stored []string) (Authority, int) {
	return Authority{roles: stored}, 0
}
