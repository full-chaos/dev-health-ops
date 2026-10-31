package rule

// The platform actions of the closed catalog (contracts/auth/v1/vocabulary).
// platformrole's catalog test pins this list to the catalog's platform_admin
// role, so the two cannot drift. admin.impersonate joins with the catalog
// change that defines it (CHAOS-10987).
const (
	ActionAdminUsers     = "admin.users"
	ActionAdminOrgs      = "admin.orgs"
	ActionAdminBilling   = "admin.billing"
	ActionAdminFlags     = "admin.flags"
	ActionAdminAudit     = "admin.audit"
	ActionAdminTelemetry = "admin.telemetry"
	ActionAdminStats     = "admin.stats"
)

// PlatformActions are the platform actions of the catalog, in catalog order.
var PlatformActions = []string{
	ActionAdminUsers, ActionAdminOrgs, ActionAdminBilling, ActionAdminFlags,
	ActionAdminAudit, ActionAdminTelemetry, ActionAdminStats,
}

// Platform is the set of platform actions a principal holds now. The zero
// value holds nothing. An action that is not a platform action of the catalog
// is never held: Has fails closed on an unknown or organization action.
type Platform struct {
	held map[string]bool
}

func isPlatformAction(action string) bool {
	for _, known := range PlatformActions {
		if action == known {
			return true
		}
	}
	return false
}

// PlatformOf is the set of the named platform actions; a name that is not a
// platform action of the catalog is dropped.
func PlatformOf(actions ...string) Platform {
	held := map[string]bool{}
	for _, action := range actions {
		if isPlatformAction(action) {
			held[action] = true
		}
	}
	return Platform{held: held}
}

// PlatformOfRole is the actions the catalog gives a platform role: every
// platform action for platform_admin, nothing for any other key.
func PlatformOfRole(role string) Platform {
	if role == PlatformAdmin {
		return PlatformOf(PlatformActions...)
	}
	return Platform{}
}

// PlatformOfSuperuser is the one place the superuser flag becomes actions:
// the flag means "holds platform_admin now" (public is_superuser while the
// platform roles switch is off, the platform role rule while it is on), and
// platform_admin lists its actions in the catalog.
func PlatformOfSuperuser(superuser bool) Platform {
	if !superuser {
		return Platform{}
	}
	return PlatformOfRole(PlatformAdmin)
}

// Has reports whether the set holds action.
func (p Platform) Has(action string) bool { return p.held[action] }

// Holds is the decision every carrier of the superuser flag makes: the
// explicit set when one is carried, else the set the flag means. explicit nil
// is a carrier that has only the flag.
func Holds(explicit *Platform, superuser bool, action string) bool {
	if explicit != nil {
		return explicit.Has(action)
	}
	return PlatformOfSuperuser(superuser).Has(action)
}
