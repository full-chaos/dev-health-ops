package apiservice

import (
	"github.com/full-chaos/dev-health-ops/internal/auth/platformrole/rule"
	"github.com/full-chaos/dev-health-ops/internal/storage/postgres"
)

// apiPostureFor is the manifest go-api's readiness check holds the api role
// to: the same two switches the River migrate step reads. A malformed
// AUTH_PLATFORM_ROLES_OWNED is a configuration error, not a guess.
func apiPostureFor(proxy AuthProxyConfig, lookup func(string) (string, bool)) (postgres.RolePosture, error) {
	platformRolesOwned, err := rule.OwnedFromLookup(lookup)
	if err != nil {
		return postgres.RolePosture{}, err
	}
	return postgres.APIPostureFor(proxy.Enabled(AuthSessions), platformRolesOwned), nil
}
