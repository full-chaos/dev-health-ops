package apiservice

import (
	"fmt"

	"github.com/full-chaos/dev-health-ops/internal/api/usersowned"
)

// ParseUsersOwned reads AUTH_USERS_OWNED for go-api. On, the routes that write
// public.users are served by auth-service, so go-api refuses to start unless
// the sessions, local login and credentials aggregates are proxied to it.
func ParseUsersOwned(lookup func(string) (string, bool), proxy AuthProxyConfig) (bool, error) {
	on, err := usersowned.FromLookup(lookup)
	if err != nil {
		return false, err
	}
	if !on {
		return false, nil
	}
	for _, aggregate := range []AuthAggregate{AuthSessions, AuthLocalLogin, AuthCredentials} {
		if !proxy.Enabled(aggregate) {
			return false, fmt.Errorf("%s is on: %s must be on too, so the routes that write users are served by auth-service",
				usersowned.Env, AuthProxyFlagEnv(aggregate))
		}
	}
	return true, nil
}
