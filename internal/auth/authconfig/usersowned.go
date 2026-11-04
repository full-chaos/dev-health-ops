package authconfig

import "github.com/full-chaos/dev-health-ops/internal/platform/secrets"

// UsersOwnedFromLookup reads AUTH_USERS_OWNED with the rule auth-service
// applies: unset or empty is false, anything but true or false is refused. The
// grants pass and the operator verbs use it so every process agrees on the
// switch.
func UsersOwnedFromLookup(lookup secrets.LookupEnv) (bool, error) {
	return usersOwned(lookup)
}
