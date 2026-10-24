package sso

import "github.com/full-chaos/dev-health-ops/internal/api/usersowned"

// refuseWhileUsersOwned is the guard of the account create: while auth-service
// owns users, a sign-in creates no public.users row.
func (h handlers) refuseWhileUsersOwned() error {
	return usersowned.RefuseWhileUsersOwned(usersowned.On())
}
