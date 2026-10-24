package sso

import "github.com/full-chaos/dev-health-ops/internal/api/usersowned"

// refuseWhileUsersOwned is the guard of the writers of public.users: while
// auth-service owns users, a sign-in creates no public.users row (the create
// refuses) and updates none (the profile and last-login update is skipped).
func (h handlers) refuseWhileUsersOwned() error {
	return usersowned.RefuseWhileUsersOwned(usersowned.On())
}
