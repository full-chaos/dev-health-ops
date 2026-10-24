package session

import (
	"net/http"

	"github.com/full-chaos/dev-health-ops/internal/api/usersowned"
)

// refuseWhileUsersOwned answers the moved-operation refusal and reports true
// while auth-service owns users; a writer calls it before its first query.
func (d Deps) refuseWhileUsersOwned(w http.ResponseWriter) bool {
	if !usersowned.On() {
		return false
	}
	usersowned.Write(w)
	return true
}

// refuseWhileUsersOwnedErr is the guard of a writer that returns an error.
func (d Deps) refuseWhileUsersOwnedErr() error {
	return usersowned.RefuseWhileUsersOwned(usersowned.On())
}
