package admin

import (
	"net/http"

	"github.com/full-chaos/dev-health-ops/internal/api/usersowned"
)

// writeUsersOwned answers a refused users write: 409, the moved operation.
func writeUsersOwned(w http.ResponseWriter) { usersowned.Write(w) }

// writePlatformRolesOwned answers a refused is_superuser write: 409, the moved operation.
func writePlatformRolesOwned(w http.ResponseWriter) { usersowned.WritePlatformRoles(w) }
