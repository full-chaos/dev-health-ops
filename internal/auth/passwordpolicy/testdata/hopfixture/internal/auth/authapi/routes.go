// Package authapi is the fixture's auth-service side of the internal routes.
package authapi

import (
	"github.com/full-chaos/dev-health-ops/internal/api/authprovision"
	"github.com/full-chaos/dev-health-ops/internal/auth/identity"
)

// patternUsersCreate is a route literal inside internal/auth (allowed); Routes
// also names the go-api constant, which does not make it a hop caller.
const patternUsersCreate = "/internal/v1/users/create"

type handlers struct{ x identity.Exec }

func (h handlers) usersCreate(hash string) { identity.InsertUser(h.x, hash) }

func (h handlers) usersPassword(hash string) { identity.SetPassword(h.x, hash) }

func Routes(h handlers) map[string]func(string) {
	return map[string]func(string){patternUsersCreate: h.usersCreate, authprovision.PathUsersPassword: h.usersPassword}
}
