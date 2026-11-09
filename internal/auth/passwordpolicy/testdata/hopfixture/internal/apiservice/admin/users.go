// Package admin is the fixture's go-api handlers: each checks the policy first.
package admin

import (
	"github.com/full-chaos/dev-health-ops/internal/api/authprovision"
	"github.com/full-chaos/dev-health-ops/internal/auth/passwordpolicy"
)

func createUser(password string) {
	if len(passwordpolicy.ValidateForHash(password)) > 0 {
		return
	}
	authprovision.PostCreate(password)
}

func setPassword(password string) {
	if len(passwordpolicy.ValidateForHash(password)) > 0 {
		return
	}
	authprovision.PostPassword(password)
}
