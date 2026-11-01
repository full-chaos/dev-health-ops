// Package usersowned is go-api's side of the users-owned switch
// (AUTH_USERS_OWNED): while auth-service owns users, the go-api routes and
// verbs that write public.users refuse with one stable answer.
package usersowned

import (
	"errors"
	"fmt"
	"net/http"
	"strings"

	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"
	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
)

// Env is the switch, with the meaning auth-service gives it.
const Env = "AUTH_USERS_OWNED"

// Reason is the machine-readable answer of a refused operation.
const Reason = "operation_moved"

// Message is the human-readable answer of a refused operation.
const Message = "This operation is served by the authentication service while it owns users"

// ErrRefused is the refusal a guarded writer returns before it writes.
var ErrRefused = errors.New(Message)

// FromLookup reads the switch like auth-service: unset or empty is off,
// anything but true or false is an error.
//
// The rule is authconfig.UsersOwnedFromLookup's, kept here so go-api does not
// link auth-service's configuration; TestFromLookupAgreesWithAuthConfig pins
// the two together.
func FromLookup(lookup func(string) (string, bool)) (bool, error) {
	raw, _ := lookup(Env)
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "", "false":
		return false, nil
	case "true":
		return true, nil
	}
	return false, fmt.Errorf("%s must be true or false", Env)
}

// On reads the switch from the process environment on every call. The
// go-api session and SSO routes use it, so the route registrations keep their
// source lines (the endpoint-profile rows pin them); startup has already
// refused a malformed value (apiservice.ParseUsersOwned), and a value that
// cannot be read here counts as on.
func On() bool {
	on, err := FromLookup(secrets.ProcessLookup)
	return err != nil || on
}

// OrgsEnv is the orgs-owned switch, with the meaning auth-service gives it.
const OrgsEnv = "AUTH_ORGS_OWNED"

// OrgsOn reads OrgsEnv.
func OrgsOn() bool { return false }

// RefuseWhileUsersOwned is the guard of a writer that returns an error: nil
// while the switch is off.
func RefuseWhileUsersOwned(on bool) error {
	if on {
		return ErrRefused
	}
	return nil
}

// Detail is the body detail of a refused request.
func Detail() *pyjson.Object {
	detail := pyjson.NewObject()
	detail.Set("message", Message)
	detail.Set("reason", Reason)
	return detail
}

// Write answers a refused request: 409 with Detail.
func Write(w http.ResponseWriter) {
	policy.WriteDetail(w, http.StatusConflict, Detail(), nil)
}

// PlatformRolesMessage is the human-readable answer of a public is_superuser
// write while auth-service owns the platform roles (AUTH_PLATFORM_ROLES_OWNED).
const PlatformRolesMessage = "This operation is served by the authentication service while it owns platform roles"

// ErrPlatformRolesRefused is the refusal of a public is_superuser write while
// auth-service owns the platform roles.
var ErrPlatformRolesRefused = errors.New(PlatformRolesMessage)

// WritePlatformRoles answers a refused is_superuser write: 409 with the
// operation_moved reason.
func WritePlatformRoles(w http.ResponseWriter) {
	detail := pyjson.NewObject()
	detail.Set("message", PlatformRolesMessage)
	detail.Set("reason", Reason)
	policy.WriteDetail(w, http.StatusConflict, detail, nil)
}
