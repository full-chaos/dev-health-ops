// Package registerorg is the sign-up organization rule shared by go-api's
// register route and auth-service's: whether a sign-up creates an organization,
// its name and its slug.
package registerorg

import (
	"strings"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"
	"github.com/full-chaos/dev-health-ops/internal/pythonparity"
)

// EnvName is api/auth/config.py's AUTH_AUTO_CREATE_ORG_ENV.
const EnvName = "AUTH_AUTO_CREATE_ORG_ON_REGISTER"

// DefaultName is the organization name when the sign-up gives none.
const DefaultName = "My Organization"

// Enabled is auth_auto_create_org_on_register(): unset or an unrecognized
// value keeps the default (true).
func Enabled(raw string) bool {
	switch pythonparity.Lower(pythonparity.Strip(raw)) {
	case "0", "false", "no", "off":
		return false
	default:
		return true
	}
}

// Name is the organization name of a sign-up: the given one, else the default.
func Name(given *string) string {
	if given != nil && *given != "" {
		return *given
	}
	return DefaultName
}

// Slug is lower(name) with spaces as "-", cut to 50 code points, then "-" and
// the first 8 characters of the user id.
func Slug(name string, userID uuid.UUID) string {
	runes := pyjson.Runes(strings.ReplaceAll(pythonparity.Lower(name), " ", "-"))
	if len(runes) > 50 {
		runes = runes[:50]
	}
	return pyjson.FromRunes(runes) + "-" + userID.String()[:8]
}

// OnboardSlug is not built.
func OnboardSlug(string, uuid.UUID) string { return "" }
