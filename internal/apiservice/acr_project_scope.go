package apiservice

import (
	"fmt"
	"strconv"
	"strings"
)

// EnvACRAccessProjectScope turns on project_scope in go-api's ACR principal
// access read (CHAOS-12224): for an organization with no repository
// inventory, the read also answers the org's project inventory
// (internal/projectinventory, (provider, id) pairs) as project_scope. It
// defaults OFF, and with it off the access read is byte-identical to the read
// before project_scope existed.
//
// DO NOT turn it on before acr accepts project_scope everywhere. acr's
// decoder refuses unknown fields (DisallowUnknownFields, acr
// internal/entitlements/principal_access.go decodePrincipalAccess), so an acr
// without the project-only scope (CHAOS-12107, gates CHAOS-12228) answers 503
// to EVERY user-class call once go-api sends project_scope. Order (design
// CHAOS-12107 D1): acr accepts project_scope -> that acr is live everywhere ->
// only then this flag.
//
// The flag also changes go-api's ClickHouse posture: with it on, the login
// must hold SELECT on projects (chclickhouse.ProjectScopeGrant), and the
// readiness check is exact both ways. So the users.d line (and, on ClickHouse
// Cloud, the GRANT) moves with this flag. On bigboy, the users.d render and
// both checks follow the flag read from go-api's env file (cigate bigboy
// go-api-project-scope, render-ch-users --project-scope; CHAOS-12268), and a
// bigboy reader test pins that the two readings of the value agree. In prod,
// the GRANT and the values.prod users.d line are the roll step of the flip.
const EnvACRAccessProjectScope = "ACR_ACCESS_PROJECT_SCOPE"

// ParseACRAccessProjectScope reads EnvACRAccessProjectScope: unset or empty
// is off, "true"/"false" (strconv.ParseBool) is the value, anything else is a
// start-up error.
func ParseACRAccessProjectScope(lookup func(string) (string, bool)) (bool, error) {
	raw, ok := lookup(EnvACRAccessProjectScope)
	if !ok || strings.TrimSpace(raw) == "" {
		return false, nil
	}
	on, err := strconv.ParseBool(strings.TrimSpace(raw))
	if err != nil {
		return false, fmt.Errorf("%s must be true or false", EnvACRAccessProjectScope)
	}
	return on, nil
}
