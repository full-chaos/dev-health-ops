package authapi

import "github.com/full-chaos/dev-health-ops/internal/auth/httpapi"

// responseModelRoutes holds the ResponseModel flag of every route the
// auth-service mounts, keyed "METHOD pattern" as go-api's table is. A route
// with a go-api twin carries the twin's value; the others carry the flag
// their own success writer calls for. Route rows stay free of the flag so
// their text does not move.
var responseModelRoutes = map[string]bool{
	"POST /api/v1/auth/login":                         true,
	"POST /api/v1/auth/refresh":                       true,
	"POST /api/v1/auth/logout":                        true,
	"POST /api/v1/auth/social-login":                  true,
	"GET /api/v1/auth/verify":                         true,
	"POST /api/v1/auth/register":                      true,
	"POST /api/v1/auth/resend-verification":           true,
	"POST /api/v1/auth/forgot-password":               true,
	"POST /api/v1/auth/reset-password":                true,
	"POST /api/v1/auth/change-password":               true,
	"POST /api/v1/auth/oidc/{provider_id}/authorize":  true,
	"POST /api/v1/auth/oidc/{provider_id}/callback":   true,
	"POST /api/v1/auth/oauth/{provider_id}/authorize": true,
	"GET /api/v1/auth/oauth/{provider_id}/authorize":  true,
	"POST /api/v1/auth/oauth/{provider_id}/callback":  true,
	"GET /api/v1/auth/saml/{provider_id}/metadata":    true,
	"POST /api/v1/auth/saml/{provider_id}/initiate":   true,
	"POST /api/v1/auth/saml/{provider_id}/acs":        true,
	"POST /internal/v1/sessions":                      false,
	"POST /internal/v1/token/exchange":                false,
	"POST /internal/v1/memberships/add":               false,
	"POST /internal/v1/memberships/role":              false,
	"POST /internal/v1/memberships/remove":            false,
	"POST /internal/v1/memberships/transfer":          false,
	"POST /internal/v1/invites/create":                false,
	"POST /internal/v1/invites/accept":                false,
	"POST /internal/v1/invites/revoke":                false,
	"POST /internal/v1/organizations/suspend":         false,
	"POST /internal/v1/service-accounts/create":       false,
	"POST /internal/v1/service-accounts/disable":      false,
	"POST /internal/v1/service-accounts/enable":       false,
	"POST /internal/v1/service-accounts/owner":        false,
	"POST /internal/v1/service-accounts/keys/issue":   false,
	"POST /internal/v1/service-accounts/keys/rotate":  false,
	"POST /internal/v1/service-accounts/keys/revoke":  false,
	"POST /internal/v1/service-accounts/keys/list":    false,
	"POST /internal/v1/organizations/deprovision":     false,
	"POST /internal/v1/organizations/reactivate":      false,
	"POST /internal/v1/organizations/state":           false,
	"POST /internal/v1/organizations/onboard":         false,
	"POST /internal/v1/organizations/create":          false,
	"POST /internal/v1/sessions/state":                false,
	"POST /internal/v1/sessions/delegated":            false,
	"POST /internal/v1/sessions/delegated/end":        false,
	"POST /v1/token/refresh":                          false,
	"GET /v1/sessions/current":                        false,
	"POST /v1/sessions/revoke":                        false,
	"POST /v1/sessions/revoke-all":                    false,
	"POST /v1/identities/social":                      false,
	"DELETE /v1/identities/social/{id}":               false,
	"GET /.well-known/jwks.json":                      false,
	"HEAD /.well-known/jwks.json":                     false,
	"GET /.well-known/openid-configuration":           false,
	"HEAD /.well-known/openid-configuration":          false,
}

// ResponseModelRoutes returns a copy of the flag table.
func ResponseModelRoutes() map[string]bool {
	out := make(map[string]bool, len(responseModelRoutes))
	for key, model := range responseModelRoutes {
		out[key] = model
	}
	return out
}

// MarkResponseModels sets each route's ResponseModel from the table; a route
// the table does not name keeps the flag it has.
func MarkResponseModels(routes []httpapi.Route) []httpapi.Route {
	marked := make([]httpapi.Route, len(routes))
	copy(marked, routes)
	for index := range marked {
		if model, named := responseModelRoutes[marked[index].Method+" "+marked[index].Pattern]; named {
			marked[index].ResponseModel = model
		}
	}
	return marked
}
