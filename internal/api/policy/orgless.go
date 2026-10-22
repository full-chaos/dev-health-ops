package policy

import "path"

// orglessRoutes are the only method and path pairs a signed-in caller with no
// organization may reach on a guarded route (D6145): onboarding, invite
// acceptance and that caller's own account routes. Every other route, and every
// other method on a listed path, needs an organization on the token.
var orglessRoutes = map[string]bool{
	"POST /api/v1/auth/onboard":                     true,
	"GET /api/v1/auth/onboarding/state":             true,
	"HEAD /api/v1/auth/onboarding/state":            true,
	"POST /api/v1/auth/onboarding/skip-integration": true,
	"POST /api/v1/auth/accept-invite":               true,
	"GET /api/v1/auth/me":                           true,
	"HEAD /api/v1/auth/me":                          true,
	"GET /api/v1/auth/me/organizations":             true,
	"HEAD /api/v1/auth/me/organizations":            true,
	"POST /api/v1/auth/switch-org":                  true,
	"POST /api/v1/auth/change-password":             true,
}

// OrglessMayReach reports whether a caller with no organization may reach the
// route; the path is cleaned first, so a dot-dot segment names the route it
// resolves to.
func OrglessMayReach(method, requestPath string) bool {
	return orglessRoutes[method+" "+path.Clean(requestPath)]
}
