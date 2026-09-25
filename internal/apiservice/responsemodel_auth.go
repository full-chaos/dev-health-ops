package apiservice

// Response model routes of the auth route family (CHAOS-6722): moved from
// the shared table in responsemodel.go so a PR that adds a route here no
// longer conflicts with every other route PR. See responsemodel_register.go.
func init() {
	registerResponseModelRoutes(map[string]bool{
		"GET /api/v1/auth/me":                                      true,
		"GET /api/v1/auth/me/organizations":                        true,
		"POST /api/v1/auth/login":                                  true,
		"POST /api/v1/auth/logout":                                 true,
		"POST /api/v1/auth/refresh":                                true,
		"POST /api/v1/auth/social-login":                           true,
		"POST /api/v1/auth/switch-org":                             true,
		"POST /api/v1/auth/validate":                               true,
		"POST /api/v1/auth/register":                               true,
		"GET /api/v1/auth/verify":                                  true,
		"POST /api/v1/auth/resend-verification":                    true,
		"POST /api/v1/auth/forgot-password":                        true,
		"POST /api/v1/auth/reset-password":                         true,
		"POST /api/v1/auth/accept-invite":                          true,
		"POST /api/v1/auth/onboard":                                true,
		"GET /api/v1/auth/onboarding/state":                        true,
		"POST /api/v1/auth/onboarding/skip-integration":            true,
		"GET /api/v1/auth/sso/providers":                           true,
		"POST /api/v1/auth/sso/providers":                          true,
		"GET /api/v1/auth/sso/providers/{provider_id}":             true,
		"PATCH /api/v1/auth/sso/providers/{provider_id}":           true,
		"DELETE /api/v1/auth/sso/providers/{provider_id}":          false,
		"POST /api/v1/auth/sso/providers/{provider_id}/activate":   true,
		"POST /api/v1/auth/sso/providers/{provider_id}/deactivate": true,
		"GET /api/v1/auth/saml/{provider_id}/metadata":             true,
		"POST /api/v1/auth/saml/{provider_id}/initiate":            true,
		"POST /api/v1/auth/saml/{provider_id}/acs":                 true,
		"POST /api/v1/auth/oidc/{provider_id}/authorize":           true,
		"POST /api/v1/auth/oidc/{provider_id}/callback":            true,
		"POST /api/v1/auth/oauth/providers":                        true,
		"PATCH /api/v1/auth/oauth/providers/{provider_id}":         true,
		"POST /api/v1/auth/oauth/{provider_id}/authorize":          true,
		"POST /api/v1/auth/oauth/{provider_id}/callback":           true,
		"GET /api/v1/auth/oauth/{provider_type}/authorize":         true,
	})
}
