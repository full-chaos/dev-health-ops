package apiservice

// Response model routes of the acr route family (CHAOS-6722): moved from
// the shared table in responsemodel.go so a PR that adds a route here no
// longer conflicts with every other route PR. See responsemodel_register.go.
func init() {
	registerResponseModelRoutes(map[string]bool{
		"GET /api/v1/internal/acr/entitlements/{org_id}":                     true,
		"GET /api/v1/internal/acr/health":                                    true,
		"GET /api/v1/internal/acr/orgs/{org_id}/principals/{user_id}/access": true,
	})
}
