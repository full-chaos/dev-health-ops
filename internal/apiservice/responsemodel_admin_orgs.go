package apiservice

// Response model routes of the admin orgs route family (CHAOS-6722): moved from
// the shared table in responsemodel.go so a PR that adds a route here no
// longer conflicts with every other route PR. See responsemodel_register.go.
func init() {
	registerResponseModelRoutes(map[string]bool{
		"DELETE /api/v1/admin/orgs/{org_id}":                                 true,
		"DELETE /api/v1/admin/orgs/{org_id}/feature-overrides/{override_id}": false,
		"DELETE /api/v1/admin/orgs/{org_id}/members/{user_id}":               true,
		"GET /api/v1/admin/orgs":                                             true,
		"GET /api/v1/admin/orgs/{org_id}":                                    true,
		"GET /api/v1/admin/orgs/{org_id}/feature-overrides":                  true,
		"GET /api/v1/admin/orgs/{org_id}/members":                            true,
		"PATCH /api/v1/admin/orgs/{org_id}":                                  true,
		"PATCH /api/v1/admin/orgs/{org_id}/feature-overrides/{override_id}":  true,
		"PATCH /api/v1/admin/orgs/{org_id}/members/{user_id}":                true,
		"POST /api/v1/admin/orgs":                                            true,
		"POST /api/v1/admin/orgs/{org_id}/feature-overrides":                 true,
		"POST /api/v1/admin/orgs/{org_id}/invites":                           true,
		"POST /api/v1/admin/orgs/{org_id}/members":                           true,
	})
}
