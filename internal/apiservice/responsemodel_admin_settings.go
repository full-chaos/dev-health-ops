package apiservice

// Response model routes of the admin settings route family (CHAOS-6722): moved from
// the shared table in responsemodel.go so a PR that adds a route here no
// longer conflicts with every other route PR. See responsemodel_register.go.
func init() {
	registerResponseModelRoutes(map[string]bool{
		"DELETE /api/v1/admin/settings/{category}/{key}": true,
		"GET /api/v1/admin/settings/categories":          true,
		"GET /api/v1/admin/settings/{category}":          true,
		"GET /api/v1/admin/settings/{category}/{key}":    true,
		"POST /api/v1/admin/settings":                    true,
		"PUT /api/v1/admin/settings/{category}/{key}":    true,
	})
}
