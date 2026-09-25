package apiservice

// Response model routes of the admin credentials route family (CHAOS-6722): moved from
// the shared table in responsemodel.go so a PR that adds a route here no
// longer conflicts with every other route PR. See responsemodel_register.go.
func init() {
	registerResponseModelRoutes(map[string]bool{
		"DELETE /api/v1/admin/credentials/{provider}/{name}": true,
		"GET /api/v1/admin/credentials":                      true,
		"GET /api/v1/admin/credentials/{provider}/{name}":    true,
		"PATCH /api/v1/admin/credentials/{provider}/{name}":  true,
		"POST /api/v1/admin/credentials":                     true,
		"POST /api/v1/admin/credentials/test":                true,
	})
}
