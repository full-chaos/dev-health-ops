package apiservice

// Response model routes of the admin identities route family (CHAOS-6722): moved from
// the shared table in responsemodel.go so a PR that adds a route here no
// longer conflicts with every other route PR. See responsemodel_register.go.
func init() {
	registerResponseModelRoutes(map[string]bool{
		"GET /api/v1/admin/identities":  true,
		"POST /api/v1/admin/identities": true,
	})
}
