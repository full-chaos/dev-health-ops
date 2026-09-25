package apiservice

// Response model routes of the admin retention policies route family (CHAOS-6722): moved from
// the shared table in responsemodel.go so a PR that adds a route here no
// longer conflicts with every other route PR. See responsemodel_register.go.
func init() {
	registerResponseModelRoutes(map[string]bool{
		"DELETE /api/v1/admin/retention-policies/{policy_id}":       true,
		"GET /api/v1/admin/retention-policies":                      true,
		"GET /api/v1/admin/retention-policies/resource-types":       true,
		"GET /api/v1/admin/retention-policies/{policy_id}":          true,
		"PATCH /api/v1/admin/retention-policies/{policy_id}":        true,
		"POST /api/v1/admin/retention-policies":                     true,
		"POST /api/v1/admin/retention-policies/{policy_id}/execute": true,
	})
}
