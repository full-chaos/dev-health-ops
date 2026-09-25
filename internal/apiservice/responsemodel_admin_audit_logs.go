package apiservice

// Response model routes of the admin audit logs route family (CHAOS-6722): moved from
// the shared table in responsemodel.go so a PR that adds a route here no
// longer conflicts with every other route PR. See responsemodel_register.go.
func init() {
	registerResponseModelRoutes(map[string]bool{
		"GET /api/v1/admin/audit-logs":                                        true,
		"GET /api/v1/admin/audit-logs/resource/{resource_type}/{resource_id}": true,
		"GET /api/v1/admin/audit-logs/user/{user_id}":                         true,
		"GET /api/v1/admin/audit-logs/{log_id}":                               true,
	})
}
