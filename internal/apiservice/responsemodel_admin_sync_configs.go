package apiservice

// Response model routes of the admin sync configs route family (CHAOS-6722): moved from
// the shared table in responsemodel.go so a PR that adds a route here no
// longer conflicts with every other route PR. See responsemodel_register.go.
func init() {
	registerResponseModelRoutes(map[string]bool{
		"DELETE /api/v1/admin/sync-configs/{config_id}":           false,
		"GET /api/v1/admin/sync-configs":                          true,
		"GET /api/v1/admin/sync-configs/auto-import-capabilities": true,
		"GET /api/v1/admin/sync-configs/{config_id}":              true,
		"GET /api/v1/admin/sync-configs/{config_id}/coverage":     true,
		"GET /api/v1/admin/sync-configs/{config_id}/jobs":         true,
		"GET /api/v1/admin/sync-configs/{config_id}/repositories": true,
		"PATCH /api/v1/admin/sync-configs/{config_id}":            true,
		"POST /api/v1/admin/sync-configs":                         true,
		"POST /api/v1/admin/sync-configs/batch":                   true,
		"PUT /api/v1/admin/sync-configs/{config_id}/repositories": true,
	})
}
