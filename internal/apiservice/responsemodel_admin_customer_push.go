package apiservice

// Response model routes of the admin customer push route family (CHAOS-6722): moved from
// the shared table in responsemodel.go so a PR that adds a route here no
// longer conflicts with every other route PR. See responsemodel_register.go.
func init() {
	registerResponseModelRoutes(map[string]bool{
		"GET /api/v1/admin/customer-push/batches/{ingestion_id}":        true,
		"GET /api/v1/admin/customer-push/schemas":                       true,
		"GET /api/v1/admin/customer-push/schemas/{schema_version}":      true,
		"GET /api/v1/admin/customer-push/sources":                       true,
		"GET /api/v1/admin/customer-push/sources/{source_id}":           true,
		"GET /api/v1/admin/customer-push/sources/{source_id}/batches":   true,
		"GET /api/v1/admin/customer-push/sources/{source_id}/tokens":    true,
		"GET /api/v1/admin/customer-push/tokens":                        true,
		"PATCH /api/v1/admin/customer-push/sources/{source_id}":         true,
		"POST /api/v1/admin/customer-push/sources":                      true,
		"POST /api/v1/admin/customer-push/sources/{source_id}/tokens":   true,
		"POST /api/v1/admin/customer-push/sources/{source_id}/validate": true,
		"POST /api/v1/admin/customer-push/tokens":                       true,
		"POST /api/v1/admin/customer-push/tokens/{token_id}/revoke":     true,
		"POST /api/v1/admin/customer-push/tokens/{token_id}/rotate":     true,
	})
}
