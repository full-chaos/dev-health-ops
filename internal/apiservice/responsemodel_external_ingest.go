package apiservice

// Response model routes of the external ingest route family (CHAOS-6722): moved from
// the shared table in responsemodel.go so a PR that adds a route here no
// longer conflicts with every other route PR. See responsemodel_register.go.
func init() {
	registerResponseModelRoutes(map[string]bool{
		"GET /api/v1/external-ingest/availability":             true,
		"GET /api/v1/external-ingest/batches":                  true,
		"GET /api/v1/external-ingest/batches/{ingestion_id}":   true,
		"GET /api/v1/external-ingest/schemas":                  true,
		"GET /api/v1/external-ingest/schemas/{schema_version}": true,
		"POST /api/v1/external-ingest/batches":                 false,
		"POST /api/v1/external-ingest/validate":                true,
	})
}
