package apiservice

// Response model routes of the ingest route family (CHAOS-6722): moved from
// the shared table in responsemodel.go so a PR that adds a route here no
// longer conflicts with every other route PR. See responsemodel_register.go.
func init() {
	registerResponseModelRoutes(map[string]bool{
		"POST /api/v1/ingest/commits":       true,
		"POST /api/v1/ingest/deployments":   true,
		"POST /api/v1/ingest/incidents":     true,
		"POST /api/v1/ingest/pull-requests": true,
		"POST /api/v1/ingest/work-items":    true,
		"POST /api/v1/ingest/telemetry":     true,
	})
}
