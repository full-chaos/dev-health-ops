package apiservice

// Response model routes of the admin backfill jobs route family (CHAOS-6722): moved from
// the shared table in responsemodel.go so a PR that adds a route here no
// longer conflicts with every other route PR. See responsemodel_register.go.
func init() {
	registerResponseModelRoutes(map[string]bool{
		"GET /api/v1/admin/backfill-jobs":          false,
		"GET /api/v1/admin/backfill-jobs/{job_id}": false,
	})
}
