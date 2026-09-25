package apiservice

// Response model routes of the health route family (CHAOS-6722): moved from
// the shared table in responsemodel.go so a PR that adds a route here no
// longer conflicts with every other route PR. See responsemodel_register.go.
func init() {
	registerResponseModelRoutes(map[string]bool{
		"GET /health":          true,
		"GET /health/workers":  false,
		"HEAD /health":         true,
		"HEAD /health/workers": false,
	})
}
