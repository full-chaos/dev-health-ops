package apiservice

// Response model routes of the telemetry route family (CHAOS-6722): moved from
// the shared table in responsemodel.go so a PR that adds a route here no
// longer conflicts with every other route PR. See responsemodel_register.go.
func init() {
	registerResponseModelRoutes(map[string]bool{
		"GET /api/v1/telemetry/status":   true,
		"POST /api/v1/telemetry/opt-in":  true,
		"POST /api/v1/telemetry/opt-out": true,
		"POST /api/v1/telemetry/report":  true,
	})
}
