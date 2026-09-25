package apiservice

// Response model routes of the webhooks route family (CHAOS-6722): moved from
// the shared table in responsemodel.go so a PR that adds a route here no
// longer conflicts with every other route PR. See responsemodel_register.go.
func init() {
	registerResponseModelRoutes(map[string]bool{
		"GET /api/v1/webhooks/health":                  true,
		"POST /api/v1/webhooks/github":                 true,
		"POST /api/v1/webhooks/gitlab":                 true,
		"POST /api/v1/webhooks/jira":                   true,
		"POST /api/v1/webhooks/pagerduty/{binding_id}": false,
	})
}
