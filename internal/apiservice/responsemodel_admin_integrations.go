package apiservice

// Response model routes of the admin integrations route family (CHAOS-6722): moved from
// the shared table in responsemodel.go so a PR that adds a route here no
// longer conflicts with every other route PR. See responsemodel_register.go.
func init() {
	registerResponseModelRoutes(map[string]bool{
		"GET /api/v1/admin/integrations/pagerduty/services":                                true,
		"GET /api/v1/admin/integrations/pagerduty/status":                                  true,
		"GET /api/v1/admin/integrations/pagerduty/webhook-bindings/{binding_id}":           true,
		"POST /api/v1/admin/integrations/github/install-callback":                          true,
		"POST /api/v1/admin/integrations/github/install-url":                               true,
		"POST /api/v1/admin/integrations/pagerduty/authorize":                              true,
		"POST /api/v1/admin/integrations/pagerduty/api-token":                              true,
		"POST /api/v1/admin/integrations/pagerduty/callback":                               true,
		"POST /api/v1/admin/integrations/pagerduty/client-credentials":                     true,
		"POST /api/v1/admin/integrations/pagerduty/disconnect":                             true,
		"POST /api/v1/admin/integrations/pagerduty/preflight":                              true,
		"POST /api/v1/admin/integrations/pagerduty/webhook-bindings":                       true,
		"POST /api/v1/admin/integrations/pagerduty/webhook-bindings/{binding_id}/activate": true,
		"POST /api/v1/admin/integrations/pagerduty/webhook-bindings/{binding_id}/revoke":   true,
		"POST /api/v1/admin/integrations/pagerduty/webhook-bindings/{binding_id}/rotate":   true,
		// The generic integration admin routes (integrations.py); a separate
		// group so gofmt aligns them without reflowing the rest.
		"GET /api/v1/admin/integrations":                                        true,
		"POST /api/v1/admin/integrations":                                       true,
		"GET /api/v1/admin/integrations/{integration_id}":                       true,
		"PATCH /api/v1/admin/integrations/{integration_id}":                     true,
		"GET /api/v1/admin/integrations/{integration_id}/sources":               true,
		"PATCH /api/v1/admin/integrations/{integration_id}/sources/{source_id}": true,
		"GET /api/v1/admin/integrations/{integration_id}/datasets":              true,
		"PATCH /api/v1/admin/integrations/{integration_id}/datasets":            true,
		"POST /api/v1/admin/integrations/{integration_id}/discover":             true,
		"POST /api/v1/admin/integrations/{integration_id}/sync":                 true,
		"POST /api/v1/admin/integrations/{integration_id}/backfill":             true,
	})
}
