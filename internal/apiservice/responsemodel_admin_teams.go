package apiservice

// Response model routes of the admin teams route family (CHAOS-6722): moved from
// the shared table in responsemodel.go so a PR that adds a route here no
// longer conflicts with every other route PR. See responsemodel_register.go.
func init() {
	registerResponseModelRoutes(map[string]bool{
		"DELETE /api/v1/admin/teams/{team_id}":                        true,
		"GET /api/v1/admin/teams":                                     true,
		"GET /api/v1/admin/teams/discover":                            true,
		"GET /api/v1/admin/teams/pending-changes":                     true,
		"GET /api/v1/admin/teams/{team_id}":                           true,
		"GET /api/v1/admin/teams/{team_id}/discover-members":          true,
		"GET /api/v1/admin/teams/{team_id}/infer-members":             true,
		"PATCH /api/v1/admin/teams/{team_id}":                         true,
		"POST /api/v1/admin/teams":                                    true,
		"POST /api/v1/admin/teams/import":                             true,
		"POST /api/v1/admin/teams/{team_id}/approve-changes":          true,
		"POST /api/v1/admin/teams/{team_id}/confirm-inferred-members": true,
		"POST /api/v1/admin/teams/{team_id}/confirm-members":          true,
		"POST /api/v1/admin/teams/{team_id}/dismiss-changes":          true,
	})
}
