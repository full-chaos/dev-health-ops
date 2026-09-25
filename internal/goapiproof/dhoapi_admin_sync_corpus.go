package goapiproof

// Batches 3+4 of the admin corpus (CHAOS-6614): the org-admin READ-ONLY sync
// routes, registered into the dho-api
// corpus by init below. Same rules as batch 1 (dhoapi_admin_access_corpus.go):
// the org-admin token on both legs, statuses from a live capture of BOTH planes
// on bigboy (ops a97b97ed, with and without an Origin header; status and
// headers identical on every row, and bodies identical on every row that is in
// this file; record _records/bigboy-a97b97ed/corpus-baseline-admin3), writes
// are real use only (R402/R406), bigboy is the leg of record until CHAOS-6570.
//
// What a case does NOT show, by design:
//
//   - GET admin/teams/{team_id}, admin/teams/{team_id}/discover-members and
//     .../infer-members are NOT in this file yet: the Go routes reached main
//     after this stack's base, so the mounted-route test refuses an entry for
//     them here. Their cases are added once the stack is on main, and
//     discover/infer-members get a MISSING-team case only: Python answers 404
//     for a missing team before any credential lookup or provider call, while
//     a real team could reach a provider API.
//   - The proof org has no sync configuration, so the {config_id} routes
//     (config, coverage, jobs, repositories) have only their missing case, and
//     there is no produced-id case: none can reach provider-side data.
//   - HELD OUT until the timestamp render fix (a corpus entry known red on main
//     would fail every STEP run): teams (list) and identities (list). On the capture their bodies differ ONLY by the known
//     datetime render (Go trims trailing zeros and appends Z, Python emits six
//     digits without Z) on created_at/updated_at; keys and counts are equal.
//     They join this corpus in the PR that fixes the render (a real team's
//     produced-id case, whose id is items[0].team_id and NOT items[0].id, joins
//     with the team route once Go serves it).

// Ids the seeded bigboy Fixture Org rows (or any org that has them) make available:
// the first sync configuration of the sync-configs list (a bare JSON array) and the
// first backfill job of the backfill-jobs list ({items: [...]}). An org with no such
// row leaves the "produced" cases unresolved: refused by name, never sent.
const (
	adminSyncConfigIDProducer  = "admin_sync_config_id"
	adminBackfillJobIDProducer = "admin_backfill_job_id"
)

// adminProducedRead is the produced-id request of an id-scoped read: the id is
// bound into path parameter param and both planes answer 200.
func adminProducedRead(producer, param string) RESTRequest {
	return RESTRequest{
		Name:                "produced",
		IDBindings:          []RESTIDBinding{{Producer: producer, PathParam: param}},
		WantCandidateStatus: 200, WantBaselineStatus: 200,
		BodyMode: RESTBodyModeJSON,
	}
}

var dhoAPIAdminSyncRunOrder = []string{
	"REST:GET:/api/v1/admin/sync-configs",
	"REST:GET:/api/v1/admin/sync-configs/auto-import-capabilities",
	"REST:GET:/api/v1/admin/sync-configs/{config_id}",
	"REST:GET:/api/v1/admin/sync-configs/{config_id}/coverage",
	"REST:GET:/api/v1/admin/sync-configs/{config_id}/jobs",
	"REST:GET:/api/v1/admin/sync-configs/{config_id}/repositories",
	"REST:GET:/api/v1/admin/sync-runs/{run_id}",
	"REST:GET:/api/v1/admin/sync-targets",
	"REST:GET:/api/v1/admin/backfill-jobs",
	"REST:GET:/api/v1/admin/backfill-jobs/{job_id}",
}

// adminMissingConfigGET is a GET on a {config_id} sub-route for a config that
// does not exist (404 on both planes).
func adminMissingConfigGET(path string) RESTEndpointSpec {
	return adminGET(path, "missing", 404, map[string]string{"config_id": adminMissingID})
}

var dhoAPIAdminSyncEndpointSpecs = map[string]RESTEndpointSpec{
	"REST:GET:/api/v1/admin/sync-configs": withProducer(adminGET("/api/v1/admin/sync-configs", "list", 200, nil),
		RESTIDProducer{Name: adminSyncConfigIDProducer, IDField: "id"}),
	"REST:GET:/api/v1/admin/sync-configs/auto-import-capabilities": adminGET("/api/v1/admin/sync-configs/auto-import-capabilities", "capabilities", 200, nil),
	"REST:GET:/api/v1/admin/sync-configs/{config_id}":              withSecondRequest(adminMissingConfigGET("/api/v1/admin/sync-configs/{config_id}"), adminProducedRead(adminSyncConfigIDProducer, "config_id")),
	"REST:GET:/api/v1/admin/sync-configs/{config_id}/coverage":     adminMissingConfigGET("/api/v1/admin/sync-configs/{config_id}/coverage"),
	"REST:GET:/api/v1/admin/sync-configs/{config_id}/jobs":         withSecondRequest(adminMissingConfigGET("/api/v1/admin/sync-configs/{config_id}/jobs"), adminProducedRead(adminSyncConfigIDProducer, "config_id")),
	"REST:GET:/api/v1/admin/sync-configs/{config_id}/repositories": withSecondRequest(adminMissingConfigGET("/api/v1/admin/sync-configs/{config_id}/repositories"), adminProducedRead(adminSyncConfigIDProducer, "config_id")),
	"REST:GET:/api/v1/admin/sync-runs/{run_id}":                    adminGET("/api/v1/admin/sync-runs/{run_id}", "missing", 404, map[string]string{"run_id": adminMissingID}),
	"REST:GET:/api/v1/admin/sync-targets":                          adminGET("/api/v1/admin/sync-targets", "targets", 200, nil),
	"REST:GET:/api/v1/admin/backfill-jobs": withProducer(adminGET("/api/v1/admin/backfill-jobs", "list", 200, nil),
		RESTIDProducer{Name: adminBackfillJobIDProducer, ListPath: "items", IDField: "id"}),
	"REST:GET:/api/v1/admin/backfill-jobs/{job_id}": withSecondRequest(
		adminGET("/api/v1/admin/backfill-jobs/{job_id}", "missing", 404, map[string]string{"job_id": adminMissingID}),
		adminProducedRead(adminBackfillJobIDProducer, "job_id")),
}

func init() {
	for operation, spec := range dhoAPIAdminSyncEndpointSpecs {
		restEndpointSpecs[operation] = spec
	}
	restRunOrder = append(restRunOrder, dhoAPIAdminSyncRunOrder...)
}
