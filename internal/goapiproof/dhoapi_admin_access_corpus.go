package goapiproof

// Batch 1 of the admin corpus (CHAOS-6603): the org-admin READ-ONLY routes of
// the audit trail, the IP allowlist, the retention policies and the
// impersonation status, registered into the dho-api corpus by init below.
//
// Every entry authenticates as the org-admin proof principal (see
// RESTCredentialOrgAdmin, -org-admin-token-file): the same token on both legs.
// The statuses are what a live capture of BOTH planes answered on bigboy (ops
// a97b97ed, Fixture Org, both with and without an Origin header: status, body
// and headers identical on all nine routes; record
// _records/bigboy-a97b97ed/corpus-baseline-admin1). A change to one is a
// change of the reference.
//
// What a case does NOT show, by design:
//
//   - The {id} routes are sent an id that does not exist (404) or, for the
//     resource/user lists, an id with no rows (an empty list). The proof org
//     had no IP-allowlist entry and no retention policy at capture time, so a
//     produced-id case for those two would have no id to bind; the audit trail
//     had 2 rows, so a produced-id case for audit-logs/{log_id} is a follow-up
//     that needs its own capture before its status is set.
//   - Writes (every POST/PATCH/DELETE of these areas, and the retention
//     execute) are real use only and have no synthetic case (R402/R406).
//   - Prod is not covered: it has no org-admin proof principal until
//     CHAOS-6570; this leg of record is bigboy.

const adminMissingID = "00000000-0000-4000-8000-000000000000"

var dhoAPIAdminAccessRunOrder = []string{
	"REST:GET:/api/v1/admin/audit-logs",
	"REST:GET:/api/v1/admin/audit-logs/{log_id}",
	"REST:GET:/api/v1/admin/audit-logs/resource/{resource_type}/{resource_id}",
	"REST:GET:/api/v1/admin/audit-logs/user/{user_id}",
	"REST:GET:/api/v1/admin/ip-allowlist",
	"REST:GET:/api/v1/admin/ip-allowlist/{entry_id}",
	"REST:GET:/api/v1/admin/retention-policies",
	"REST:GET:/api/v1/admin/retention-policies/{policy_id}",
	"REST:GET:/api/v1/admin/impersonate/status",
}

// adminGET builds one org-admin read entry: a single request answered with
// the same status by both planes and compared as JSON.
func adminGET(path, name string, status int, literals map[string]string) RESTEndpointSpec {
	return RESTEndpointSpec{
		Method: "GET", Path: path, Service: RESTServiceDHOAPI, Credential: RESTCredentialOrgAdmin,
		Requests: []RESTRequest{{
			Name:                name,
			PathLiterals:        literals,
			WantCandidateStatus: status, WantBaselineStatus: status,
			BodyMode: RESTBodyModeJSON,
		}},
	}
}

var dhoAPIAdminAccessEndpointSpecs = map[string]RESTEndpointSpec{
	"REST:GET:/api/v1/admin/audit-logs":                     adminGET("/api/v1/admin/audit-logs", "list", 200, nil),
	"REST:GET:/api/v1/admin/audit-logs/{log_id}":            adminGET("/api/v1/admin/audit-logs/{log_id}", "missing", 404, map[string]string{"log_id": adminMissingID}),
	"REST:GET:/api/v1/admin/audit-logs/user/{user_id}":      adminGET("/api/v1/admin/audit-logs/user/{user_id}", "user_without_rows", 200, map[string]string{"user_id": adminMissingID}),
	"REST:GET:/api/v1/admin/ip-allowlist":                   adminGET("/api/v1/admin/ip-allowlist", "list", 200, nil),
	"REST:GET:/api/v1/admin/ip-allowlist/{entry_id}":        adminGET("/api/v1/admin/ip-allowlist/{entry_id}", "missing", 404, map[string]string{"entry_id": adminMissingID}),
	"REST:GET:/api/v1/admin/retention-policies":             adminGET("/api/v1/admin/retention-policies", "list", 200, nil),
	"REST:GET:/api/v1/admin/impersonate/status":             adminGET("/api/v1/admin/impersonate/status", "status", 200, nil),
	"REST:GET:/api/v1/admin/retention-policies/{policy_id}": adminGET("/api/v1/admin/retention-policies/{policy_id}", "missing", 404, map[string]string{"policy_id": adminMissingID}),
	"REST:GET:/api/v1/admin/audit-logs/resource/{resource_type}/{resource_id}": adminGET(
		"/api/v1/admin/audit-logs/resource/{resource_type}/{resource_id}", "resource_without_rows", 200,
		map[string]string{"resource_type": "user", "resource_id": adminMissingID}),
}

func init() {
	for operation, spec := range dhoAPIAdminAccessEndpointSpecs {
		restEndpointSpecs[operation] = spec
	}
	restRunOrder = append(restRunOrder, dhoAPIAdminAccessRunOrder...)
}
