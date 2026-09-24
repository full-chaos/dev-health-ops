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
//     produced-id case for those two would have no id to bind. The audit trail
//     had 2 rows, so audit-logs/{log_id} also has a produced-id case: the id
//     is read from the first item of the audit-logs list (200 on both planes
//     in the same capture family, corpus-baseline-admin2). An org with an
//     empty trail leaves that case unresolved: refused by name, never sent.
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

// adminAuditLogIDProducer names the id read from the audit-logs list's first
// item and bound into audit-logs/{log_id}.
const adminAuditLogIDProducer = "admin_audit_log_id"

// withProducer declares that the entry's first request makes an id available.
func withProducer(spec RESTEndpointSpec, producer RESTIDProducer) RESTEndpointSpec {
	spec.Requests[0].Produces = append(spec.Requests[0].Produces, producer)
	return spec
}

// withSecondRequest adds one more request case to an entry.
func withSecondRequest(spec RESTEndpointSpec, request RESTRequest) RESTEndpointSpec {
	spec.Requests = append(spec.Requests, request)
	return spec
}

var dhoAPIAdminAccessEndpointSpecs = map[string]RESTEndpointSpec{
	"REST:GET:/api/v1/admin/audit-logs": withProducer(adminGET("/api/v1/admin/audit-logs", "list", 200, nil),
		RESTIDProducer{Name: adminAuditLogIDProducer, ListPath: "items", IDField: "id"}),
	"REST:GET:/api/v1/admin/audit-logs/{log_id}": withSecondRequest(adminGET("/api/v1/admin/audit-logs/{log_id}", "missing", 404, map[string]string{"log_id": adminMissingID}),
		RESTRequest{
			Name:                "produced",
			IDBindings:          []RESTIDBinding{{Producer: adminAuditLogIDProducer, PathParam: "log_id"}},
			WantCandidateStatus: 200, WantBaselineStatus: 200,
			BodyMode: RESTBodyModeJSON,
		}),
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
