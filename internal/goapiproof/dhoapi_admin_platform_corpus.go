package goapiproof

// Batch 6 of the admin corpus (CHAOS-6617): the platform-superadmin READ-ONLY
// routes, registered into the dho-api corpus by init below. Every entry
// authenticates as the dedicated platform-superadmin proof principal (see
// RESTCredentialPlatformSuperadmin, -platform-token-file): the same token on
// both legs; the principal holds no org membership (R429). Statuses are what a
// live capture of BOTH planes answered on bigboy (ops a97b97ed, with and
// without an Origin header: status, body and headers identical on all seven
// routes; record _records/bigboy-a97b97ed/corpus-baseline-admin6).
//
// What a case does NOT show, by design:
//
//   - Writes (feature-flag PATCH, org create/update/delete, feature-override
//     writes, impersonate, impersonate/stop) are real use only (R402/R406).
//   - Prod: bigboy is the leg of record; prod platform reads happen at a roll's
//     PASS step with the R429 principal.
//   - Volatile bodies. platform/stats counters and the platform/audit-logs page
//     change with activity; both legs are sent back to back and compared as
//     JSON, so a change landing between the legs reads as a mismatch, never a
//     pass.
//   - The feature-overrides list is empty in the proof org (no produced-id
//     case for feature-overrides/{override_id}).

var dhoAPIAdminPlatformRunOrder = []string{
	"REST:GET:/api/v1/admin/feature-flags",
	"REST:GET:/api/v1/admin/orgs",
	"REST:GET:/api/v1/admin/orgs/{org_id}",
	"REST:GET:/api/v1/admin/orgs/{org_id}/feature-overrides",
	"REST:GET:/api/v1/admin/platform/stats",
	"REST:GET:/api/v1/admin/platform/audit-logs",
}

// platformGET is adminGET with the platform-superadmin credential.
func platformGET(path, name string, status int, literals map[string]string) RESTEndpointSpec {
	spec := adminGET(path, name, status, literals)
	spec.Credential = RESTCredentialPlatformSuperadmin
	return spec
}

var dhoAPIAdminPlatformEndpointSpecs = map[string]RESTEndpointSpec{
	"REST:GET:/api/v1/admin/feature-flags": platformGET("/api/v1/admin/feature-flags", "list", 200, nil),
	"REST:GET:/api/v1/admin/orgs":          platformGET("/api/v1/admin/orgs", "list", 200, nil),
	// The org id is the operator-supplied one (-bind org_id=<the -org value>),
	// the same binding the entitlements and members entries use.
	"REST:GET:/api/v1/admin/orgs/{org_id}": withSecondRequest(
		withIDBinding(platformGET("/api/v1/admin/orgs/{org_id}", "own_org", 200, nil), dhoAPIOrgIDProducer, "org_id"),
		RESTRequest{
			Name:                "missing",
			PathLiterals:        map[string]string{"org_id": adminMissingID},
			WantCandidateStatus: 404, WantBaselineStatus: 404,
			BodyMode: RESTBodyModeJSON,
		}),
	"REST:GET:/api/v1/admin/orgs/{org_id}/feature-overrides": withIDBinding(
		platformGET("/api/v1/admin/orgs/{org_id}/feature-overrides", "own_org", 200, nil), dhoAPIOrgIDProducer, "org_id"),
	"REST:GET:/api/v1/admin/platform/stats":      platformGET("/api/v1/admin/platform/stats", "stats", 200, nil),
	"REST:GET:/api/v1/admin/platform/audit-logs": platformGET("/api/v1/admin/platform/audit-logs", "list", 200, nil),
}

// withIDBinding binds an operator-supplied or produced id into the entry's
// first request path.
func withIDBinding(spec RESTEndpointSpec, producer, pathParam string) RESTEndpointSpec {
	spec.Requests[0].IDBindings = append(spec.Requests[0].IDBindings, RESTIDBinding{Producer: producer, PathParam: pathParam})
	return spec
}

func init() {
	for operation, spec := range dhoAPIAdminPlatformEndpointSpecs {
		restEndpointSpecs[operation] = spec
	}
	restRunOrder = append(restRunOrder, dhoAPIAdminPlatformRunOrder...)
}
