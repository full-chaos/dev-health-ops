package goapiproof

// The org-admin setup status and PagerDuty preflight cases (CHAOS-6702),
// registered into the dho-api corpus by init below. Same rules as batch 1
// (dhoapi_admin_access_corpus.go): the org-admin token on both legs, statuses
// from a live capture of BOTH planes on bigboy (with and without an Origin
// header; status, body and headers identical on every row;
// _records/bigboy-a803526d/corpus-baseline-admin9 for setup/status and
// .../corpus-baseline-admin8 for the preflight), bigboy is the leg of record
// until the org-admin proof principal exists on prod.
//
//   - GET admin/setup/status answers the org's onboarding state; both planes read
//     the same rows, so the body is compared as JSON whatever the state is.
//   - POST integrations/pagerduty/preflight is on the persist-nothing POST
//     allowlist (adminPersistNothingPOSTs): the Python handler was read to answer
//     from the database only, with no PagerDuty call. Two requests: a missing
//     credential with two valid datasets (200 {connected:false, datasets:[2]})
//     and an unknown dataset (400), each with an identical body on the capture.
//     Its body needs enabled_datasets; valid names are the keys of the Python
//     DATASET_OAUTH_FAMILIES map.
//
// Named limit (r1 P1): "missing_credential" names the credential "zz-missing"
// and ASSUMES the org has no stored PagerDuty credential of that name. If one
// exists, both planes answer connected:true from the stored descriptor and
// OAuth metadata (still no decryption, no PagerDuty call, no write) and the
// case reads SAME without exercising the missing branch. Parity still holds;
// only the branch differs. The corpus compares planes, not content, so it
// cannot detect this; the bigboy pass records the actual body (connected:false
// on the Fixture Org), and the name was chosen to be one no operator has a
// reason to create. The status read (adminPagerDutyMissingName) has the same
// assumption.

var dhoAPIAdminSetupRunOrder = []string{
	"REST:GET:/api/v1/admin/setup/status",
	"REST:POST:/api/v1/admin/integrations/pagerduty/preflight",
}

var dhoAPIAdminSetupEndpointSpecs = map[string]RESTEndpointSpec{
	"REST:GET:/api/v1/admin/setup/status": adminGET("/api/v1/admin/setup/status", "status", 200, nil),
	"REST:POST:/api/v1/admin/integrations/pagerduty/preflight": {
		Method: "POST", Path: "/api/v1/admin/integrations/pagerduty/preflight", Service: RESTServiceDHOAPI, Credential: RESTCredentialOrgAdmin,
		Requests: []RESTRequest{
			{
				Name:                "missing_credential",
				Body:                map[string]any{"credential_name": "zz-missing", "enabled_datasets": []string{"incidents", "services"}},
				WantCandidateStatus: 200, WantBaselineStatus: 200,
				BodyMode: RESTBodyModeJSON,
			},
			{
				Name:                "unknown_dataset",
				Body:                map[string]any{"credential_name": "zz-missing", "enabled_datasets": []string{"zz-unknown"}},
				WantCandidateStatus: 400, WantBaselineStatus: 400,
				BodyMode: RESTBodyModeJSON,
			},
		},
	},
}

func init() {
	for operation, spec := range dhoAPIAdminSetupEndpointSpecs {
		restEndpointSpecs[operation] = spec
	}
	restRunOrder = append(restRunOrder, dhoAPIAdminSetupRunOrder...)
}
