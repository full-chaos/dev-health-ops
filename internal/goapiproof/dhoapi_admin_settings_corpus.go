package goapiproof

import "net/url"

// The org-admin routes of the bigboy admin pass that are not in the earlier
// batches (CHAOS-6688): settings, llm-settings, sync-run units, backfill-job
// detail, PagerDuty status and the GitHub install-url mint, registered into the
// dho-api corpus by init below. Same rules as batch 1
// (dhoapi_admin_access_corpus.go): the org-admin token on both legs, statuses
// from a live capture of BOTH planes on bigboy (with and without an Origin
// header; status and headers identical on every row here, bodies identical
// except the install-url state; record
// _records/bigboy-a2b9bf79/corpus-baseline-admin7), bigboy is the leg of record
// until the org-admin proof principal exists on prod.
//
// What a case does NOT show, by design:
//
//   - GET admin/llm-settings/status, /budget and /spend: on the capture the Go
//     plane answers 404 where Python answers 200, so the routes are not served
//     yet. That is a route gap for the porting ticket, not a corpus case (an
//     entry would fail every STEP run, and the mounted-route test refuses an
//     entry for a route Go does not mount).
//   - PagerDuty preflight (POST: it may call PagerDuty), install-callback (needs
//     an OAuth state; there is no fixture flow), and every write (settings
//     PUT/POST/DELETE, llm-settings PUT/DELETE, DELETE sync-configs/{id}, PUT
//     sync-configs/{id}/repositories): real use only (R402/R406), never a
//     synthetic case; the bigboy pass script runs them on the Fixture Org.
//   - Produced-id cases: the Fixture Org has no setting row, sync run or
//     backfill job, so only missing-id cases exist.
//   - The GitHub install-url mint is compared on STATUS only: its signed state
//     differs on every call. It persists nothing (the Python handler signs a
//     state and builds a URL), which is why it is on the persist-nothing POST
//     allowlist (adminPersistNothingPOSTs).

const (
	adminSettingsCategory        = "general"
	adminSettingsMissingCategory = "zz-missing-category"
	adminSettingsMissingKey      = "zz-missing-key"
	adminPagerDutyMissingName    = "zz-missing"
)

var dhoAPIAdminSettingsRunOrder = []string{
	"REST:GET:/api/v1/admin/settings/{category}",
	"REST:GET:/api/v1/admin/settings/{category}/{key}",
	"REST:GET:/api/v1/admin/llm-settings",
	"REST:GET:/api/v1/admin/sync-runs/{run_id}/units",
	"REST:GET:/api/v1/admin/backfill-jobs/{job_id}",
	"REST:GET:/api/v1/admin/integrations/pagerduty/status",
	"REST:POST:/api/v1/admin/integrations/github/install-url",
}

var dhoAPIAdminSettingsEndpointSpecs = map[string]RESTEndpointSpec{
	// GET /settings/categories is a literal beside GET /settings/{category} in
	// Python; the Go plane serves both through the {category} pattern (its
	// handler answers the category list for the value "categories"), so the
	// list is a request of this entry, not an entry of its own. A category with
	// no rows answers 200 with an empty list on both planes, so an unknown
	// category is a 200 case, not a 404.
	"REST:GET:/api/v1/admin/settings/{category}": withSecondRequest(withSecondRequest(
		adminGET("/api/v1/admin/settings/{category}", "categories", 200, map[string]string{"category": "categories"}),
		RESTRequest{
			Name:                "empty_category",
			PathLiterals:        map[string]string{"category": adminSettingsCategory},
			WantCandidateStatus: 200, WantBaselineStatus: 200,
			BodyMode: RESTBodyModeJSON,
		}),
		RESTRequest{
			Name:                "unknown_category",
			PathLiterals:        map[string]string{"category": adminSettingsMissingCategory},
			WantCandidateStatus: 200, WantBaselineStatus: 200,
			BodyMode: RESTBodyModeJSON,
		}),
	"REST:GET:/api/v1/admin/settings/{category}/{key}": adminGET("/api/v1/admin/settings/{category}/{key}", "missing_key", 404,
		map[string]string{"category": adminSettingsCategory, "key": adminSettingsMissingKey}),
	"REST:GET:/api/v1/admin/llm-settings":             adminGET("/api/v1/admin/llm-settings", "unconfigured", 200, nil),
	"REST:GET:/api/v1/admin/sync-runs/{run_id}/units": adminGET("/api/v1/admin/sync-runs/{run_id}/units", "missing", 404, map[string]string{"run_id": adminMissingID}),
	"REST:GET:/api/v1/admin/backfill-jobs/{job_id}":   adminGET("/api/v1/admin/backfill-jobs/{job_id}", "missing", 404, map[string]string{"job_id": adminMissingID}),
	"REST:GET:/api/v1/admin/integrations/pagerduty/status": withSecondRequest(
		adminGET("/api/v1/admin/integrations/pagerduty/status", "default", 200, nil),
		RESTRequest{
			Name:                "missing_credential",
			Query:               url.Values{"credential_name": {adminPagerDutyMissingName}},
			WantCandidateStatus: 200, WantBaselineStatus: 200,
			BodyMode: RESTBodyModeJSON,
		}),
	"REST:POST:/api/v1/admin/integrations/github/install-url": {
		Method: "POST", Path: "/api/v1/admin/integrations/github/install-url", Service: RESTServiceDHOAPI, Credential: RESTCredentialOrgAdmin,
		Requests: []RESTRequest{{
			Name:                "mint",
			Body:                map[string]any{},
			WantCandidateStatus: 200, WantBaselineStatus: 200,
			BodyMode: RESTBodyModeStatusOnly,
		}},
	},
}

func init() {
	for operation, spec := range dhoAPIAdminSettingsEndpointSpecs {
		restEndpointSpecs[operation] = spec
	}
	restRunOrder = append(restRunOrder, dhoAPIAdminSettingsRunOrder...)
}
