package apiservice

import (
	"testing"

	authv1 "github.com/full-chaos/dev-health-ops/contracts/auth/v1"
)

// orgAdminReadRows are the org-admin GET rows whose action is a read (D6714 Q2): a delegated bearer reaches them and
// the PDP judges it as its target, so a target without the admin role gets the deny.
var orgAdminReadRows = []string{
	"GET /api/v1/admin/integrations",
	"GET /api/v1/admin/integrations/pagerduty/services",
	"GET /api/v1/admin/integrations/pagerduty/status",
	"GET /api/v1/admin/integrations/pagerduty/webhook-bindings/{binding_id}",
	"GET /api/v1/admin/integrations/{integration_id}",
	"GET /api/v1/admin/integrations/{integration_id}/datasets",
	"GET /api/v1/admin/integrations/{integration_id}/sources",
	"GET /api/v1/admin/orgs/{org_id}/members",
	"GET /api/v1/admin/settings/{category}",
	"GET /api/v1/admin/settings/{category}/{key}",
	"GET /api/v1/admin/teams",
	"GET /api/v1/admin/teams/{team_id}",
}

// The 12 org-admin read rows admit a delegated bearer; the platform-admin rows and every org-admin write stay denied.
func TestTheOrgAdminReadRowsAdmitADelegatedBearer(t *testing.T) {
	allowed, err := parseDelegatedRows(authv1.GoAPIEndpointProfiles)
	if err != nil {
		t.Fatal(err)
	}
	for _, profile := range orgAdminReadRows {
		if !allowed[profile] {
			t.Errorf("%s: a delegated bearer is refused, want admitted", profile)
		}
	}
	for _, profile := range []string{"GET /api/v1/admin/orgs", "GET /api/v1/admin/platform/stats", "GET /api/v1/admin/feature-flags",
		"POST /api/v1/admin/teams", "PATCH /api/v1/admin/integrations/{integration_id}", "POST /api/v1/admin/impersonate/stop"} {
		if allowed[profile] {
			t.Errorf("%s: a delegated bearer is admitted, want refused", profile)
		}
	}
}
