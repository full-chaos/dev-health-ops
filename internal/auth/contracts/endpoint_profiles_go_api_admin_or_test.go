package contracts

import (
	"encoding/json"
	"slices"
	"sort"
	"testing"
)

// adminOrPlatformRows are the go-api rows whose action is a platform action on an org_scoped row: the rows mounted
// WrapAdminOr(<platform action>), where an org owner or admin of the token's organization passes too.
var adminOrPlatformRows = []string{
	"DELETE /api/v1/admin/users/{user_id} [dev-health-ops-api @api]",
	"GET /api/v1/admin/users [dev-health-ops-api @api]",
	"GET /api/v1/admin/users/{user_id} [dev-health-ops-api @api]",
	"PATCH /api/v1/admin/users/{user_id} [dev-health-ops-api @api]",
	"POST /api/v1/admin/users [dev-health-ops-api @api]",
	"POST /api/v1/admin/users/{user_id}/password [dev-health-ops-api @api]",
}

// platformDenyRows are the go-api rows whose action is a platform action on an org_scoped row mounted
// WrapPlatformDeny: only the platform holder passes, and the refusal keeps the handler's text.
var platformDenyRows = []string{
	"POST /api/v1/telemetry/report [dev-health-ops-api @api]",
}

// Census: every covered row that names a platform action on an org_scoped row is listed in adminOrPlatformRows and is
// mounted admin-or, or is listed in platformDenyRows and is mounted WrapPlatformDeny; the lists have no other row.
func TestPlatformActionsOnOrgScopedRowsAreTheAdminOrMounts(t *testing.T) {
	root := testRoot(t)
	scopes := catalogScopes(t)
	var got []string
	for _, row := range coveredRows(t) {
		action, _ := row["action"].(string)
		tenancy, _ := row["tenant_requirement"].(string)
		if scopes[action] != "platform" || tenancy != "org_scoped" {
			continue
		}
		id, _ := row["id"].(string)
		got = append(got, id)
		adminOr, err := registeredAdminOr(row, root)
		deny, errDeny := registeredPlatformDeny(row, root)
		listedDeny := slices.Contains(platformDenyRows, id)
		if err != nil || errDeny != nil || (listedDeny && !deny) || (!listedDeny && !adminOr) {
			t.Errorf("%s: a platform action on an org_scoped row whose registration is not the mount its list names (%v %v)", id, err, errDeny)
		}
	}
	sort.Strings(got)
	want := append(append([]string{}, adminOrPlatformRows...), platformDenyRows...)
	sort.Strings(want)
	if !slices.Equal(got, want) {
		t.Fatalf("platform actions on org_scoped rows %v, want %v", got, want)
	}
}

// The narrowed rule refuses a platform action on an org_scoped row whose mount is not admin-or: the planted row is an
// admin-or users row whose method, route and source name a plain Admin mount (the org members list). Its source line
// is arbitrary: the registration is found by symbol.
func TestAPlatformActionOnAnOrgScopedRowWithAnotherMountFails(t *testing.T) {
	root := testRoot(t)
	scopes := catalogScopes(t)
	for _, row := range coveredRows(t) {
		if row["id"] != adminOrPlatformRows[1] {
			continue
		}
		if got := adminFieldViolations(row, scopes, root); len(got) != 0 {
			t.Fatalf("the unmodified admin-or row has violations: %v", got)
		}
		raw, _ := json.Marshal(row)
		var planted map[string]any
		_ = json.Unmarshal(raw, &planted)
		planted["method"], planted["route"] = "GET", "/api/v1/admin/orgs/{org_id}/members"
		planted["source"] = map[string]any{"file": "internal/apiservice/admin/orgs.go", "line": float64(1)}
		if !violationsContain(adminFieldViolations(planted, scopes, root), "tenant_requirement") {
			t.Fatal("a platform action on an org_scoped row mounted at plain Admin passed every check")
		}
		return
	}
	t.Fatal("the GET /api/v1/admin/users row is missing")
}

// The same refusal for the WrapPlatformDeny mount: the planted row is the telemetry report row whose method, route and
// source name a plain Authenticated mount (the status route).
func TestAPlatformActionOnAnOrgScopedRowWithoutThePlatformDenyMountFails(t *testing.T) {
	root := testRoot(t)
	scopes := catalogScopes(t)
	for _, row := range coveredRows(t) {
		if row["id"] != platformDenyRows[0] {
			continue
		}
		if got := adminFieldViolations(row, scopes, root); len(got) != 0 {
			t.Fatalf("the unmodified platform-deny row has violations: %v", got)
		}
		raw, _ := json.Marshal(row)
		var planted map[string]any
		_ = json.Unmarshal(raw, &planted)
		planted["method"], planted["route"] = "GET", "/api/v1/telemetry/status"
		if !violationsContain(adminFieldViolations(planted, scopes, root), "tenant_requirement") {
			t.Fatal("a platform action on an org_scoped row without the WrapPlatformDeny mount passed every check")
		}
		return
	}
	t.Fatal("the POST /api/v1/telemetry/report row is missing")
}
