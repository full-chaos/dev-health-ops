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

// Census: every covered row that names a platform action on an org_scoped row is listed in adminOrPlatformRows and is
// mounted admin-or; the list has no other row.
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
		if mount, err := registeredAdminOr(row, root); err != nil || !mount {
			t.Errorf("%s: a platform action on an org_scoped row whose registration is not admin-or (%v)", id, err)
		}
	}
	sort.Strings(got)
	if !slices.Equal(got, adminOrPlatformRows) {
		t.Fatalf("platform actions on org_scoped rows %v, want %v", got, adminOrPlatformRows)
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
