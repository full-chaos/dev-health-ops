package contracts

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/full-chaos/dev-health-ops/contracts/auth/v1/vocabulary"
)

// unflaggedOrganizationActionRows is the closed list of profile rows that may ask an organization action without the
// platform_admin flag. PATCH /api/v1/orgs/me asks org.write: a platform admin with no membership row keeps the legacy 403
// (product rule 1), executed on the real handler and on a real Postgres by
// TestLegacyNoRowAndPlatformAdminAnswersOrgsMePatchVenueOracle (internal/apiservice).
var unflaggedOrganizationActionRows = map[string]string{
	"PATCH /api/v1/orgs/me [dev-health-ops-api @api]": "org.write",
}

// C4. An organization action without the platform_admin flag has no legacy cell for a superuser with no membership
// row: no served route asks it, so there is no legacy path to execute. A profile row or GraphQL field that starts to
// ask one must bring its legacy cell (and the flag, if legacy admits); this census fails until it does.
func TestPlatformAdminUnflaggedOrganizationActionsAskedByNoRoute(t *testing.T) {
	var catalog struct {
		Actions []struct {
			Action        string `json:"action"`
			Scope         string `json:"scope"`
			ResourceKind  string `json:"resource_kind"`
			PlatformAdmin string `json:"platform_admin"`
		} `json:"actions"`
	}
	if err := json.Unmarshal(vocabulary.ActionsV1, &catalog); err != nil {
		t.Fatal(err)
	}
	unflagged := map[string]bool{}
	for _, a := range catalog.Actions {
		if a.Scope == "organization" && a.ResourceKind == "organization" && a.PlatformAdmin != "allowed" {
			unflagged[a.Action] = true
		}
	}
	if len(unflagged) == 0 {
		t.Fatal("no unflagged organization action; the census is vacuous")
	}

	rows, seen := 0, map[string]bool{}
	for _, profile := range platformAdminProfiles {
		raw, err := os.ReadFile(filepath.Join(testRoot(t), "contracts/auth/v1/endpoint-profiles."+profile+".json"))
		if err != nil {
			t.Fatal(err)
		}
		var doc struct {
			Rows []platformAdminProfileRow `json:"rows"`
		}
		if err := json.Unmarshal(raw, &doc); err != nil {
			t.Fatalf("%s: %v", profile, err)
		}
		for _, r := range doc.Rows {
			rows++
			if r.Action == nil || !unflagged[*r.Action] {
				continue
			}
			if want, ok := unflaggedOrganizationActionRows[r.ID]; ok && want == *r.Action {
				seen[r.ID] = true
				continue
			}
			t.Errorf("%s row %q asks the unflagged organization action %s: it needs an executed legacy cell for a superuser with no membership row", profile, r.ID, *r.Action)
		}
	}
	if rows == 0 {
		t.Fatal("no profile row read")
	}
	for id := range unflaggedOrganizationActionRows {
		if !seen[id] {
			t.Errorf("exempt row %q is gone; drop it from the list", id)
		}
	}

	raw, err := os.ReadFile(filepath.Join(testRoot(t), "contracts/auth/v1/graphql-field-actions.json"))
	if err != nil {
		t.Fatal(err)
	}
	var fields struct {
		Rows []map[string]any `json:"rows"`
	}
	if err := json.Unmarshal(raw, &fields); err != nil {
		t.Fatal(err)
	}
	if len(fields.Rows) == 0 {
		t.Fatal("no GraphQL field row read")
	}
	for _, f := range fields.Rows {
		if a, _ := f["action"].(string); unflagged[a] {
			t.Errorf("GraphQL field %v asks the unflagged organization action %s", f, a)
		}
	}
}
