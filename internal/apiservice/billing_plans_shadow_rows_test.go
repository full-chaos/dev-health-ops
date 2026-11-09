package apiservice

import (
	"encoding/json"
	"maps"
	"slices"
	"testing"

	authv1 "github.com/full-chaos/dev-health-ops/contracts/auth/v1"
	"github.com/full-chaos/dev-health-ops/internal/api/policy"
)

// billingPlanRows are the plan catalog reads: policy.Optional, public rows with no action.
var billingPlanRows = []string{"GET /api/v1/billing/plans", "GET /api/v1/billing/plans/{plan_id}"}

// profilesWithPlanRowsAs is the go-api profile document with the plan catalog rows classified as classification.
func profilesWithPlanRowsAs(t *testing.T, classification string) []byte {
	t.Helper()
	var document map[string]any
	if err := json.Unmarshal(authv1.GoAPIEndpointProfiles, &document); err != nil {
		t.Fatal(err)
	}
	found := 0
	for _, item := range document["rows"].([]any) {
		row := item.(map[string]any)
		method, _ := row["method"].(string)
		route, _ := row["route"].(string)
		if row["service"] != goAPIAccessService || !slices.Contains(billingPlanRows, method+" "+route) {
			continue
		}
		if row["action"] != nil {
			t.Fatalf("%s %s names action %v: its classification now reaches the shadow and enforce rows", method, route, row["action"])
		}
		row["classification"] = classification
		found++
	}
	if found != len(billingPlanRows) {
		t.Fatalf("found %d of the %d plan catalog rows", found, len(billingPlanRows))
	}
	raw, err := json.Marshal(document)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

// The plan catalog rows' classification reaches no PDP row: ParseShadowRows, which builds the shadow's and the
// enforcer's rows, gives the same go-api rows with the plan rows protected (the old profile) or public (the
// served form), and neither holds a plan row.
func TestBillingPlanRowsClassificationChangesNoShadowRow(t *testing.T) {
	parse := func(name string, raw []byte) map[string]policy.ShadowRow {
		rows, err := policy.ParseShadowRows(raw, goAPIAccessService)
		if err != nil {
			t.Fatalf("%s profile: %v", name, err)
		}
		if len(rows) == 0 {
			t.Fatalf("%s profile: no shadow row, the parse measured nothing", name)
		}
		for _, key := range billingPlanRows {
			if _, ok := rows[key]; ok {
				t.Errorf("%s profile: the shadow rows hold %s", name, key)
			}
		}
		return rows
	}
	protected := parse("protected", profilesWithPlanRowsAs(t, "protected"))
	public := parse("public", profilesWithPlanRowsAs(t, "public"))
	embedded := parse("embedded", authv1.GoAPIEndpointProfiles)
	if !maps.Equal(protected, public) {
		t.Errorf("the shadow rows differ: %d with the plan rows protected, %d with them public", len(protected), len(public))
	}
	if !maps.Equal(embedded, public) {
		t.Errorf("the embedded profile's shadow rows (%d) differ from the public form's (%d)", len(embedded), len(public))
	}
}
