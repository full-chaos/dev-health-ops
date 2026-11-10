package apiservice

import (
	"encoding/json"
	"fmt"
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
	return planRowsDocument(t, classification, false)
}

// planRowsDocument is the go-api profile document with the plan catalog rows classified as classification; with only,
// the document holds the plan catalog rows and no other row.
func planRowsDocument(t *testing.T, classification string, only bool) []byte {
	t.Helper()
	var document map[string]any
	if err := json.Unmarshal(authv1.GoAPIEndpointProfiles, &document); err != nil {
		t.Fatal(err)
	}
	found := 0
	var plans []any
	for _, item := range document["rows"].([]any) {
		row := item.(map[string]any)
		method, _ := row["method"].(string)
		route, _ := row["route"].(string)
		if row["service"] != goAPIAccessService || !slices.Contains(billingPlanRows, method+" "+route) {
			continue
		}
		plans = append(plans, row)
		if row["action"] != nil {
			t.Fatalf("%s %s names action %v: its classification now reaches the shadow and enforce rows", method, route, row["action"])
		}
		row["classification"] = classification
		found++
	}
	if found != len(billingPlanRows) {
		t.Fatalf("found %d of the %d plan catalog rows", found, len(billingPlanRows))
	}
	if only {
		document["rows"] = plans
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

// noShadowRow is ParseShadowRows' refusal of a document in which no row names an action.
const noShadowRow = "shadow rows: no row names an action"

// The plan catalog rows alone give no shadow row in either form: ParseShadowRows over a document of only the two
// rows, protected (the old profile) or public (the served form), names no row, so neither form reaches the shadow
// or the enforcer.
func TestBillingPlanRowsAloneGiveNoShadowRow(t *testing.T) {
	results := map[string]string{}
	for _, form := range []struct{ name, classification string }{{"base", "protected"}, {"fixed", "public"}} {
		rows, err := policy.ParseShadowRows(planRowsDocument(t, form.classification, true), goAPIAccessService)
		results[form.name] = fmt.Sprintf("rows %v, error %v", rows, err)
		for _, key := range billingPlanRows {
			if _, ok := rows[key]; ok {
				t.Errorf("%s form (plan rows %s): the shadow rows hold %s", form.name, form.classification, key)
			}
		}
		if err == nil || err.Error() != noShadowRow {
			t.Errorf("%s form (plan rows %s): ParseShadowRows over the plan rows alone gave %s, want %q", form.name, form.classification, results[form.name], noShadowRow)
		}
	}
	if results["base"] != results["fixed"] {
		t.Errorf("the base form gave %s, the fixed form %s", results["base"], results["fixed"])
	}
}
