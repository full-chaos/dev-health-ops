package contracts

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

func loadGoAPIDocument(t *testing.T) map[string]any {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join(ContractsDir(testRoot(t)), "endpoint-profiles.go-api.json"))
	if err != nil {
		t.Fatal(err)
	}
	var document map[string]any
	if err := json.Unmarshal(raw, &document); err != nil {
		t.Fatal(err)
	}
	return document
}

// The rows' completeness against the routes the go-api listeners mount is
// policed in internal/apiservice (TestEveryMountedRouteHasAnEndpointProfileRow...);
// this test owns the contract side: schema validity and the closed vocabularies.
func TestGoAPIEndpointProfilesValidateAndUseTheClosedVocabularies(t *testing.T) {
	resolved, _ := resolveEndpointProfileSchema(t)
	document := loadGoAPIDocument(t)
	if err := resolved.Validate(document); err != nil {
		t.Fatalf("go-api profile rows do not satisfy endpoint-profile.schema.json: %v", err)
	}
	frozen := frozenCredentialClassIDs(t)
	services := map[string]bool{"dev-health-ops-api": true, "dev-health-ops-billing-edge": true}
	ids := map[string]bool{}
	rows := document["rows"].([]any)
	if len(rows) < 200 {
		t.Fatalf("only %d rows: the inventory lost the go-api surface", len(rows))
	}
	for _, item := range rows {
		row := item.(map[string]any)
		id := row["id"].(string)
		if ids[id] {
			t.Errorf("duplicate row id %q", id)
		}
		ids[id] = true
		if !services[row["service"].(string)] {
			t.Errorf("row %v has service %v, want dev-health-ops-api or dev-health-ops-billing-edge", id, row["service"])
		}
		if row["surface_kind"] != "rest" {
			t.Errorf("row %v has surface_kind %v: go-api serves REST only", id, row["surface_kind"])
		}
		classes, _ := row["accepted_credential_classes"].([]any)
		gaps, _ := row["gaps"].([]any)
		if row["classification"] == "protected" && len(classes) == 0 && len(gaps) == 0 {
			t.Errorf("protected row %v accepts no credential class and declares no gap saying why", id)
		}
		if row["classification"] == "public" && row["public_rationale"] == nil {
			t.Errorf("public row %v has no public_rationale", id)
		}
		for _, violation := range advisoryNullViolations(row) {
			t.Error(violation)
		}
		for _, class := range classes {
			if !frozen[class.(string)] {
				t.Errorf("row %v accepts %v, which is not in credential-classes.json", id, class)
			}
		}
	}
}
