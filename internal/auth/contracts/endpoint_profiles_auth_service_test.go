package contracts

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

func loadAuthServiceDocument(t *testing.T) map[string]any {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join(ContractsDir(testRoot(t)), "endpoint-profiles.auth-service.json"))
	if err != nil {
		t.Fatal(err)
	}
	var document map[string]any
	if err := json.Unmarshal(raw, &document); err != nil {
		t.Fatal(err)
	}
	return document
}

// The rows' completeness against the routes auth-service serves is policed in
// internal/auth/authruntime (TestEveryServedAuthRouteHasAnEndpointProfileRow...);
// this test owns the contract side: schema validity and the closed
// vocabularies, for the accepted classes and for every issued credential.
func TestAuthServiceEndpointProfilesValidateAndUseTheClosedVocabularies(t *testing.T) {
	resolved, _ := resolveEndpointProfileSchema(t)
	document := loadAuthServiceDocument(t)
	if err := resolved.Validate(document); err != nil {
		t.Fatalf("auth-service profile rows do not satisfy endpoint-profile.schema.json: %v", err)
	}
	frozen := frozenCredentialClassIDs(t)
	ids := map[string]bool{}
	rows := document["rows"].([]any)
	if len(rows) == 0 {
		t.Fatal("endpoint-profiles.auth-service.json has no rows")
	}
	for _, item := range rows {
		row := item.(map[string]any)
		id := row["id"].(string)
		if ids[id] {
			t.Errorf("duplicate row id %q", id)
		}
		ids[id] = true
		if row["service"] != "dev-health-auth-service" {
			t.Errorf("row %v has service %v, want dev-health-auth-service", id, row["service"])
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
		issued, _ := row["issued_credential"].([]any)
		for _, item := range issued {
			class, _ := item.(map[string]any)["class_id"].(string)
			if !frozen[class] {
				t.Errorf("row %v issues %v, which is not in credential-classes.json", id, class)
			}
		}
	}
	if !frozen["refresh_credential"] {
		t.Error("the refresh_credential class is missing from credential-classes.json")
	}
}
