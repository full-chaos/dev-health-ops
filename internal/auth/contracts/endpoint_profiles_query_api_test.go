package contracts

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/google/jsonschema-go/jsonschema"
)

// preQueryAPIServices are the services the closed enum held before query-api
// joined it. Any other enum value must come with rows in a rows file here.
var preQueryAPIServices = map[string]bool{
	"dev-health-ops-api": true, "dev-health-ops-billing-edge": true, "dev-health-web": true,
	"dev-health-acr-api": true, "dev-health-acr-mcp": true,
}

func resolveEndpointProfileSchema(t *testing.T) (*jsonschema.Resolved, struct {
	Services []string
}) {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join(ContractsDir(testRoot(t)), "endpoint-profile.schema.json"))
	if err != nil {
		t.Fatal(err)
	}
	var schema jsonschema.Schema
	if err := json.Unmarshal(raw, &schema); err != nil {
		t.Fatal(err)
	}
	resolved, err := schema.Resolve(&jsonschema.ResolveOptions{})
	if err != nil {
		t.Fatal(err)
	}
	var enum struct {
		Defs struct {
			Profile struct {
				Properties struct {
					Service struct {
						Enum []string `json:"enum"`
					} `json:"service"`
				} `json:"properties"`
			} `json:"endpointProfile"`
		} `json:"$defs"`
	}
	if err := json.Unmarshal(raw, &enum); err != nil {
		t.Fatal(err)
	}
	return resolved, struct{ Services []string }{enum.Defs.Profile.Properties.Service.Enum}
}

func loadQueryAPIDocument(t *testing.T) map[string]any {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join(ContractsDir(testRoot(t)), "endpoint-profiles.query-api.json"))
	if err != nil {
		t.Fatal(err)
	}
	var document map[string]any
	if err := json.Unmarshal(raw, &document); err != nil {
		t.Fatal(err)
	}
	return document
}

// The rows' completeness against the routes the binary serves is policed in
// internal/queryapi/server (TestEveryServedRouteHasAnEndpointProfileRow...);
// this test owns the contract side: schema validity and the closed vocabularies.
func TestQueryAPIEndpointProfilesValidateAndUseTheClosedVocabularies(t *testing.T) {
	resolved, _ := resolveEndpointProfileSchema(t)
	document := loadQueryAPIDocument(t)
	if err := resolved.Validate(document); err != nil {
		t.Fatalf("query-api profile rows do not satisfy endpoint-profile.schema.json: %v", err)
	}

	frozen := frozenCredentialClassIDs(t)
	ids := map[string]bool{}
	for _, item := range document["rows"].([]any) {
		row := item.(map[string]any)
		id := row["id"].(string)
		if ids[id] {
			t.Errorf("duplicate row id %q", id)
		}
		ids[id] = true
		if row["service"] != "dev-health-query-api" {
			t.Errorf("row %v has service %v, want dev-health-query-api", id, row["service"])
		}
		classes, _ := row["accepted_credential_classes"].([]any)
		gaps, _ := row["gaps"].([]any)
		if row["classification"] == "protected" && len(classes) == 0 && len(gaps) == 0 {
			t.Errorf("protected row %v accepts no credential class and declares no gap saying why", id)
		}
		if row["classification"] == "public" && row["public_rationale"] == nil {
			t.Errorf("public row %v has no public_rationale", id)
		}
		for _, class := range classes {
			if !frozen[class.(string)] {
				t.Errorf("row %v accepts %v, which is not in credential-classes.json", id, class)
			}
		}
	}
	if !frozen["effective_principal_envelope"] {
		t.Error("the effective_principal_envelope class is missing from credential-classes.json")
	}
}

// TestAnUnknownServiceFailsAndTheEnumHoldsNoServiceWithoutRows: a row naming a
// service outside the closed enum is refused by the schema, and every enum
// value beyond the original five has rows in this file.
func TestAnUnknownServiceFailsAndTheEnumHoldsNoServiceWithoutRows(t *testing.T) {
	resolved, enum := resolveEndpointProfileSchema(t)

	document := loadQueryAPIDocument(t)
	rows := document["rows"].([]any)
	rows[0].(map[string]any)["service"] = "dev-health-not-registered"
	err := resolved.Validate(document)
	if err == nil || !strings.Contains(err.Error(), "enum") {
		t.Fatalf("a row with an unknown service validated (err = %v)", err)
	}

	withRows := map[string]bool{}
	for _, item := range loadQueryAPIDocument(t)["rows"].([]any) {
		withRows[item.(map[string]any)["service"].(string)] = true
	}
	for _, service := range enum.Services {
		if !preQueryAPIServices[service] && !withRows[service] {
			t.Errorf("service %q is in the enum but has no rows in endpoint-profiles.query-api.json", service)
		}
	}
	for service := range withRows {
		found := false
		for _, candidate := range enum.Services {
			found = found || candidate == service
		}
		if !found {
			t.Errorf("rows name service %q, which is not in the enum", service)
		}
	}
}
