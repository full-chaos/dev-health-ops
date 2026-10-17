package contracts

import (
	"encoding/json"
	"os"
	"path/filepath"
	"sort"
	"testing"

	"github.com/google/jsonschema-go/jsonschema"
)

// queryAPISurfaces is the closed list of query-api's HTTP surfaces that carry an
// endpoint-profile row (CHAOS-4737). The enum value and its rows ship together:
// the service enum names query-api only because these rows exist.
var queryAPISurfaces = []string{"GET /healthz", "GET /readyz", "POST /query"}

func TestQueryAPIEndpointProfilesValidateAndUseTheClosedVocabularies(t *testing.T) {
	dir := ContractsDir(testRoot(t))
	schemaRaw, err := os.ReadFile(filepath.Join(dir, "endpoint-profile.schema.json"))
	if err != nil {
		t.Fatal(err)
	}
	var schema jsonschema.Schema
	if err := json.Unmarshal(schemaRaw, &schema); err != nil {
		t.Fatal(err)
	}
	resolved, err := schema.Resolve(&jsonschema.ResolveOptions{})
	if err != nil {
		t.Fatal(err)
	}
	rowsRaw, err := os.ReadFile(filepath.Join(dir, "endpoint-profiles.query-api.json"))
	if err != nil {
		t.Fatal(err)
	}
	var document map[string]any
	if err := json.Unmarshal(rowsRaw, &document); err != nil {
		t.Fatal(err)
	}
	if err := resolved.Validate(document); err != nil {
		t.Fatalf("query-api profile rows do not satisfy endpoint-profile.schema.json: %v", err)
	}

	frozen := frozenCredentialClassIDs(t)
	var got []string
	for _, item := range document["rows"].([]any) {
		row := item.(map[string]any)
		if row["service"] != "dev-health-query-api" {
			t.Errorf("row %v has service %v, want dev-health-query-api", row["id"], row["service"])
		}
		got = append(got, row["method"].(string)+" "+row["route"].(string))
		classes, _ := row["accepted_credential_classes"].([]any)
		if row["classification"] == "protected" && len(classes) == 0 {
			t.Errorf("protected row %v accepts no credential class", row["id"])
		}
		if row["classification"] == "public" && row["public_rationale"] == nil {
			t.Errorf("public row %v has no public_rationale", row["id"])
		}
		for _, class := range classes {
			if !frozen[class.(string)] {
				t.Errorf("row %v accepts %v, which is not in credential-classes.json", row["id"], class)
			}
		}
	}
	sort.Strings(got)
	want := append([]string(nil), queryAPISurfaces...)
	sort.Strings(want)
	if len(got) != len(want) {
		t.Fatalf("rows = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("rows = %v, want %v", got, want)
		}
	}

	if !frozen["effective_principal_envelope"] {
		t.Error("the effective_principal_envelope class is missing from credential-classes.json")
	}
}

// TestServiceEnumNamesQueryAPIOnlyWithItsRows pins that the enum value cannot
// be dropped or added alone: a row naming an unknown service must fail.
func TestServiceEnumNamesQueryAPIOnlyWithItsRows(t *testing.T) {
	raw, err := os.ReadFile(filepath.Join(ContractsDir(testRoot(t)), "endpoint-profile.schema.json"))
	if err != nil {
		t.Fatal(err)
	}
	var schema struct {
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
	if err := json.Unmarshal(raw, &schema); err != nil {
		t.Fatal(err)
	}
	found := false
	for _, service := range schema.Defs.Profile.Properties.Service.Enum {
		found = found || service == "dev-health-query-api"
	}
	if !found {
		t.Fatal("dev-health-query-api is missing from the service enum")
	}
}
