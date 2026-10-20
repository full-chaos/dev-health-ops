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
		for _, violation := range advisoryNullViolations(row) {
			t.Error(violation)
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
// value beyond the original five has rows in an endpoint-profiles.*.json file.
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
	files, err := filepath.Glob(filepath.Join(ContractsDir(testRoot(t)), "endpoint-profiles.*.json"))
	if err != nil || len(files) == 0 {
		t.Fatalf("no endpoint-profiles.*.json inventory found (err=%v)", err)
	}
	for _, file := range files {
		raw, err := os.ReadFile(file)
		if err != nil {
			t.Fatal(err)
		}
		var inventory struct {
			Rows []struct {
				Service string `json:"service"`
			} `json:"rows"`
		}
		if err := json.Unmarshal(raw, &inventory); err != nil {
			t.Fatalf("%s: %v", file, err)
		}
		for _, row := range inventory.Rows {
			withRows[row.Service] = true
		}
	}
	for _, service := range enum.Services {
		if !preQueryAPIServices[service] && !withRows[service] {
			t.Errorf("service %q is in the enum but has no rows in any endpoint-profiles.*.json", service)
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

// A gaps entry names a field only when it starts with "<field>:"; a substring
// match would let prose such as "transaction details" excuse a null action.
// advisoryFields: absent means not applicable, null means undetermined and must
// be paired with a gaps entry naming the field (CHAOS-4744, decision sheet B10
// shape b).
var advisoryFields = []string{
	"action", "resource_resolver", "current_state_cache_behavior",
	"entitlement_requirement", "disclosure_behavior", "token_shape",
}

func advisoryNullViolations(row map[string]any) []string {
	gaps, _ := row["gaps"].([]any)
	var out []string
	for _, field := range advisoryFields {
		value, present := row[field]
		if !present || value != nil {
			continue
		}
		named := false
		for _, gap := range gaps {
			if text, ok := gap.(string); ok && strings.HasPrefix(strings.TrimSpace(text), field+":") {
				named = true
			}
		}
		if !named {
			out = append(out, "row "+row["id"].(string)+" has "+field+"=null (undetermined) with no gaps entry naming "+field)
		}
	}
	return out
}

func TestAdvisoryNullGateNegativeControls(t *testing.T) {
	cases := []struct {
		name string
		row  map[string]any
		want int
	}{
		{"null without a gap is red", map[string]any{"id": "r", "action": nil, "gaps": []any{}}, 1},
		{"absent is green", map[string]any{"id": "r", "gaps": []any{}}, 0},
		{"null with a naming gap is green", map[string]any{"id": "r", "action": nil, "gaps": []any{"action: not traced"}}, 0},
		{"a gap whose prose contains the field name as a substring does not excuse it", map[string]any{"id": "r", "action": nil, "gaps": []any{"transaction details were not profiled"}}, 1},
		{"a gap that mentions the field mid-sentence does not excuse it", map[string]any{"id": "r", "action": nil, "gaps": []any{"no action was taken"}}, 1},
		{"a non-string gap entry does not excuse it", map[string]any{"id": "r", "action": nil, "gaps": []any{42}}, 1},
		{"a prefixed gap for a sibling field does not excuse it", map[string]any{"id": "r", "action": nil, "gaps": []any{"action_scope: not traced"}}, 1},
		{"an unrelated gap does not excuse the null", map[string]any{"id": "r", "token_shape": nil, "gaps": []any{"exposure: unknown"}}, 1},
		{"every advisory field is policed", map[string]any{"id": "r", "action": nil, "resource_resolver": nil, "current_state_cache_behavior": nil,
			"entitlement_requirement": nil, "disclosure_behavior": nil, "token_shape": nil, "gaps": []any{}}, 6},
	}
	for _, c := range cases {
		if got := len(advisoryNullViolations(c.row)); got != c.want {
			t.Errorf("%s: %d violations, want %d", c.name, got, c.want)
		}
	}
}

func TestSchemaStatesAbsentVersusNullOnEveryAdvisoryField(t *testing.T) {
	raw, err := os.ReadFile(filepath.Join(ContractsDir(testRoot(t)), "endpoint-profile.schema.json"))
	if err != nil {
		t.Fatal(err)
	}
	var doc struct {
		Defs struct {
			Profile struct {
				Properties map[string]struct {
					Description string `json:"description"`
				} `json:"properties"`
			} `json:"endpointProfile"`
		} `json:"$defs"`
	}
	if err := json.Unmarshal(raw, &doc); err != nil {
		t.Fatal(err)
	}
	for _, field := range advisoryFields {
		description := doc.Defs.Profile.Properties[field].Description
		const rule = "ABSENT means not applicable to this route (code evidence says so); null means undetermined in this pass and MUST be paired with a gaps entry naming this field"
		if !strings.Contains(description, rule) && !(field == "token_shape" && strings.Contains(description, "ABSENT when no field applies") && strings.Contains(description, "null only when undetermined, paired with a gaps entry naming token_shape,")) {
			t.Errorf("%s description does not state the absent / null rule in full", field)
		}
	}
}

const multiClassQueryAPIRows = 33

func TestEveryMultiClassQueryAPIRowNamesASelectorTheRegistryHonours(t *testing.T) {
	registry := realRegistry(t)
	checked := 0
	for _, item := range loadQueryAPIDocument(t)["rows"].([]any) {
		row := item.(map[string]any)
		classes, _ := row["accepted_credential_classes"].([]any)
		if len(classes) < 2 {
			if _, has := row["class_disambiguator"]; has {
				t.Errorf("row %v accepts %d class(es) and names a selector", row["id"], len(classes))
			}
			continue
		}
		checked++
		if err := CheckClassSelector(row, registry); err != nil {
			t.Errorf("row %v: %v", row["id"], err)
		}
	}
	if checked != multiClassQueryAPIRows {
		t.Errorf("checked %d multi-class rows, want %d", checked, multiClassQueryAPIRows)
	}
}

func TestTheRowSchemaClosesTheSelectorKindAndShape(t *testing.T) {
	resolved, _ := resolveEndpointProfileSchema(t)
	for name, selector := range map[string]any{
		"kind outside the closed set": map[string]any{"kind": "try_each", "name": "Authorization"},
		"missing name":                map[string]any{"kind": "jwt_header_alg"},
		"extra field":                 map[string]any{"kind": "jwt_header_alg", "name": "HS256", "also": "x"},
	} {
		document := loadQueryAPIDocument(t)
		for _, item := range document["rows"].([]any) {
			if row := item.(map[string]any); row["class_disambiguator"] != nil {
				row["class_disambiguator"] = selector
				break
			}
		}
		if err := resolved.Validate(document); err == nil {
			t.Errorf("%s: the row schema accepted it", name)
		}
	}
}

func TestTheRowSchemaRequiresASelectorOnARowThatAcceptsTwoClasses(t *testing.T) {
	resolved, _ := resolveEndpointProfileSchema(t)
	document := loadQueryAPIDocument(t)
	var target map[string]any
	for _, item := range document["rows"].([]any) {
		if row := item.(map[string]any); row["class_disambiguator"] != nil {
			target = row
			break
		}
	}
	if target == nil {
		t.Fatal("no multi-class row to mutate")
	}
	if err := resolved.Validate(document); err != nil {
		t.Fatalf("control: the unmutated document must validate: %v", err)
	}
	delete(target, "class_disambiguator")
	if err := resolved.Validate(document); err == nil {
		t.Errorf("the row schema accepted row %v with two classes and no class_disambiguator", target["id"])
	}
}

func TestTheRowSchemaStillAcceptsASingleClassRowWithoutASelector(t *testing.T) {
	resolved, _ := resolveEndpointProfileSchema(t)
	document := loadQueryAPIDocument(t)
	for _, item := range document["rows"].([]any) {
		if row := item.(map[string]any); row["class_disambiguator"] == nil {
			if classes, _ := row["accepted_credential_classes"].([]any); len(classes) == 1 {
				if err := resolved.Validate(document); err != nil {
					t.Fatalf("a single-class row without a selector must validate: %v", err)
				}
				return
			}
		}
	}
	t.Fatal("no single-class row found")
}

func TestTheRowSchemaRejectsARowWithoutAcceptedCredentialClasses(t *testing.T) {
	resolved, _ := resolveEndpointProfileSchema(t)
	document := loadQueryAPIDocument(t)
	if err := resolved.Validate(document); err != nil {
		t.Fatalf("control: the unmutated document must validate: %v", err)
	}
	for _, withSelector := range []bool{true, false} {
		document := loadQueryAPIDocument(t)
		for _, item := range document["rows"].([]any) {
			row := item.(map[string]any)
			if (row["class_disambiguator"] != nil) != withSelector {
				continue
			}
			delete(row, "accepted_credential_classes")
			delete(row, "class_disambiguator")
			if err := resolved.Validate(document); err == nil {
				t.Errorf("the row schema accepted row %v with no accepted_credential_classes (selector row: %v)", row["id"], withSelector)
			}
			break
		}
	}
}
