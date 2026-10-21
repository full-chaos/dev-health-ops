package contracts

import (
	"encoding/json"
	"os"
	"path/filepath"
	"slices"
	"testing"
)

const (
	legacyHS256Class             = "ops_access_token_hs256"
	inventoryAccessTokenRulesRef = "#/$defs/accessTokenValidationRules"
	wireAccessTokenRulesRef      = "../endpoint-profile.schema.json#/$defs/accessTokenValidationRules"
	wireAccessTokenFieldRef      = "../endpoint-profile.schema.json#/$defs/endpointProfile/properties/access_token_validation"
)

func inventorySchemaDocument(t *testing.T) map[string]any {
	t.Helper()
	return readJSONFile(t, "contracts/auth/v1/endpoint-profile.schema.json")
}

// accessTokenClasses is the access-token class list the rules carry (the
// forbid branch's enum), read from the schema, never restated here.
func accessTokenClasses(t *testing.T) []string {
	t.Helper()
	var document struct {
		Defs struct {
			Rules struct {
				AllOf []struct {
					If struct {
						Properties struct {
							Classes struct {
								Not struct {
									Contains struct {
										Enum []string `json:"enum"`
									} `json:"contains"`
								} `json:"not"`
							} `json:"accepted_credential_classes"`
						} `json:"properties"`
					} `json:"if"`
				} `json:"allOf"`
			} `json:"accessTokenValidationRules"`
		} `json:"$defs"`
	}
	raw, err := os.ReadFile(filepath.Join(ContractsDir(testRoot(t)), "endpoint-profile.schema.json"))
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(raw, &document); err != nil {
		t.Fatal(err)
	}
	var classes []string
	for _, rule := range document.Defs.Rules.AllOf {
		classes = append(classes, rule.If.Properties.Classes.Not.Contains.Enum...)
	}
	if !slices.Contains(classes, legacyHS256Class) || len(classes) < 2 {
		t.Fatalf("access-token classes %v: the rules lost the list or this path into them is stale", classes)
	}
	return classes
}

// withAccessTokenValidation gives a probe profile for one class what the
// access_token_validation rules require of it, so a probe's verdict stays the
// one its test is about: the object for an access-token class (legacy_hs256
// true only for the HS256 class) and HS256 pins for the HS256 class.
func withAccessTokenValidation(t *testing.T, document map[string]any, class string) {
	t.Helper()
	if !slices.Contains(accessTokenClasses(t), class) {
		return
	}
	document["access_token_validation"] = map[string]any{"legacy_hs256": class == legacyHS256Class}
	if pins, ok := document["token"].(map[string]any); ok && class == legacyHS256Class {
		hs256 := map[string]any{}
		for key, value := range pins {
			hs256[key] = value
		}
		hs256["algorithm"] = "HS256"
		document["token"] = hs256
	}
}

func TestAccessTokenValidationIsDefinedOnceAndReferencedByTheWireProfile(t *testing.T) {
	inventory := inventorySchemaDocument(t)
	profile := inventory["$defs"].(map[string]any)["endpointProfile"].(map[string]any)
	field, ok := profile["properties"].(map[string]any)["access_token_validation"].(map[string]any)
	if !ok || field["additionalProperties"] != false || !slices.Contains(anyStrings(field["required"]), "legacy_hs256") {
		t.Fatalf("inventory access_token_validation is not a closed object requiring legacy_hs256: %v", field)
	}
	if !slices.Contains(refsOf(profile["allOf"]), inventoryAccessTokenRulesRef) {
		t.Errorf("the inventory row does not apply %s", inventoryAccessTokenRulesRef)
	}
	wire := profileSchemaDocument(t)
	if got, _ := profileProperty(t, "access_token_validation")["$ref"].(string); got != wireAccessTokenFieldRef {
		t.Errorf("wire access_token_validation $ref = %q, want %q", got, wireAccessTokenFieldRef)
	}
	if !slices.Contains(refsOf(wire["allOf"]), wireAccessTokenRulesRef) {
		t.Errorf("the wire profile does not apply %s", wireAccessTokenRulesRef)
	}
}

func anyStrings(value any) []string {
	items, _ := value.([]any)
	out := make([]string, 0, len(items))
	for _, item := range items {
		if text, ok := item.(string); ok {
			out = append(out, text)
		}
	}
	return out
}

func refsOf(allOf any) []string {
	items, _ := allOf.([]any)
	var refs []string
	for _, item := range items {
		if rule, ok := item.(map[string]any); ok {
			if ref, ok := rule["$ref"].(string); ok {
				refs = append(refs, ref)
			}
		}
	}
	return refs
}

func TestInventoryRowsDeclareTheHS256OptInExplicitly(t *testing.T) {
	resolved, _ := resolveEndpointProfileSchema(t)
	classes := accessTokenClasses(t)
	for _, file := range []string{"endpoint-profiles.auth-service.json", "endpoint-profiles.query-api.json"} {
		t.Run(file, func(t *testing.T) {
			path := "contracts/auth/v1/" + file
			if err := resolved.Validate(readJSONFile(t, path)); err != nil {
				t.Fatalf("%s does not satisfy endpoint-profile.schema.json: %v", file, err)
			}
			rows := readJSONFile(t, path)["rows"].([]any)
			legacy, plain := -1, -1
			for i, item := range rows {
				accepted := anyStrings(item.(map[string]any)["accepted_credential_classes"])
				switch {
				case slices.Contains(accepted, legacyHS256Class) && legacy < 0:
					legacy = i
				case !slices.ContainsFunc(accepted, func(c string) bool { return slices.Contains(classes, c) }) && plain < 0:
					plain = i
				}
			}
			if legacy < 0 || plain < 0 {
				t.Fatalf("%s has no HS256 row (%d) or no row without an access-token class (%d) to probe", file, legacy, plain)
			}
			for _, probe := range []struct {
				name   string
				row    int
				mutate func(row map[string]any)
			}{
				{"an HS256 row without access_token_validation", legacy, func(row map[string]any) { delete(row, "access_token_validation") }},
				{"an HS256 row with legacy_hs256 false", legacy, func(row map[string]any) {
					row["access_token_validation"] = map[string]any{"legacy_hs256": false}
				}},
				{"an HS256 row whose object repeats the classes", legacy, func(row map[string]any) {
					row["access_token_validation"] = map[string]any{"legacy_hs256": true, "classes": []any{legacyHS256Class}}
				}},
				{"access_token_validation on a row with no access-token class", plain, func(row map[string]any) {
					row["access_token_validation"] = map[string]any{"legacy_hs256": false}
				}},
				{"an HS256 opt-in on a row with no access-token class", plain, func(row map[string]any) {
					row["access_token_validation"] = map[string]any{"legacy_hs256": true}
				}},
			} {
				document := readJSONFile(t, path)
				probe.mutate(document["rows"].([]any)[probe.row].(map[string]any))
				if err := resolved.Validate(document); err == nil {
					t.Errorf("%s validated", probe.name)
				}
			}
		})
	}
}

// The wire rule that holds a single-class HS256 route's `token` pins to HS256
// names the other pinned classes; that list must stay the pinned list less
// the HS256 class, or the rule widens or narrows silently.
func TestTheHS256PinRuleNamesEveryOtherPinnedClass(t *testing.T) {
	pinned, _ := profileClassLists(t)
	var want []string
	for _, class := range pinned {
		if class != legacyHS256Class && !slices.Contains(want, class) {
			want = append(want, class)
		}
	}
	found := 0
	for _, item := range profileSchemaDocument(t)["allOf"].([]any) {
		rule, _ := item.(map[string]any)
		cond, _ := rule["if"].(map[string]any)
		props, _ := cond["properties"].(map[string]any)
		classes, _ := props["accepted_credential_classes"].(map[string]any)
		contains, _ := classes["contains"].(map[string]any)
		not, _ := classes["not"].(map[string]any)
		if contains["const"] != legacyHS256Class || not == nil {
			continue
		}
		found++
		others := anyStrings(not["contains"].(map[string]any)["enum"])
		slices.Sort(others)
		sorted := slices.Clone(want)
		slices.Sort(sorted)
		if !slices.Equal(others, sorted) {
			t.Errorf("the HS256 pin rule names %v, want %v", others, sorted)
		}
	}
	if found != 1 {
		t.Fatalf("found %d HS256 single-pin rules, want 1", found)
	}
}
