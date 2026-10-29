package contracts

import (
	"encoding/json"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
)

const (
	bootstrapTokenType = "urn:dev-health:params:oauth:token-type:workload-bootstrap"
	jwtTokenType       = "urn:ietf:params:oauth:token-type:jwt"
)

var exchangeClasses = []any{"kubernetes_sa_token_exchange", "workload_bootstrap_credential"}

// The workload-token exchange's two classes share the form content type; the
// RFC 8693 subject_token_type is what picks one, and each class declares its
// own token-type URI.
func TestSelectorGateSeparatesTheTwoExchangeClassesBySubjectTokenType(t *testing.T) {
	registry := realRegistry(t)
	for _, name := range []string{bootstrapTokenType, jwtTokenType} {
		if err := CheckClassSelector(multiClassProfile(t, exchangeClasses, "subject_token_type", name), registry); err != nil {
			t.Errorf("gate refused subject_token_type %q for the exchange classes: %v", name, err)
		}
	}
	for _, class := range []string{"kubernetes_sa_token_exchange", "workload_bootstrap_credential"} {
		var owned []string
		for _, marker := range registry[class].Markers {
			if marker.Kind == "subject_token_type" {
				owned = append(owned, marker.Name)
			}
		}
		if len(owned) != 1 {
			t.Errorf("class %s declares %d subject_token_type markers %v, want exactly one", class, len(owned), owned)
		}
	}
	if err := CheckClassSelector(multiClassProfile(t, exchangeClasses, "content_type", "application/x-www-form-urlencoded"), registry); err == nil {
		t.Error("gate accepted the content type both exchange classes share")
	}
}

// A token-type URI is compared byte for byte: no case folding, no trimming,
// no prefix or parameter match.
func TestSubjectTokenTypeSelectorComparesExactly(t *testing.T) {
	registry := realRegistry(t)
	for _, name := range []string{
		"URN:DEV-HEALTH:PARAMS:OAUTH:TOKEN-TYPE:WORKLOAD-BOOTSTRAP",
		"urn:dev-health:params:oauth:token-type:Workload-Bootstrap",
		bootstrapTokenType + " ",
		" " + bootstrapTokenType,
		"urn:dev-health:params:oauth:token-type:workload",
		bootstrapTokenType + "-v2",
		bootstrapTokenType + ";v=1",
	} {
		if err := CheckClassSelector(multiClassProfile(t, exchangeClasses, "subject_token_type", name), registry); err == nil {
			t.Errorf("gate matched subject_token_type %q, which no class declares byte for byte", name)
		}
	}
	if !markerMatches(ClassMarker{Kind: "subject_token_type", Name: jwtTokenType}, "subject_token_type", jwtTokenType) {
		t.Error("markerMatches refused an identical token-type URI")
	}
}

func TestSelectorGateRejectsASubjectTokenTypeBothClassesDeclare(t *testing.T) {
	shared := ClassMarker{Kind: "subject_token_type", Name: bootstrapTokenType}
	registry := syntheticRegistry(
		RegistryClass{ClassID: "kubernetes_sa_token_exchange", Markers: []ClassMarker{shared}},
		RegistryClass{ClassID: "workload_bootstrap_credential", Markers: []ClassMarker{shared}},
	)
	err := CheckClassSelector(multiClassProfile(t, exchangeClasses, "subject_token_type", bootstrapTokenType), registry)
	if err == nil || !strings.Contains(err.Error(), "shared by accepted classes") {
		t.Fatalf("gate = %v, want the shared-selector refusal", err)
	}
}

// A selector kind outside the closed vocabulary is refused by the gate itself,
// even when a (synthetic) registry class declares a marker of that kind.
func TestSelectorGateRefusesAnUnknownSelectorKind(t *testing.T) {
	registry := syntheticRegistry(
		RegistryClass{ClassID: "kubernetes_sa_token_exchange", Markers: []ClassMarker{{Kind: "form_field", Name: "subject_token_type"}}},
		RegistryClass{ClassID: "workload_bootstrap_credential", Markers: []ClassMarker{{Kind: "credential_prefix", Name: "fcwlb_"}}},
	)
	err := CheckClassSelector(multiClassProfile(t, exchangeClasses, "form_field", "subject_token_type"), registry)
	if err == nil || !strings.Contains(err.Error(), "unknown selector kind") {
		t.Fatalf("gate = %v, want the unknown-kind refusal", err)
	}
}

// The gate's kind list is the schema's enum, both ways.
func TestSelectorKindsEqualTheSchemaEnum(t *testing.T) {
	raw, err := os.ReadFile(filepath.Join(ContractsDir(testRoot(t)), "endpoint-profile.schema.json"))
	if err != nil {
		t.Fatal(err)
	}
	var schema struct {
		Defs struct {
			Profile struct {
				Properties struct {
					Disambiguator struct {
						Properties struct {
							Kind struct {
								Enum []string `json:"enum"`
							} `json:"kind"`
						} `json:"properties"`
					} `json:"class_disambiguator"`
				} `json:"properties"`
			} `json:"endpointProfile"`
		} `json:"$defs"`
	}
	if err := json.Unmarshal(raw, &schema); err != nil {
		t.Fatal(err)
	}
	enum := slices.Clone(schema.Defs.Profile.Properties.Disambiguator.Properties.Kind.Enum)
	kinds := slices.Clone(selectorKinds)
	slices.Sort(enum)
	slices.Sort(kinds)
	if len(enum) == 0 || !slices.Equal(enum, kinds) {
		t.Fatalf("gate kinds %v, schema enum %v", kinds, enum)
	}
	if !slices.Contains(kinds, "subject_token_type") {
		t.Fatal("subject_token_type is not a selector kind")
	}
}

// The selector name rules (content_type and subject_token_type if/then) are
// written out three times: the inventory row schema, the wire profile schema
// and the registry marker schema. They must be the same rules, so a rule
// loosened in one copy fails here.
func TestTheSelectorNameRulesAreTheSameInEverySchema(t *testing.T) {
	inventory := readJSONFile(t, "contracts/auth/v1/endpoint-profile.schema.json")
	wire := readJSONFile(t, "contracts/auth/v1/jsonschema/endpoint-auth-profile.v1.schema.json")
	registry := readJSONFile(t, "contracts/auth/v1/credential-classes.schema.json")
	dig := func(node any, path ...string) any {
		for _, key := range path {
			object, ok := node.(map[string]any)
			if !ok {
				t.Fatalf("path %v: %T is not an object", path, node)
			}
			node = object[key]
		}
		return node
	}
	rules := map[string]any{
		"inventory": dig(inventory, "$defs", "endpointProfile", "properties", "class_disambiguator", "allOf"),
		"wire":      dig(wire, "properties", "class_disambiguator", "allOf"),
		"registry":  dig(registry, "$defs", "credentialClass", "properties", "markers", "items", "allOf"),
	}
	want, _ := json.Marshal(rules["inventory"])
	if list, _ := rules["inventory"].([]any); len(list) != 2 {
		t.Fatalf("inventory selector rules %s, want the content_type and subject_token_type rules", want)
	}
	for name, rule := range rules {
		got, _ := json.Marshal(rule)
		if string(got) != string(want) {
			t.Errorf("%s selector rules differ from the inventory's:\n%s\n%s", name, got, want)
		}
	}
}
