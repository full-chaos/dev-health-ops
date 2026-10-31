package contracts

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/google/jsonschema-go/jsonschema"
)

// RFC 6749 Appendix A.10: grant-name = 1*name-char, name-char = "-" / "." / "_" /
// DIGIT / ALPHA. The contract keeps one spelling: lowercase, 1 to 64 characters.
var (
	grantNamesAccepted = []string{
		"a-b", "a.b", "1a", "_a", ".a", "-a", "a_b_c", strings.Repeat("a", 64),
		"refresh_token", "urn:ietf:params:oauth:grant-type:device_code",
	}
	grantNamesRefused = []string{strings.Repeat("a", 65), "a~b", "A-B", "a b", ""}
)

type selectorValidator struct {
	name     string
	validate func(t *testing.T, kind, name string) error
}

func selectorValidators(t *testing.T) []selectorValidator {
	t.Helper()
	root := testRoot(t)
	raw, err := os.ReadFile(filepath.Join(ContractsDir(root), "endpoint-profile.schema.json"))
	if err != nil {
		t.Fatal(err)
	}
	var full map[string]any
	if err := json.Unmarshal(raw, &full); err != nil {
		t.Fatal(err)
	}
	sub, _ := json.Marshal(map[string]any{
		"$schema": full["$schema"], "$defs": full["$defs"],
		"$ref": "#/$defs/endpointProfile/properties/class_disambiguator",
	})
	var inventorySchema jsonschema.Schema
	if err := json.Unmarshal(sub, &inventorySchema); err != nil {
		t.Fatal(err)
	}
	inventory, err := inventorySchema.Resolve(&jsonschema.ResolveOptions{})
	if err != nil {
		t.Fatal(err)
	}
	registry := registrySchemaResolved(t)
	registryRaw, err := os.ReadFile(filepath.Join(ContractsDir(root), "credential-classes.json"))
	if err != nil {
		t.Fatal(err)
	}
	return []selectorValidator{
		{"inventory", func(t *testing.T, kind, name string) error {
			return inventory.Validate(map[string]any{"kind": kind, "name": name})
		}},
		{"wire", func(t *testing.T, kind, name string) error {
			profile := multiClassProfile(t, []any{"acr_device_flow_code", "acr_browser_refresh_token"}, kind, name)
			return Validate(root, EndpointAuthProfileSurface, profile)
		}},
		{"registry", func(t *testing.T, kind, name string) error {
			var doc map[string]any
			if err := json.Unmarshal(registryRaw, &doc); err != nil {
				t.Fatal(err)
			}
			class := doc["classes"].([]any)[0].(map[string]any)
			class["markers"] = []any{map[string]any{"kind": kind, "name": name}}
			delete(class, "markers_none_reason")
			return registry.Validate(doc)
		}},
	}
}

func TestGrantTypeNameIsTheRFC6749GrantNameInEverySchema(t *testing.T) {
	for _, v := range selectorValidators(t) {
		for _, name := range grantNamesAccepted {
			if err := v.validate(t, "grant_type", name); err != nil {
				t.Errorf("%s schema refused grant_type %q: %v", v.name, name, err)
			}
		}
		for _, name := range grantNamesRefused {
			if err := v.validate(t, "grant_type", name); err == nil {
				t.Errorf("%s schema accepted grant_type %q", v.name, name)
			}
		}
		for _, kind := range []string{"content_type", "dedicated_header", "credential_prefix"} {
			if err := v.validate(t, kind, "_a"); err == nil {
				t.Errorf("%s schema accepted a leading underscore on a %s selector", v.name, kind)
			}
		}
	}
}

func TestSelectorGateAcceptsEveryGrantNameTheSchemasAccept(t *testing.T) {
	classes := []any{"acr_device_flow_code", "acr_browser_refresh_token"}
	for _, name := range grantNamesAccepted {
		registry := syntheticRegistry(
			RegistryClass{ClassID: "acr_device_flow_code", Markers: []ClassMarker{{Kind: "grant_type", Name: name}}},
			RegistryClass{ClassID: "acr_browser_refresh_token", Markers: []ClassMarker{{Kind: "grant_type", Name: "other"}}},
		)
		if err := CheckClassSelector(multiClassProfile(t, classes, "grant_type", name), registry); err != nil {
			t.Errorf("gate refused grant_type %q: %v", name, err)
		}
	}
}

// The schemas cap a selector name at 64 characters; the gate holds the same bound.
func TestSelectorGateRefusesANameLongerThanTheSchemaCap(t *testing.T) {
	classes := []any{"acr_device_flow_code", "acr_browser_refresh_token"}
	cases := []struct{ kind, name string }{
		{"grant_type", strings.Repeat("a", 65)},
		{"grant_type", "urn:example:oauth:grant-type:" + strings.Repeat("x", 40)},
		{"credential_prefix", "a" + strings.Repeat("b", 64)},
		{"dedicated_header", "X" + strings.Repeat("a", 64)},
	}
	for _, c := range cases {
		registry := syntheticRegistry(
			RegistryClass{ClassID: "acr_device_flow_code", Markers: []ClassMarker{{Kind: c.kind, Name: c.name}}},
			RegistryClass{ClassID: "acr_browser_refresh_token", Markers: []ClassMarker{{Kind: c.kind, Name: "other"}}},
		)
		err := CheckClassSelector(multiClassProfile(t, classes, c.kind, c.name), registry)
		if err == nil || !strings.Contains(err.Error(), "longer than 64") {
			t.Errorf("gate = %v for %s name of %d chars, want the length refusal", err, c.kind, len([]rune(c.name)))
		}
	}
	for _, v := range selectorValidators(t) {
		for _, name := range []string{strings.Repeat("a", 65), "urn:example:oauth:grant-type:" + strings.Repeat("x", 40)} {
			if err := v.validate(t, "grant_type", name); err == nil {
				t.Errorf("%s schema accepted a %d-char grant_type name", v.name, len(name))
			}
		}
	}
	ok := strings.Repeat("a", 64)
	registry := syntheticRegistry(
		RegistryClass{ClassID: "acr_device_flow_code", Markers: []ClassMarker{{Kind: "grant_type", Name: ok}}},
		RegistryClass{ClassID: "acr_browser_refresh_token", Markers: []ClassMarker{{Kind: "grant_type", Name: "other"}}},
	)
	if err := CheckClassSelector(multiClassProfile(t, classes, "grant_type", ok), registry); err != nil {
		t.Errorf("gate refused a 64-char grant_type name: %v", err)
	}
}
