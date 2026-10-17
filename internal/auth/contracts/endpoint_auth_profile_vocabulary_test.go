package contracts

import (
	"encoding/json"
	"net/url"
	"os"
	"sort"
	"testing"
)

func profileSchemaDocument(t *testing.T) map[string]any {
	t.Helper()
	raw, err := os.ReadFile(SchemaPath(testRoot(t), EndpointAuthProfileSurface))
	if err != nil {
		t.Fatalf("reading profile schema: %v", err)
	}
	var document map[string]any
	if err := json.Unmarshal(raw, &document); err != nil {
		t.Fatalf("parsing profile schema: %v", err)
	}
	return document
}

func profileProperty(t *testing.T, name string) map[string]any {
	t.Helper()
	properties, _ := profileSchemaDocument(t)["properties"].(map[string]any)
	property, ok := properties[name].(map[string]any)
	if !ok {
		t.Fatalf("profile schema has no property %q", name)
	}
	return property
}

// A profile that restates a vocabulary another contract owns is the second
// copy the REFERENCE ruling (CHAOS-4931) exists to prevent. These fields must
// be a $ref and must carry no enum, pattern or type of their own: a sibling
// enum would silently narrow or widen the referenced definition.
func TestSharedProfileFieldsAreReferencesNotCopies(t *testing.T) {
	shared := map[string]string{
		"classification":                    "../endpoint-profile.schema.json#/$defs/endpointProfile/properties/classification",
		"minimum_assurance":                 "principal.v1.schema.json#/properties/authentication/properties/assurance",
		"action":                            "principal.v1.schema.json#/properties/actor_chain/items/properties/permitted_actions/items",
		"accepted_credential_classes/items": "principal.v1.schema.json#/properties/credential/properties/class",
	}
	for field, wantRef := range shared {
		t.Run(field, func(t *testing.T) {
			var node map[string]any
			if field == "accepted_credential_classes/items" {
				node, _ = profileProperty(t, "accepted_credential_classes")["items"].(map[string]any)
			} else {
				node = profileProperty(t, field)
			}
			if got, _ := node["$ref"].(string); got != wantRef {
				t.Fatalf("$ref = %q, want %q", got, wantRef)
			}
			for _, banned := range []string{"enum", "pattern", "type", "const"} {
				if _, present := node[banned]; present {
					t.Errorf("%s carries its own %q beside the $ref: a second definition of a shared field", field, banned)
				}
			}
		})
	}
	token, _ := profileProperty(t, "token")["properties"].(map[string]any)
	for _, field := range []string{"issuer", "audience"} {
		node, _ := token[field].(map[string]any)
		want := "principal.v1.schema.json#/properties/credential/properties/" + field
		if got, _ := node["$ref"].(string); got != want {
			t.Errorf("token.%s $ref = %q, want %q", field, got, want)
		}
	}
}

// Set-equality in BOTH directions between what the profile accepts as a
// credential class and credential-classes.json (plus the documented Wave 1
// platform classes). A one-directional check passes while the two drift: a
// class added to the frozen file and not reachable here would pass an
// "everything accepted is frozen" test.
func TestProfileCredentialClassVocabularyEqualsTheFrozenSetBothWays(t *testing.T) {
	root := testRoot(t)
	frozen := frozenCredentialClassIDs(t)
	want := map[string]bool{}
	for id := range frozen {
		want[id] = true
	}
	for id := range waveOnePlatformCredentialClasses {
		want[id] = true
	}

	candidates := map[string]bool{"bearer_any": true, "jwt": true, "": true, "USER_ACCESS_TOKEN": true}
	for id := range want {
		candidates[id] = true
	}
	for _, id := range principalCredentialClassEnum(t) {
		candidates[id] = true
	}

	accepts := func(class string) bool {
		document := loadEndpointAuthProfileFixture(t, "valid-protected-opaque-class-no-token.json").(map[string]any)
		document["accepted_credential_classes"] = []any{class}
		return Validate(root, EndpointAuthProfileSurface, document) == nil
	}

	var accepted []string
	for id := range candidates {
		if accepts(id) {
			accepted = append(accepted, id)
		}
	}
	sort.Strings(accepted)

	got := map[string]bool{}
	for _, id := range accepted {
		got[id] = true
	}
	for id := range want {
		if !got[id] {
			t.Errorf("class %q is in credential-classes.json or the Wave 1 list but the profile rejects it", id)
		}
	}
	for id := range got {
		if !want[id] {
			t.Errorf("profile accepts class %q which is in neither credential-classes.json nor the Wave 1 list", id)
		}
	}
	if len(got) == 0 {
		t.Fatal("profile accepted no class at all; the comparison is vacuous")
	}
}

func TestEveryProfileObjectIsClosed(t *testing.T) {
	document := profileSchemaDocument(t)
	if document["additionalProperties"] != false {
		t.Error("root profile object is not closed (additionalProperties must be false)")
	}
	for _, name := range []string{"class_disambiguator", "token"} {
		if profileProperty(t, name)["additionalProperties"] != false {
			t.Errorf("%s is not closed (additionalProperties must be false)", name)
		}
	}
}

func TestTheLoaderRefusesAReferenceOutsideTheContractsNamespace(t *testing.T) {
	root := testRoot(t)
	load := localLoader(root)
	for _, raw := range []string{
		"https://example.com/contracts/auth/v1/jsonschema/error.v1.schema.json",
		"http://fullchaos.dev/contracts/auth/v1/jsonschema/error.v1.schema.json",
		"https://fullchaos.dev/contracts/auth/v1/../../../etc/passwd",
		"https://fullchaos.dev/contracts/auth/v1/",
		"file:///etc/passwd",
	} {
		uri, err := url.Parse(raw)
		if err != nil {
			t.Fatalf("parse %s: %v", raw, err)
		}
		if _, err := load(uri); err == nil {
			t.Errorf("loader followed %s; it must refuse everything outside the contracts namespace", raw)
		}
	}
	uri, _ := url.Parse("https://fullchaos.dev/contracts/auth/v1/jsonschema/error.v1.schema.json")
	if _, err := load(uri); err != nil {
		t.Errorf("loader refused an in-namespace schema: %v", err)
	}
}
