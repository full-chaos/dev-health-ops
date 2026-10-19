package contracts

import (
	"encoding/json"
	"net/url"
	"os"
	"path/filepath"
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
		"class_disambiguator/kind":          "../endpoint-profile.schema.json#/$defs/endpointProfile/properties/class_disambiguator/properties/kind",
		"minimum_assurance":                 "principal.v1.schema.json#/properties/authentication/properties/assurance",
		"action":                            "principal.v1.schema.json#/properties/actor_chain/items/properties/permitted_actions/items",
		"accepted_credential_classes/items": "principal.v1.schema.json#/properties/credential/properties/class",
	}
	for field, wantRef := range shared {
		t.Run(field, func(t *testing.T) {
			var node map[string]any
			if field == "accepted_credential_classes/items" {
				node, _ = profileProperty(t, "accepted_credential_classes")["items"].(map[string]any)
			} else if field == "class_disambiguator/kind" {
				node, _ = profileProperty(t, "class_disambiguator")["properties"].(map[string]any)["kind"].(map[string]any)
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

// profileClassLists reads the two class lists the schema carries beside the
// $ref'd vocabulary: the classes whose token pins are required (the first
// allOf branch's contains.enum) and the signed classes the profile cannot yet
// describe (items.not.enum). Both come from the schema, never restated here.
func profileClassLists(t *testing.T) (pinned, unexpressible []string) {
	t.Helper()
	var document struct {
		AllOf []struct {
			If struct {
				Properties struct {
					Classes struct {
						Contains struct {
							Enum []string `json:"enum"`
						} `json:"contains"`
					} `json:"accepted_credential_classes"`
				} `json:"properties"`
			} `json:"if"`
		} `json:"allOf"`
		Properties struct {
			Classes struct {
				Items struct {
					Not struct {
						Enum []string `json:"enum"`
					} `json:"not"`
				} `json:"items"`
			} `json:"accepted_credential_classes"`
		} `json:"properties"`
	}
	raw, err := os.ReadFile(SchemaPath(testRoot(t), EndpointAuthProfileSurface))
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(raw, &document); err != nil {
		t.Fatal(err)
	}
	for _, rule := range document.AllOf {
		pinned = append(pinned, rule.If.Properties.Classes.Contains.Enum...)
	}
	unexpressible = document.Properties.Classes.Items.Not.Enum
	if len(pinned) == 0 || len(unexpressible) == 0 {
		t.Fatalf("found pinned=%v unexpressible=%v; the schema lost a list or this test's path into it is stale", pinned, unexpressible)
	}
	return pinned, unexpressible
}

// Set-equality in BOTH directions between what the profile accepts as a
// credential class and credential-classes.json (plus the documented Wave 1
// platform classes), less the signed classes the profile refuses by name. A
// one-directional check passes while the two drift. Each probe carries token
// pins exactly when the class is a pinned one, so a rejection is the class
// vocabulary's and not the token rule's.
func TestProfileCredentialClassVocabularyEqualsTheFrozenSetBothWays(t *testing.T) {
	root := testRoot(t)
	pinned, unexpressible := profileClassLists(t)
	isPinned := map[string]bool{}
	for _, id := range pinned {
		isPinned[id] = true
	}
	refused := map[string]bool{}
	for _, id := range unexpressible {
		refused[id] = true
	}
	frozen := frozenCredentialClassIDs(t)
	want := map[string]bool{}
	for id := range frozen {
		want[id] = true
	}
	for id := range waveOnePlatformCredentialClasses {
		want[id] = true
	}
	for id := range refused {
		if !want[id] {
			t.Errorf("refused class %q is not in the vocabulary", id)
		}
		if isPinned[id] {
			t.Errorf("class %q is both pinned and refused", id)
		}
		delete(want, id)
	}
	for id := range isPinned {
		if !frozen[id] && waveOnePlatformCredentialClasses[id] == "" {
			t.Errorf("pinned class %q is not in the vocabulary", id)
		}
	}

	candidates := map[string]bool{"bearer_any": true, "jwt": true, "": true, "USER_ACCESS_TOKEN": true}
	for id := range frozen {
		candidates[id] = true
	}
	for id := range waveOnePlatformCredentialClasses {
		candidates[id] = true
	}
	for _, id := range principalCredentialClassEnum(t) {
		candidates[id] = true
	}

	accepts := func(class string) bool {
		document := loadEndpointAuthProfileFixture(t, "valid-protected-opaque-class-no-token.json").(map[string]any)
		document["accepted_credential_classes"] = []any{class}
		if isPinned[class] {
			document["token"] = loadEndpointAuthProfileFixture(t, "valid-protected-jwt-single-class.json").(map[string]any)["token"]
		}
		return Validate(root, EndpointAuthProfileSurface, document) == nil
	}
	got := map[string]bool{}
	for id := range candidates {
		if accepts(id) {
			got[id] = true
		}
	}
	for id := range want {
		if !got[id] {
			t.Errorf("class %q is in the vocabulary and not refused by name, but the profile rejects it", id)
		}
	}
	for id := range got {
		if !want[id] {
			t.Errorf("profile accepts class %q which is outside the vocabulary or refused by name", id)
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

func TestTheLoaderRefusesASymlinkThatEscapesTheContractsDirectory(t *testing.T) {
	outside := t.TempDir()
	if err := os.WriteFile(filepath.Join(outside, "foreign.schema.json"), []byte(`{"required":["outsideMarker"]}`), 0o644); err != nil {
		t.Fatal(err)
	}
	root := t.TempDir()
	schemas := filepath.Join(ContractsDir(root), "jsonschema")
	if err := os.MkdirAll(schemas, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(filepath.Join(outside, "foreign.schema.json"), filepath.Join(schemas, "foreign.schema.json")); err != nil {
		t.Skipf("cannot create a symlink here: %v", err)
	}
	uri, _ := url.Parse("https://fullchaos.dev/contracts/auth/v1/jsonschema/foreign.schema.json")
	if _, err := localLoader(root)(uri); err == nil {
		t.Fatal("loader followed a symlink out of the contracts directory")
	}
	real := filepath.Join(schemas, "real.schema.json")
	if err := os.WriteFile(real, []byte(`{}`), 0o644); err != nil {
		t.Fatal(err)
	}
	uri, _ = url.Parse("https://fullchaos.dev/contracts/auth/v1/jsonschema/real.schema.json")
	if _, err := localLoader(root)(uri); err != nil {
		t.Fatalf("loader refused an ordinary in-tree file: %v", err)
	}
}

func TestTheTokenRuleCoversEveryAcceptedClassInBothDirections(t *testing.T) {
	// A pinned class must carry pins and an unpinned one must not. Probe every
	// accepted class both ways: pins on an unpinned class and no pins on a
	// pinned one are both rejected.
	root := testRoot(t)
	pinned, unexpressible := profileClassLists(t)
	isPinned := map[string]bool{}
	for _, id := range pinned {
		isPinned[id] = true
	}
	refused := map[string]bool{}
	for _, id := range unexpressible {
		refused[id] = true
	}
	pins := loadEndpointAuthProfileFixture(t, "valid-protected-jwt-single-class.json").(map[string]any)["token"]
	checked := 0
	for id := range frozenCredentialClassIDs(t) {
		if refused[id] {
			continue
		}
		for _, withPins := range []bool{true, false} {
			document := loadEndpointAuthProfileFixture(t, "valid-protected-opaque-class-no-token.json").(map[string]any)
			document["accepted_credential_classes"] = []any{id}
			if withPins {
				document["token"] = pins
			}
			err := Validate(root, EndpointAuthProfileSurface, document)
			if wantOK := withPins == isPinned[id]; (err == nil) != wantOK {
				t.Errorf("class %q withPins=%v: accepted=%v, want %v (%v)", id, withPins, err == nil, wantOK, err)
			}
			checked++
		}
	}
	if checked == 0 {
		t.Fatal("probed nothing")
	}
}
