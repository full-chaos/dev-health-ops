package contracts

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/google/jsonschema-go/jsonschema"
)

func multiClassProfile(t *testing.T, classes []any, selectorKind, selectorName string) map[string]any {
	t.Helper()
	profile := loadEndpointAuthProfileFixture(t, "valid-multi-class-with-disambiguator.json").(map[string]any)
	profile["accepted_credential_classes"] = classes
	profile["class_disambiguator"] = map[string]any{"kind": selectorKind, "name": selectorName}
	return profile
}

func syntheticRegistry(classes ...RegistryClass) map[string]RegistryClass {
	registry := map[string]RegistryClass{}
	for _, class := range classes {
		registry[class.ClassID] = class
	}
	return registry
}

func realRegistry(t *testing.T) map[string]RegistryClass {
	t.Helper()
	registry, err := LoadRegistryClasses(testRoot(t))
	if err != nil {
		t.Fatal(err)
	}
	return registry
}

func TestSelectorGateAcceptsTheValidMultiClassFixture(t *testing.T) {
	profile := loadEndpointAuthProfileFixture(t, "valid-multi-class-with-disambiguator.json").(map[string]any)
	if err := CheckClassSelector(profile, realRegistry(t)); err != nil {
		t.Fatalf("gate refused the valid multi-class fixture: %v", err)
	}
}

func TestSelectorGateIgnoresASingleClassRoute(t *testing.T) {
	profile := loadEndpointAuthProfileFixture(t, "valid-protected-jwt-single-class.json").(map[string]any)
	if err := CheckClassSelector(profile, realRegistry(t)); err != nil {
		t.Fatalf("gate refused a single-class route: %v", err)
	}
}

func TestSelectorGateRejectsASelectorTwoAcceptedClassesShare(t *testing.T) {
	// The shared Authorization: Bearer marker the schema alone cannot see.
	profile := multiClassProfile(t, []any{"acr_client_credential", "external_push_ingest_token"}, "authorization_scheme", "Bearer")
	if err := CheckClassSelector(profile, realRegistry(t)); err == nil {
		t.Fatal("gate accepted authorization_scheme/Bearer for two classes that both use it")
	}
	shared := ClassMarker{Kind: "dedicated_header", Name: "X-Shared"}
	registry := syntheticRegistry(
		RegistryClass{ClassID: "acr_client_credential", Markers: []ClassMarker{shared}},
		RegistryClass{ClassID: "external_push_ingest_token", Markers: []ClassMarker{shared}},
	)
	profile = multiClassProfile(t, []any{"acr_client_credential", "external_push_ingest_token"}, "dedicated_header", "x-shared")
	if err := CheckClassSelector(profile, registry); err == nil {
		t.Fatal("gate accepted a dedicated_header that both synthetic classes declare (case-insensitive)")
	}
}

func TestSelectorGateRejectsAClassWithNoMarker(t *testing.T) {
	profile := multiClassProfile(t, []any{"acr_client_credential", "ops_refresh_token"}, "credential_prefix", "fcacr_")
	if err := CheckClassSelector(profile, realRegistry(t)); err == nil {
		t.Fatal("gate accepted a route whose second class (ops_refresh_token) has no marker")
	}
	registry := syntheticRegistry(
		RegistryClass{ClassID: "acr_client_credential", Markers: []ClassMarker{{Kind: "credential_prefix", Name: "fcacr_"}}},
		RegistryClass{ClassID: "external_push_ingest_token", MarkersNoneReason: "carried in a request body, not on the head"},
	)
	profile = multiClassProfile(t, []any{"acr_client_credential", "external_push_ingest_token"}, "credential_prefix", "fcacr_")
	if err := CheckClassSelector(profile, registry); err == nil {
		t.Fatal("gate accepted a synthetic class whose markers are empty")
	}
}

func TestSelectorGateRejectsASelectorThatMatchesNoAcceptedClass(t *testing.T) {
	profile := multiClassProfile(t, []any{"acr_client_credential", "external_push_ingest_token"}, "dedicated_header", "X-Web-Assertion")
	if err := CheckClassSelector(profile, realRegistry(t)); err == nil {
		t.Fatal("gate accepted a selector that no accepted class declares")
	}
	profile = multiClassProfile(t, []any{"acr_client_credential", "external_push_ingest_token"}, "credential_prefix", "fcpush")
	if err := CheckClassSelector(profile, realRegistry(t)); err == nil {
		t.Fatal("gate accepted a prefix that is only part of a declared prefix")
	}
}

func TestSelectorGateRejectsAMultiClassRouteWithoutASelector(t *testing.T) {
	profile := loadEndpointAuthProfileFixture(t, "valid-multi-class-with-disambiguator.json").(map[string]any)
	delete(profile, "class_disambiguator")
	if err := CheckClassSelector(profile, realRegistry(t)); err == nil {
		t.Fatal("gate accepted a multi-class route with no class_disambiguator")
	}
}

func TestSelectorGateRejectsAClassMissingFromTheRegistry(t *testing.T) {
	profile := multiClassProfile(t, []any{"acr_client_credential", "not_a_class"}, "credential_prefix", "fcacr_")
	if err := CheckClassSelector(profile, realRegistry(t)); err == nil {
		t.Fatal("gate accepted a class the registry does not define")
	}
}

func TestSelectorGateAcceptsASelectorOneClassOwnsWhileAnotherSharesBearer(t *testing.T) {
	profile := multiClassProfile(t, []any{"acr_client_credential", "external_push_ingest_token"}, "credential_prefix", "fcpush_")
	if err := CheckClassSelector(profile, realRegistry(t)); err != nil {
		t.Fatalf("gate refused a selector that exactly one class owns: %v", err)
	}
}

func registryDocument(t *testing.T) []map[string]any {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join(ContractsDir(testRoot(t)), "credential-classes.json"))
	if err != nil {
		t.Fatal(err)
	}
	var document struct {
		Classes []map[string]any `json:"classes"`
	}
	if err := json.Unmarshal(raw, &document); err != nil {
		t.Fatal(err)
	}
	return document.Classes
}

func TestEveryRegistryClassDeclaresMarkersOrAReasonWhyNot(t *testing.T) {
	registry := realRegistry(t)
	if len(registry) == 0 {
		t.Fatal("registry is empty; the check is vacuous")
	}
	for id, class := range registry {
		switch {
		case len(class.Markers) == 0 && len(class.MarkersNoneReason) < 20:
			t.Errorf("class %s has no markers and no substantive markers_none_reason", id)
		case len(class.Markers) > 0 && class.MarkersNoneReason != "":
			t.Errorf("class %s has markers and a markers_none_reason", id)
		}
	}
}

func registrySchemaResolved(t *testing.T) *jsonschema.Resolved {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join(ContractsDir(testRoot(t)), "credential-classes.schema.json"))
	if err != nil {
		t.Fatal(err)
	}
	var schema jsonschema.Schema
	if err := json.Unmarshal(raw, &schema); err != nil {
		t.Fatal(err)
	}
	resolved, err := schema.Resolve(&jsonschema.ResolveOptions{Loader: localLoader(testRoot(t))})
	if err != nil {
		t.Fatal(err)
	}
	return resolved
}

func TestRegistryValidatesAgainstItsSchemaIncludingMarkers(t *testing.T) {
	resolved := registrySchemaResolved(t)
	raw, err := os.ReadFile(filepath.Join(ContractsDir(testRoot(t)), "credential-classes.json"))
	if err != nil {
		t.Fatal(err)
	}
	var document any
	if err := json.Unmarshal(raw, &document); err != nil {
		t.Fatal(err)
	}
	if err := resolved.Validate(document); err != nil {
		t.Fatalf("credential-classes.json does not satisfy its schema: %v", err)
	}
	mutate := func(edit func(class map[string]any)) any {
		var copyDoc map[string]any
		if err := json.Unmarshal(raw, &copyDoc); err != nil {
			t.Fatal(err)
		}
		classes := copyDoc["classes"].([]any)
		edit(classes[0].(map[string]any))
		return copyDoc
	}
	rejects := map[string]func(class map[string]any){
		"empty markers without a reason": func(c map[string]any) { c["markers"] = []any{}; delete(c, "markers_none_reason") },
		"markers with a reason": func(c map[string]any) {
			c["markers"] = []any{map[string]any{"kind": "dedicated_header", "name": "X-A"}}
			c["markers_none_reason"] = "a reason that must not be here"
		},
		"unknown marker kind": func(c map[string]any) {
			c["markers"] = []any{map[string]any{"kind": "cookie", "name": "sid"}}
			delete(c, "markers_none_reason")
		},
		"markers absent": func(c map[string]any) { delete(c, "markers"); delete(c, "markers_none_reason") },
	}
	for name, edit := range rejects {
		if err := resolved.Validate(mutate(edit)); err == nil {
			t.Errorf("schema accepted a class with %s", name)
		}
	}
}

func TestMarkerKindIsAReferenceToTheProfileSelectorNotACopy(t *testing.T) {
	raw, err := os.ReadFile(filepath.Join(ContractsDir(testRoot(t)), "credential-classes.schema.json"))
	if err != nil {
		t.Fatal(err)
	}
	var registrySchema struct {
		Defs struct {
			Class struct {
				Properties struct {
					Markers struct {
						Items struct {
							Properties struct {
								Kind map[string]any `json:"kind"`
							} `json:"properties"`
						} `json:"items"`
					} `json:"markers"`
				} `json:"properties"`
			} `json:"credentialClass"`
		} `json:"$defs"`
	}
	if err := json.Unmarshal(raw, &registrySchema); err != nil {
		t.Fatal(err)
	}
	kind := registrySchema.Defs.Class.Properties.Markers.Items.Properties.Kind
	const want = "jsonschema/endpoint-auth-profile.v1.schema.json#/properties/class_disambiguator/properties/kind"
	if got, _ := kind["$ref"].(string); got != want {
		t.Fatalf("marker kind $ref = %q, want %q", got, want)
	}
	for _, banned := range []string{"enum", "const", "type", "pattern"} {
		if _, present := kind[banned]; present {
			t.Errorf("marker kind carries its own %q beside the $ref: a second definition of the selector kinds", banned)
		}
	}
}

func TestProfileValidationRunsTheSelectorGate(t *testing.T) {
	root := testRoot(t)
	ambiguous := multiClassProfile(t, []any{"acr_client_credential", "external_push_ingest_token"}, "authorization_scheme", "Bearer")
	if err := Validate(root, EndpointAuthProfileSurface, ambiguous); err != nil {
		t.Fatalf("precondition: the wire schema must accept this profile (the gate is the only check): %v", err)
	}
	if err := ValidateEndpointAuthProfile(root, ambiguous); err == nil {
		t.Fatal("ValidateEndpointAuthProfile accepted a selector two classes share")
	}
	valid := loadEndpointAuthProfileFixture(t, "valid-multi-class-with-disambiguator.json")
	if err := ValidateEndpointAuthProfile(root, valid); err != nil {
		t.Fatalf("ValidateEndpointAuthProfile refused the valid multi-class fixture: %v", err)
	}
	if err := ValidateEndpointAuthProfile(root, "not an object"); err == nil {
		t.Fatal("ValidateEndpointAuthProfile accepted a non-object document")
	}
}

func TestEveryMarkerIsDiscriminatedInItsClassTransport(t *testing.T) {
	counted := 0
	for _, class := range registryDocument(t) {
		transport := class["transport"].(map[string]any)
		head := strings.ToLower(transport["header_or_cookie"].(string))
		shape := strings.ToLower(transport["shape"].(string))
		markers, _ := class["markers"].([]any)
		for _, raw := range markers {
			counted++
			marker := raw.(map[string]any)
			kind, name := marker["kind"].(string), strings.ToLower(marker["name"].(string))
			ok := markerMatchesTransport(kind, name, head, shape)
			if !ok {
				t.Errorf("class %v marker %s/%q is not how its transport text presents the credential (header_or_cookie %q, shape %q)",
					class["class_id"], kind, name, transport["header_or_cookie"], transport["shape"])
			}
		}
	}
	if counted == 0 {
		t.Fatal("no class declares a marker; the check is vacuous")
	}
}

func markerMatchesTransport(kind, name, head, shape string) bool {
	switch kind {
	case "credential_prefix":
		return strings.HasPrefix(shape, name+"<")
	case "authorization_scheme":
		rest, found := strings.CutPrefix(head, "authorization:")
		return found && strings.EqualFold(markerTransportToken(rest), name)
	case "dedicated_header", "signature_header":
		return strings.EqualFold(markerTransportToken(head), name)
	}
	return false
}

func markerTransportToken(text string) string {
	text = strings.TrimSpace(text)
	if end := strings.IndexAny(text, " ,/("); end >= 0 {
		return text[:end]
	}
	return text
}

func TestMarkerTransportCompareIsExact(t *testing.T) {
	cases := []struct {
		kind, name, head string
		want             bool
	}{
		{"dedicated_header", "x-api-key", "x-api-key", true},
		{"dedicated_header", "X-API-Key", "x-api-key", true},
		{"dedicated_header", "x-api-", "x-api-key", false},
		{"signature_header", "x-hub-signature", "x-hub-signature-256", false},
		{"signature_header", "x-hub-signature-256", "x-hub-signature-256", true},
		{"authorization_scheme", "Bearer", "authorization: bearer (by construction)", true},
		{"authorization_scheme", "bear", "authorization: bearer", false},
		{"authorization_scheme", "Bearer", "authorization: bearer/token sent by ops", true},
		{"authorization_scheme", "bearer", "x-authorization: bearer", false},
	}
	for _, c := range cases {
		got := markerMatchesTransport(c.kind, strings.ToLower(c.name), strings.ToLower(c.head), "")
		if got != c.want {
			t.Errorf("%s/%q against %q: got %v, want %v", c.kind, c.name, c.head, got, c.want)
		}
	}
}
