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
								Name map[string]any `json:"name"`
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
	marker := registrySchema.Defs.Class.Properties.Markers.Items.Properties
	for field, node := range map[string]map[string]any{"kind": marker.Kind, "name": marker.Name} {
		want := "jsonschema/endpoint-auth-profile.v1.schema.json#/properties/class_disambiguator/properties/" + field
		if got, _ := node["$ref"].(string); got != want {
			t.Errorf("marker %s $ref = %q, want %q", field, got, want)
		}
		for _, banned := range []string{"enum", "const", "type", "pattern"} {
			if _, present := node[banned]; present {
				t.Errorf("marker %s carries its own %q beside the $ref: a second definition of the selector %s", field, banned, field)
			}
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
	case "jwt_header_alg":
		return strings.EqualFold(markerTransportToken(shape), name)
	case "jwt_header_typ":
		_, rest, found := strings.Cut(shape, "typ ")
		return found && markerTransportToken(rest) == name
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
		kind, name, head, shape string
		want                    bool
	}{
		{"dedicated_header", "x-api-key", "x-api-key", "", true},
		{"dedicated_header", "X-API-Key", "x-api-key", "", true},
		{"dedicated_header", "x-api-", "x-api-key", "", false},
		{"signature_header", "x-hub-signature", "x-hub-signature-256", "", false},
		{"signature_header", "x-hub-signature-256", "x-hub-signature-256", "", true},
		{"authorization_scheme", "Bearer", "authorization: bearer (by construction)", "", true},
		{"authorization_scheme", "bear", "authorization: bearer", "", false},
		{"authorization_scheme", "Bearer", "authorization: bearer/token sent by ops", "", true},
		{"authorization_scheme", "bearer", "x-authorization: bearer", "", false},
		{"jwt_header_alg", "HS256", "", "hs256 jwt, type=access", true},
		{"jwt_header_alg", "EdDSA", "", "eddsa/ed25519 jwt, kid header", true},
		{"jwt_header_alg", "HS256", "", "xhs256 jwt, type=access", false},
		{"jwt_header_alg", "HS25", "", "hs256 jwt, type=access", false},
		{"jwt_header_typ", "at+jwt", "", "eddsa jws, header {alg eddsa, typ at+jwt, kid}", true},
		{"jwt_header_typ", "jwt", "", "eddsa/ed25519 jwt, header {alg eddsa, typ jwt, kid}", true},
		{"jwt_header_typ", "jwt", "", "eddsa jws, header {alg eddsa, typ at+jwt, kid}", false},
		{"jwt_header_typ", "at+jwt", "", "eddsa jws, header {alg eddsa, typ at+jwtx, kid}", false},
		{"jwt_header_typ", "at+jwt", "", "eddsa jws, at+jwt", false},
	}
	for _, c := range cases {
		got := markerMatchesTransport(c.kind, strings.ToLower(c.name), strings.ToLower(c.head), strings.ToLower(c.shape))
		if got != c.want {
			t.Errorf("%s/%q against %q / %q: got %v, want %v", c.kind, c.name, c.head, c.shape, got, c.want)
		}
	}
}

func TestSelectorGateSeparatesTheTwoBearerJWTClassesByHeaderAlg(t *testing.T) {
	pair := []any{"effective_principal_envelope", "ops_access_token_hs256"}
	registry := realRegistry(t)
	shared := multiClassProfile(t, pair, "authorization_scheme", "Bearer")
	if err := CheckClassSelector(shared, registry); err == nil {
		t.Fatal("gate accepted authorization_scheme/Bearer for two classes that both ride a Bearer header")
	}
	for _, alg := range []string{"HS256", "EdDSA"} {
		profile := multiClassProfile(t, pair, "jwt_header_alg", alg)
		if err := CheckClassSelector(profile, registry); err != nil {
			t.Errorf("gate refused jwt_header_alg/%s for envelope vs ops access token: %v", alg, err)
		}
	}
	wrongCase := multiClassProfile(t, pair, "jwt_header_alg", "hs256")
	if err := CheckClassSelector(wrongCase, registry); err == nil {
		t.Fatal("gate matched a JWT alg case-insensitively; the header alg is compared exactly")
	}
}

func TestTokensKeySetEqualsTheAcceptedPinnedClassesInTheSchema(t *testing.T) {
	root := testRoot(t)
	valid := loadEndpointAuthProfileFixture(t, "valid-multi-class-envelope-vs-ops-token.json").(map[string]any)
	if err := Validate(root, EndpointAuthProfileSurface, valid); err != nil {
		t.Fatalf("the schema refused the valid two-algorithm fixture: %v", err)
	}
	tokens := valid["tokens"].(map[string]any)
	mutate := func(edit func(map[string]any)) map[string]any {
		doc := loadEndpointAuthProfileFixture(t, "valid-multi-class-envelope-vs-ops-token.json").(map[string]any)
		edit(doc["tokens"].(map[string]any))
		return doc
	}
	cases := map[string]map[string]any{
		"missing key": mutate(func(m map[string]any) { delete(m, "ops_access_token_hs256") }),
		"extra key":   mutate(func(m map[string]any) { m["user_access_token"] = tokens["ops_access_token_hs256"] }),
		"renamed key": mutate(func(m map[string]any) {
			m["ops_refresh_token"] = m["ops_access_token_hs256"]
			delete(m, "ops_access_token_hs256")
		}),
	}
	for name, doc := range cases {
		if err := Validate(root, EndpointAuthProfileSurface, doc); err == nil {
			t.Errorf("%s: the schema accepted a tokens key set that differs from the accepted pinned classes", name)
		}
	}
}

func TestSelectorGateSeparatesTheEnvelopeAndTheUserTokenByHeaderTyp(t *testing.T) {
	pair := []any{"effective_principal_envelope", "user_access_token"}
	registry := realRegistry(t)
	for _, shared := range [][2]string{{"authorization_scheme", "Bearer"}, {"jwt_header_alg", "EdDSA"}} {
		profile := multiClassProfile(t, pair, shared[0], shared[1])
		if err := CheckClassSelector(profile, registry); err == nil {
			t.Errorf("gate accepted %s/%s, which both classes carry", shared[0], shared[1])
		}
	}
	for _, typ := range []string{"at+jwt", "jwt"} {
		profile := multiClassProfile(t, pair, "jwt_header_typ", typ)
		if err := CheckClassSelector(profile, registry); err != nil {
			t.Errorf("gate refused jwt_header_typ/%s for the envelope vs the user access token: %v", typ, err)
		}
	}
	for _, typ := range []string{"JWT", "AT+JWT", "application/at+jwt"} {
		profile := multiClassProfile(t, pair, "jwt_header_typ", typ)
		if err := CheckClassSelector(profile, registry); err == nil {
			t.Errorf("gate matched jwt_header_typ/%s; a typ marker is compared exactly against its canonical form", typ)
		}
	}
}

func TestTheProfileSchemaAcceptsAJWTHeaderTypSelector(t *testing.T) {
	root := testRoot(t)
	profile := loadEndpointAuthProfileFixture(t, "valid-multi-class-envelope-vs-user-token-by-typ.json")
	if err := Validate(root, EndpointAuthProfileSurface, profile); err != nil {
		t.Fatalf("the schema refused a jwt_header_typ selector: %v", err)
	}
	if err := ValidateEndpointAuthProfile(root, profile); err != nil {
		t.Fatalf("the profile gate refused the typ-separated pair: %v", err)
	}
}

func pinnedProfile(t *testing.T, class, algorithm, tokenType string) map[string]any {
	t.Helper()
	profile := loadEndpointAuthProfileFixture(t, "valid-protected-jwt-single-class.json").(map[string]any)
	profile["accepted_credential_classes"] = []any{class}
	token := profile["token"].(map[string]any)
	token["algorithm"] = algorithm
	token["token_type"] = tokenType
	return profile
}

func TestTokenPinGateHoldsEachPinToItsClassMarkers(t *testing.T) {
	registry := syntheticRegistry(
		RegistryClass{ClassID: "user_access_token", Markers: []ClassMarker{
			{Kind: "authorization_scheme", Name: "Bearer"},
			{Kind: "jwt_header_alg", Name: "EdDSA"},
			{Kind: "jwt_header_typ", Name: "at+jwt"},
		}},
		RegistryClass{ClassID: "ops_access_token_hs256", Markers: []ClassMarker{
			{Kind: "authorization_scheme", Name: "Bearer"},
			{Kind: "jwt_header_alg", Name: "HS256"},
		}},
	)
	cases := []struct {
		name                  string
		class, algorithm, typ string
		wantRefusal           string
	}{
		{"both pins match", "user_access_token", "EdDSA", "at+jwt", ""},
		{"typ differs", "user_access_token", "EdDSA", "jwt", `pins token_type "jwt"`},
		{"typ differs in case only", "user_access_token", "EdDSA", "AT+JWT", `pins token_type "AT+JWT"`},
		{"alg differs", "user_access_token", "HS256", "at+jwt", `pins algorithm "HS256"`},
		{"class with no typ marker", "ops_access_token_hs256", "HS256", "jwt", ""},
		{"class with no typ marker, alg differs", "ops_access_token_hs256", "EdDSA", "jwt", `pins algorithm "EdDSA"`},
		{"class absent from the registry", "workload_token", "EdDSA", "at+jwt", ""},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			err := CheckTokenPins(pinnedProfile(t, c.class, c.algorithm, c.typ), registry)
			if c.wantRefusal == "" {
				if err != nil {
					t.Fatalf("refused: %v", err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), c.wantRefusal) {
				t.Fatalf("got %v, want a refusal containing %q", err, c.wantRefusal)
			}
		})
	}
}

func TestTokenPinGateReadsEveryTokensBlockUnderItsOwnKey(t *testing.T) {
	registry := realRegistry(t)
	valid := loadEndpointAuthProfileFixture(t, "valid-multi-class-envelope-vs-user-token-by-typ.json").(map[string]any)
	if err := CheckTokenPins(valid, registry); err != nil {
		t.Fatalf("refused the valid typ-separated pair: %v", err)
	}
	for _, class := range []string{"effective_principal_envelope", "user_access_token"} {
		doc := loadEndpointAuthProfileFixture(t, "valid-multi-class-envelope-vs-user-token-by-typ.json").(map[string]any)
		doc["tokens"].(map[string]any)[class].(map[string]any)["algorithm"] = "HS256"
		err := CheckTokenPins(doc, registry)
		if err == nil || !strings.Contains(err.Error(), `class "`+class+`" pins algorithm "HS256"`) {
			t.Errorf("tokens.%s with algorithm HS256: got %v", class, err)
		}
	}
}

func TestUserAccessTokenTypMarkerIsTheAccessTokenSchemaTyp(t *testing.T) {
	raw, err := os.ReadFile(SchemaPath(testRoot(t), AccessTokenSurface))
	if err != nil {
		t.Fatal(err)
	}
	var schema struct {
		Properties struct {
			Header struct {
				Properties struct {
					Typ struct {
						Const string `json:"const"`
					} `json:"typ"`
					Alg struct {
						Const string `json:"const"`
					} `json:"alg"`
				} `json:"properties"`
			} `json:"header"`
		} `json:"properties"`
	}
	if err := json.Unmarshal(raw, &schema); err != nil {
		t.Fatal(err)
	}
	header := schema.Properties.Header.Properties
	if header.Typ.Const == "" || header.Alg.Const == "" {
		t.Fatalf("access-token.v1 header typ/alg const not found (typ %q, alg %q); this test's path into the schema is stale", header.Typ.Const, header.Alg.Const)
	}
	class := realRegistry(t)["user_access_token"]
	// typ is a media type, compared without case on the wire (RFC 7515
	// section 4.1.9); the contract files carry one lowercase spelling.
	if got := markerNames(class, "jwt_header_typ"); len(got) != 1 || !strings.EqualFold(got[0], header.Typ.Const) || got[0] != strings.ToLower(got[0]) {
		t.Errorf("user_access_token jwt_header_typ markers = %v, want exactly one lowercase spelling of %q (access-token.v1 header)", got, header.Typ.Const)
	}
	if got := markerNames(class, "jwt_header_alg"); len(got) != 1 || got[0] != header.Alg.Const {
		t.Errorf("user_access_token jwt_header_alg markers = %v, want exactly [%s] (access-token.v1 header)", got, header.Alg.Const)
	}
}

func markerNames(class RegistryClass, kind string) []string {
	var names []string
	for _, marker := range class.Markers {
		if marker.Kind == kind {
			names = append(names, marker.Name)
		}
	}
	return names
}
