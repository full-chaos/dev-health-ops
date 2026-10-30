package contracts

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
)

// bearerClasses are the credential classes carried as an Authorization bearer
// token: a row accepting one must declare a bearer security requirement.
var bearerClasses = map[string]bool{"ops_access_token_hs256": true}

func loadOpenAPIRaw(t *testing.T) map[string]any {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join(ContractsDir(testRoot(t)), "openapi", "auth-v1.openapi.json"))
	if err != nil {
		t.Fatal(err)
	}
	var document map[string]any
	if err := json.Unmarshal(raw, &document); err != nil {
		t.Fatal(err)
	}
	return document
}

// collectRefs returns every "$ref" string under node.
func collectRefs(node any, out *[]string) {
	switch value := node.(type) {
	case map[string]any:
		for key, child := range value {
			if ref, ok := child.(string); ok && key == "$ref" {
				*out = append(*out, ref)
			}
			collectRefs(child, out)
		}
	case []any:
		for _, child := range value {
			collectRefs(child, out)
		}
	}
}

// transportStatuses are the answers the shared transport writes before a
// route runs, in transport-error.v1, never error.v1.
var transportStatuses = []string{"400", "413", "429"}

const transportErrorRef = "#/components/schemas/TransportError"

// compatPathPrefix is go-api's own path space. auth-service serves routes in
// it with go-api's wire (the go-api proxy forwards them unchanged), so
// auth-v1's OpenAPI describes none of them: go-api's contract is theirs.
const compatPathPrefix = "/api/v1/auth/"

// internalPathPrefix is go-api's internal call space: POST /internal/v1/sessions
// is the session issue call go-api makes with its service credential, never a
// browser or SDK surface, so auth-v1's public OpenAPI describes none of it.
const internalPathPrefix = "/internal/v1/"

// oneOfHasTransportError: a transport status may also carry a route's own refusal, declared as a oneOf whose
// first member is the transport envelope.
func oneOfHasTransportError(schema map[string]any) bool {
	members, _ := schema["oneOf"].([]any)
	if len(members) == 0 {
		return false
	}
	first, _ := members[0].(map[string]any)
	return first["$ref"] == transportErrorRef
}

// uncheckedKeywords are schema keywords the extra-member check does not read; a member that uses one fails closed.
var uncheckedKeywords = []string{"$ref", "oneOf", "anyOf", "allOf", "not", "if", "patternProperties", "additionalItems", "prefixItems"}

// closedSchemaProblems walks an inline schema: every object (type object, or one that lists properties) must set
// additionalProperties to false, and no keyword the walk cannot read may appear.
func closedSchemaProblems(schema map[string]any, path string) []string {
	var problems []string
	for _, keyword := range uncheckedKeywords {
		if _, present := schema[keyword]; present {
			problems = append(problems, fmt.Sprintf("%s uses %q, which the gate does not check", path, keyword))
		}
	}
	properties, hasProperties := schema["properties"].(map[string]any)
	if schema["type"] == "object" || hasProperties {
		if schema["additionalProperties"] != false {
			problems = append(problems, fmt.Sprintf("%s is open (additionalProperties is not false)", path))
		}
	}
	names := make([]string, 0, len(properties))
	for name := range properties {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		if child, ok := properties[name].(map[string]any); ok {
			problems = append(problems, closedSchemaProblems(child, path+"/properties/"+name)...)
		}
	}
	if items, ok := schema["items"].(map[string]any); ok {
		problems = append(problems, closedSchemaProblems(items, path+"/items")...)
	}
	return problems
}

// oneOfExtraMemberProblems checks every member of a transport status oneOf after the envelope: an inline object
// schema, closed at every level, and disjoint from the envelope. The envelope is closed and requires "error", so a
// closed member without an "error" property cannot match the same body.
func oneOfExtraMemberProblems(key, status string, schema map[string]any) []string {
	members, _ := schema["oneOf"].([]any)
	var problems []string
	for index, raw := range members {
		if index == 0 {
			continue
		}
		path := fmt.Sprintf("OpenAPI operation %s: its %s oneOf member %d", key, status, index)
		member, ok := raw.(map[string]any)
		if !ok || member["type"] != "object" || member["$ref"] != nil {
			problems = append(problems, path+" must be an inline object schema")
			continue
		}
		problems = append(problems, closedSchemaProblems(member, path)...)
		properties, _ := member["properties"].(map[string]any)
		required, _ := member["required"].([]any)
		_, namesError := properties["error"]
		for _, name := range required {
			if name == "error" {
				namesError = true
			}
		}
		if namesError {
			problems = append(problems, path+" names \"error\" and can match the transport envelope")
		}
	}
	return problems
}

// openAPIPathProblems checks the OpenAPI document against the auth-service API
// rows: one operation per API row outside go-api's path space and no other,
// every $ref inside an operation names an existing component schema, and
// every security requirement names an existing security scheme.
func openAPIPathProblems(document map[string]any, rows []map[string]any) []string {
	var problems []string
	want := map[string]bool{}
	bearer := map[string]bool{}
	for _, row := range rows {
		id, _ := row["id"].(string)
		route, _ := row["route"].(string)
		if !strings.HasSuffix(id, "@api]") || strings.HasPrefix(route, compatPathPrefix) || strings.HasPrefix(route, internalPathPrefix) {
			continue
		}
		key := strings.ToLower(row["method"].(string)) + " " + row["route"].(string)
		want[key] = true
		classes, _ := row["accepted_credential_classes"].([]any)
		for _, class := range classes {
			if bearerClasses[class.(string)] {
				bearer[key] = true
			}
		}
	}
	components, _ := document["components"].(map[string]any)
	schemas, _ := components["schemas"].(map[string]any)
	schemes, _ := components["securitySchemes"].(map[string]any)
	paths, _ := document["paths"].(map[string]any)
	got := map[string]bool{}
	for path, item := range paths {
		operations, _ := item.(map[string]any)
		for method, op := range operations {
			key := method + " " + path
			got[key] = true
			if !want[key] {
				problems = append(problems, fmt.Sprintf("OpenAPI operation %s has no auth-service API profile row", key))
			}
			var refs []string
			collectRefs(op, &refs)
			for _, ref := range refs {
				name, ok := strings.CutPrefix(ref, "#/components/schemas/")
				if !ok || schemas[name] == nil {
					problems = append(problems, fmt.Sprintf("OpenAPI operation %s references %q, which is not a component schema", key, ref))
				}
			}
			operation, _ := op.(map[string]any)
			security, _ := operation["security"].([]any)
			bearerScheme := false
			for _, requirement := range security {
				names, _ := requirement.(map[string]any)
				for name := range names {
					scheme, _ := schemes[name].(map[string]any)
					if scheme == nil {
						problems = append(problems, fmt.Sprintf("OpenAPI operation %s requires security scheme %q, which is not defined", key, name))
						continue
					}
					if scheme["type"] == "http" && scheme["scheme"] == "bearer" {
						bearerScheme = true
					}
				}
			}
			if bearer[key] && !bearerScheme {
				problems = append(problems, fmt.Sprintf("OpenAPI operation %s accepts a bearer credential class in its profile row but declares no bearer security requirement", key))
			}
			if len(refs) == 0 && method != "head" {
				problems = append(problems, fmt.Sprintf("OpenAPI operation %s references no schema", key))
			}
			responses, _ := operation["responses"].(map[string]any)
			if method == "head" {
				for status, answer := range responses {
					if body, _ := answer.(map[string]any); body["content"] != nil {
						problems = append(problems, fmt.Sprintf("OpenAPI operation %s: its %s declares a body, and a HEAD answer carries none", key, status))
					}
				}
			}
			for _, status := range transportStatuses {
				response, declared := responses[status].(map[string]any)
				if !declared || method == "head" {
					continue
				}
				content, _ := response["content"].(map[string]any)
				body, _ := content["application/json"].(map[string]any)
				schema, _ := body["schema"].(map[string]any)
				if schema["$ref"] != transportErrorRef && !oneOfHasTransportError(schema) {
					problems = append(problems, fmt.Sprintf("OpenAPI operation %s: its %s is the transport's answer and must reference %s", key, status, transportErrorRef))
				}
				problems = append(problems, oneOfExtraMemberProblems(key, status, schema)...)
			}
		}
	}
	for key := range want {
		if !got[key] {
			problems = append(problems, fmt.Sprintf("auth-service API row %s has no OpenAPI operation", key))
		}
	}
	sort.Strings(problems)
	return problems
}

func TestEveryAuthServiceAPIRouteHasAResolvingOpenAPIOperation(t *testing.T) {
	rows := authServiceRows(t)
	for _, problem := range openAPIPathProblems(loadOpenAPIRaw(t), rows) {
		t.Error(problem)
	}
	if len(loadOpenAPIRaw(t)["paths"].(map[string]any)) == 0 {
		t.Fatal("the OpenAPI document has no paths: the check measured nothing")
	}
}

func TestTheOpenAPIPathGateRefusesPlantedMismatches(t *testing.T) {
	rows := authServiceRows(t)
	cases := map[string]func(document map[string]any){
		"an operation is removed": func(document map[string]any) {
			delete(document["paths"].(map[string]any), "/v1/sessions/revoke")
		},
		"a response references a missing schema": func(document map[string]any) {
			op := document["paths"].(map[string]any)["/v1/sessions/current"].(map[string]any)["get"].(map[string]any)
			op["responses"].(map[string]any)["200"].(map[string]any)["content"].(map[string]any)["application/json"].(map[string]any)["schema"] = map[string]any{"$ref": "#/components/schemas/NoSuchSchema"}
		},
		"a request body references a missing schema": func(document map[string]any) {
			op := document["paths"].(map[string]any)["/v1/token/refresh"].(map[string]any)["post"].(map[string]any)
			op["requestBody"].(map[string]any)["content"].(map[string]any)["application/json"].(map[string]any)["schema"] = map[string]any{"$ref": "#/components/schemas/Missing"}
		},
		"a security requirement names a missing scheme": func(document map[string]any) {
			op := document["paths"].(map[string]any)["/v1/sessions/revoke-all"].(map[string]any)["post"].(map[string]any)
			op["security"] = []any{map[string]any{"noSuchScheme": []any{}}}
		},
		"a protected operation loses its security requirement": func(document map[string]any) {
			op := document["paths"].(map[string]any)["/v1/sessions/current"].(map[string]any)["get"].(map[string]any)
			delete(op, "security")
		},
		"a protected operation names a scheme that is not a bearer": func(document map[string]any) {
			document["components"].(map[string]any)["securitySchemes"].(map[string]any)["apiKeyOnly"] = map[string]any{"type": "apiKey", "in": "header", "name": "X-Key"}
			op := document["paths"].(map[string]any)["/v1/sessions/revoke"].(map[string]any)["post"].(map[string]any)
			op["security"] = []any{map[string]any{"apiKeyOnly": []any{}}}
		},
		"a HEAD answer declares a body": func(document map[string]any) {
			op := document["paths"].(map[string]any)["/.well-known/jwks.json"].(map[string]any)["head"].(map[string]any)
			op["responses"].(map[string]any)["200"].(map[string]any)["content"] = map[string]any{"application/json": map[string]any{"schema": map[string]any{"$ref": "#/components/schemas/Jwks"}}}
		},
		"an operation no row profiles": func(document map[string]any) {
			document["paths"].(map[string]any)["/v1/extra"] = map[string]any{"get": map[string]any{"responses": map[string]any{"200": map[string]any{"description": "x", "content": map[string]any{"application/json": map[string]any{"schema": map[string]any{"$ref": "#/components/schemas/Session"}}}}}}}
		},
		"a transport 429 loses its envelope schema": func(document map[string]any) {
			op := document["paths"].(map[string]any)["/v1/token/refresh"].(map[string]any)["post"].(map[string]any)
			delete(op["responses"].(map[string]any)["429"].(map[string]any), "content")
		},
		"a transport 413 names error.v1": func(document map[string]any) {
			op := document["paths"].(map[string]any)["/v1/sessions/revoke"].(map[string]any)["post"].(map[string]any)
			op["responses"].(map[string]any)["413"].(map[string]any)["content"] = map[string]any{"application/json": map[string]any{"schema": map[string]any{"$ref": "#/components/schemas/Error"}}}
		},
		"a transport 400 loses its envelope schema": func(document map[string]any) {
			op := document["paths"].(map[string]any)["/v1/sessions/current"].(map[string]any)["get"].(map[string]any)
			delete(op["responses"].(map[string]any)["400"].(map[string]any), "content")
		},
		"a transport 400 oneOf without the envelope first": func(document map[string]any) {
			op := document["paths"].(map[string]any)["/v1/identities/social"].(map[string]any)["post"].(map[string]any)
			schema := op["responses"].(map[string]any)["400"].(map[string]any)["content"].(map[string]any)["application/json"].(map[string]any)["schema"].(map[string]any)
			members := schema["oneOf"].([]any)
			schema["oneOf"] = []any{members[1], members[0]}
		},
		"a transport 400 oneOf with no envelope": func(document map[string]any) {
			op := document["paths"].(map[string]any)["/v1/identities/social"].(map[string]any)["post"].(map[string]any)
			schema := op["responses"].(map[string]any)["400"].(map[string]any)["content"].(map[string]any)["application/json"].(map[string]any)["schema"].(map[string]any)
			schema["oneOf"] = schema["oneOf"].([]any)[1:]
		},
		"an operation in go-api's internal path space": func(document map[string]any) {
			document["paths"].(map[string]any)["/internal/v1/sessions"] = map[string]any{"post": map[string]any{"responses": map[string]any{"200": map[string]any{"description": "x", "content": map[string]any{"application/json": map[string]any{"schema": map[string]any{"$ref": "#/components/schemas/Session"}}}}}}}
		},
		"an operation in go-api's path space": func(document map[string]any) {
			document["paths"].(map[string]any)["/api/v1/auth/login"] = map[string]any{"post": map[string]any{"responses": map[string]any{"200": map[string]any{"description": "x", "content": map[string]any{"application/json": map[string]any{"schema": map[string]any{"$ref": "#/components/schemas/Session"}}}}}}}
		},
	}
	for name, plant := range cases {
		document := loadOpenAPIRaw(t)
		plant(document)
		if len(openAPIPathProblems(document, rows)) == 0 {
			t.Errorf("%s: the gate reported nothing", name)
		}
	}
}

// closedGoAPIMember is a go-api refusal member as the document declares it for POST /v1/identities/social: closed
// at both levels and without a property of the transport envelope.
func closedGoAPIMember() map[string]any {
	return map[string]any{
		"type": "object", "required": []any{"detail"}, "additionalProperties": false,
		"properties": map[string]any{"detail": map[string]any{
			"type": "object", "required": []any{"message"}, "additionalProperties": false,
			"properties": map[string]any{"message": map[string]any{"const": "x"}},
		}},
	}
}

// plantOneOf declares the status of an operation as a oneOf of the transport envelope and members.
func plantOneOf(document map[string]any, route, method, status string, members ...any) {
	op := document["paths"].(map[string]any)[route].(map[string]any)[method].(map[string]any)
	response := op["responses"].(map[string]any)[status].(map[string]any)
	response["content"] = map[string]any{"application/json": map[string]any{"schema": map[string]any{
		"oneOf": append([]any{map[string]any{"$ref": transportErrorRef}}, members...),
	}}}
}

// The extra members of a transport status oneOf are checked: each is an inline object, closed at every level,
// and disjoint from the envelope. A closed disjoint member passes.
func TestTheOpenAPIPathGateChecksTheOneOfExtraMembers(t *testing.T) {
	rows := authServiceRows(t)
	cases := map[string]struct {
		plant     func(document map[string]any)
		wantProbs bool
	}{
		"an extra member open at the top": {func(d map[string]any) {
			m := closedGoAPIMember()
			delete(m, "additionalProperties")
			plantOneOf(d, "/v1/identities/social", "post", "400", m)
		}, true},
		"an extra member open at a nested level": {func(d map[string]any) {
			m := closedGoAPIMember()
			delete(m["properties"].(map[string]any)["detail"].(map[string]any), "additionalProperties")
			plantOneOf(d, "/v1/identities/social", "post", "400", m)
		}, true},
		"an extra member that is open to true": {func(d map[string]any) {
			m := closedGoAPIMember()
			m["additionalProperties"] = true
			plantOneOf(d, "/v1/identities/social", "post", "400", m)
		}, true},
		"an extra member that names the envelope's error": {func(d map[string]any) {
			m := closedGoAPIMember()
			m["properties"].(map[string]any)["error"] = map[string]any{"type": "object", "additionalProperties": false}
			plantOneOf(d, "/v1/identities/social", "post", "400", m)
		}, true},
		"an extra member that is a ref": {func(d map[string]any) {
			plantOneOf(d, "/v1/identities/social", "post", "400", map[string]any{"$ref": "#/components/schemas/Error"})
		}, true},
		"an extra member that is a string schema": {func(d map[string]any) {
			plantOneOf(d, "/v1/identities/social", "post", "400", map[string]any{"type": "string"})
		}, true},
		"an extra member that nests a oneOf the gate cannot read": {func(d map[string]any) {
			m := closedGoAPIMember()
			m["properties"].(map[string]any)["detail"].(map[string]any)["oneOf"] = []any{map[string]any{"type": "string"}}
			plantOneOf(d, "/v1/identities/social", "post", "400", m)
		}, true},
		"an open extra member on a transport 413": {func(d map[string]any) {
			m := closedGoAPIMember()
			delete(m, "additionalProperties")
			plantOneOf(d, "/v1/sessions/revoke", "post", "413", m)
		}, true},
		"an open extra member on a transport 429": {func(d map[string]any) {
			m := closedGoAPIMember()
			delete(m, "additionalProperties")
			plantOneOf(d, "/v1/token/refresh", "post", "429", m)
		}, true},
		"a second closed disjoint member passes": {func(d map[string]any) {
			second := closedGoAPIMember()
			second["properties"].(map[string]any)["other"] = map[string]any{"type": "string"}
			plantOneOf(d, "/v1/identities/social", "post", "400", closedGoAPIMember(), second)
		}, false},
		"a closed disjoint member on a transport 413 passes": {func(d map[string]any) {
			plantOneOf(d, "/v1/sessions/revoke", "post", "413", closedGoAPIMember())
		}, false},
	}
	for name, c := range cases {
		document := loadOpenAPIRaw(t)
		c.plant(document)
		problems := openAPIPathProblems(document, rows)
		if c.wantProbs && len(problems) == 0 {
			t.Errorf("%s: the gate reported nothing", name)
		}
		if !c.wantProbs && len(problems) != 0 {
			t.Errorf("%s: the gate reported %v", name, problems)
		}
	}
}

// Only go-api's own path space is exempt: an API row anywhere else, one that
// merely starts like it included, still needs its operation.
func TestTheOpenAPIPathGateExemptsOnlyGoAPIsPathSpace(t *testing.T) {
	document := loadOpenAPIRaw(t)
	for _, route := range []string{"/v1/new-route", "/api/v1/authx/login", "/api/v1/auth", "/internal/v1x/sessions", "/internal/v1"} {
		rows := append(authServiceRows(t), map[string]any{
			"id": "POST " + route + " [dev-health-auth-service @api]", "method": "POST", "route": route,
		})
		if len(openAPIPathProblems(document, rows)) == 0 {
			t.Errorf("an API row at %s without an operation passed", route)
		}
	}
}
