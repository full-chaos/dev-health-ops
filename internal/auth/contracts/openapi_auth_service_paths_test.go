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
	return isTransportErrorRef(members[0])
}

// isTransportErrorRef: exactly {"$ref": TransportError}; a sibling keyword (not, if, ...) could make the envelope
// match nothing, so it is not the envelope.
func isTransportErrorRef(schema any) bool {
	object, _ := schema.(map[string]any)
	return len(object) == 1 && object["$ref"] == transportErrorRef
}

// checkedKeywords are the schema keywords an extra oneOf member may use, from the census of the keywords the live
// OpenAPI document's inline schemas use (type, required, additionalProperties, properties, const, enum, description,
// format); any other keyword fails closed. The census also holds $ref and oneOf; a member may not use $ref (the walk
// does not resolve it), and oneOf is walked below.
var checkedKeywords = map[string]bool{
	"type": true, "required": true, "additionalProperties": true, "properties": true,
	"const": true, "enum": true, "description": true, "format": true,
}

// walkedKeywords are the structural keywords the walk descends into: a schema in properties, items, or a list of
// schemas in oneOf, anyOf and allOf.
var walkedKeywords = map[string]bool{"items": true, "oneOf": true, "anyOf": true, "allOf": true}

// isObjectSchema: type object (or a type list holding object), or any keyword that constrains an object.
func isObjectSchema(schema map[string]any) bool {
	switch typed := schema["type"].(type) {
	case string:
		if typed == "object" {
			return true
		}
	case []any:
		for _, name := range typed {
			if name == "object" {
				return true
			}
		}
	}
	for _, keyword := range []string{"properties", "required", "additionalProperties", "minProperties", "maxProperties"} {
		if _, present := schema[keyword]; present {
			return true
		}
	}
	return false
}

// jsonStringList reads a JSON list of strings; ok is false for any other shape.
func jsonStringList(raw any) (values []string, ok bool) {
	list, isList := raw.([]any)
	if !isList {
		return nil, false
	}
	for _, item := range list {
		text, isText := item.(string)
		if !isText {
			return nil, false
		}
		values = append(values, text)
	}
	return values, true
}

// keywordShapeProblems holds the one accepted value shape of each keyword, at every walked level; any other shape
// fails closed with the keyword and its path: type is a string or a non-empty list of strings; required is a list of
// unique strings; properties is a map; items is a map; oneOf, anyOf and allOf are non-empty lists; additionalProperties
// is the literal false; $ref, description and format are strings; enum is a non-empty list; const is any value.
func keywordShapeProblems(schema map[string]any, path string) []string {
	var problems []string
	bad := func(keyword, want string) {
		problems = append(problems, fmt.Sprintf("%s/%s must be %s", path, keyword, want))
	}
	for keyword, value := range schema {
		switch keyword {
		case "type":
			if _, isText := value.(string); isText {
				continue
			}
			if names, ok := jsonStringList(value); !ok || len(names) == 0 {
				bad(keyword, "a string or a non-empty list of strings")
			}
		case "required":
			names, ok := jsonStringList(value)
			if !ok {
				bad(keyword, "a list of strings")
				continue
			}
			seen := map[string]bool{}
			for _, name := range names {
				if seen[name] {
					bad(keyword, "a list of unique strings")
					break
				}
				seen[name] = true
			}
		case "properties":
			if _, ok := value.(map[string]any); !ok {
				bad(keyword, "a map of schemas")
			}
		case "items":
			if _, ok := value.(map[string]any); !ok {
				bad(keyword, "a schema object")
			}
		case "oneOf", "anyOf", "allOf":
			if list, ok := value.([]any); !ok || len(list) == 0 {
				bad(keyword, "a non-empty list of schemas")
			}
		case "additionalProperties":
			if value != false {
				bad(keyword, "the boolean false")
			}
		case "$ref", "description", "format":
			if _, ok := value.(string); !ok {
				bad(keyword, "a string")
			}
		case "enum":
			if list, ok := value.([]any); !ok || len(list) == 0 {
				bad(keyword, "a non-empty list")
			}
		}
	}
	sort.Strings(problems)
	return problems
}

// closedSchemaProblems walks an inline schema: a keyword outside checkedKeywords and walkedKeywords fails closed
// (with its path), every object schema must set additionalProperties to false, and properties, items and the
// members of oneOf, anyOf and allOf are walked the same way.
func closedSchemaProblems(schema map[string]any, path string) []string {
	var problems []string
	keywords := make([]string, 0, len(schema))
	for keyword := range schema {
		keywords = append(keywords, keyword)
	}
	sort.Strings(keywords)
	for _, keyword := range keywords {
		if !checkedKeywords[keyword] && !walkedKeywords[keyword] {
			problems = append(problems, fmt.Sprintf("%s uses %q, which the gate does not check", path, keyword))
		}
	}
	problems = append(problems, keywordShapeProblems(schema, path)...)
	if isObjectSchema(schema) && schema["additionalProperties"] != false {
		problems = append(problems, fmt.Sprintf("%s is open (additionalProperties is not false)", path))
	}
	unreadable := func(at string) {
		problems = append(problems, fmt.Sprintf("%s is not an object schema the gate can read", at))
	}
	if raw, present := schema["properties"]; present {
		properties, ok := raw.(map[string]any)
		if !ok {
			unreadable(path + "/properties")
		}
		names := make([]string, 0, len(properties))
		for name := range properties {
			names = append(names, name)
		}
		sort.Strings(names)
		for _, name := range names {
			if child, ok := properties[name].(map[string]any); ok {
				problems = append(problems, closedSchemaProblems(child, path+"/properties/"+name)...)
			} else {
				unreadable(path + "/properties/" + name)
			}
		}
	}
	if raw, present := schema["items"]; present {
		if items, ok := raw.(map[string]any); ok {
			problems = append(problems, closedSchemaProblems(items, path+"/items")...)
		} else {
			unreadable(path + "/items")
		}
	}
	for _, keyword := range []string{"oneOf", "anyOf", "allOf"} {
		raw, present := schema[keyword]
		if !present {
			continue
		}
		members, ok := raw.([]any)
		if !ok {
			unreadable(path + "/" + keyword)
			continue
		}
		for index, member := range members {
			if child, ok := member.(map[string]any); ok {
				problems = append(problems, closedSchemaProblems(child, fmt.Sprintf("%s/%s/%d", path, keyword, index))...)
			} else {
				unreadable(fmt.Sprintf("%s/%s/%d", path, keyword, index))
			}
		}
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
		properties, _ := member["properties"].(map[string]any) // a non-map properties is reported by closedSchemaProblems
		required, _ := jsonStringList(member["required"])      // a malformed or absent required is reported by keywordShapeProblems or has no name
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
				raw, present := responses[status]
				if !present || method == "head" {
					continue
				}
				response, isObject := raw.(map[string]any)
				if !isObject {
					problems = append(problems, fmt.Sprintf("OpenAPI operation %s: its %s must be a response object", key, status))
					continue
				}
				content, _ := response["content"].(map[string]any)
				body, _ := content["application/json"].(map[string]any)
				schema, _ := body["schema"].(map[string]any)
				if !isTransportErrorRef(schema) && !oneOfHasTransportError(schema) {
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
		"an extra member that nests a oneOf with an open object": {func(d map[string]any) {
			m := closedGoAPIMember()
			m["properties"].(map[string]any)["detail"].(map[string]any)["oneOf"] = []any{map[string]any{"type": "object"}}
			plantOneOf(d, "/v1/identities/social", "post", "400", m)
		}, true},
		"an extra member that nests a oneOf of closed members": {func(d map[string]any) {
			m := closedGoAPIMember()
			m["properties"].(map[string]any)["detail"] = map[string]any{"oneOf": []any{closedGoAPIMember(), map[string]any{"type": "string"}}}
			plantOneOf(d, "/v1/identities/social", "post", "400", m)
		}, false},
		"an open object hidden under contains": {func(d map[string]any) {
			m := closedGoAPIMember()
			m["properties"].(map[string]any)["detail"].(map[string]any)["contains"] = map[string]any{"type": "object", "properties": map[string]any{}}
			plantOneOf(d, "/v1/identities/social", "post", "400", m)
		}, true},
		"a keyword outside the checked set": {func(d map[string]any) {
			m := closedGoAPIMember()
			m["properties"].(map[string]any)["detail"].(map[string]any)["properties"].(map[string]any)["message"] = map[string]any{"type": "string", "pattern": "^x$"}
			plantOneOf(d, "/v1/identities/social", "post", "400", m)
		}, true},
		"an untyped object constraint": {func(d map[string]any) {
			m := closedGoAPIMember()
			m["properties"].(map[string]any)["detail"].(map[string]any)["properties"].(map[string]any)["message"] = map[string]any{"required": []any{"x"}}
			plantOneOf(d, "/v1/identities/social", "post", "400", m)
		}, true},
		"an open object typed by a type list": {func(d map[string]any) {
			m := closedGoAPIMember()
			m["properties"].(map[string]any)["detail"].(map[string]any)["properties"].(map[string]any)["message"] = map[string]any{"type": []any{"object", "null"}}
			plantOneOf(d, "/v1/identities/social", "post", "400", m)
		}, true},
		"a closed object typed by a type list": {func(d map[string]any) {
			m := closedGoAPIMember()
			m["properties"].(map[string]any)["detail"].(map[string]any)["properties"].(map[string]any)["message"] = map[string]any{"type": []any{"object", "null"}, "additionalProperties": false}
			plantOneOf(d, "/v1/identities/social", "post", "400", m)
		}, false},
		"a boolean true items schema": {func(d map[string]any) {
			m := closedGoAPIMember()
			message := m["properties"].(map[string]any)["detail"].(map[string]any)["properties"].(map[string]any)
			message["message"] = map[string]any{"type": "array", "items": true}
			plantOneOf(d, "/v1/identities/social", "post", "400", m)
		}, true},
		"a boolean false items schema": {func(d map[string]any) {
			m := closedGoAPIMember()
			message := m["properties"].(map[string]any)["detail"].(map[string]any)["properties"].(map[string]any)
			message["message"] = map[string]any{"type": "array", "items": false}
			plantOneOf(d, "/v1/identities/social", "post", "400", m)
		}, true},
		"items as a list": {func(d map[string]any) {
			m := closedGoAPIMember()
			message := m["properties"].(map[string]any)["detail"].(map[string]any)["properties"].(map[string]any)
			message["message"] = map[string]any{"type": "array", "items": []any{map[string]any{"type": "string"}}}
			plantOneOf(d, "/v1/identities/social", "post", "400", m)
		}, true},
		"a boolean oneOf member": {func(d map[string]any) {
			m := closedGoAPIMember()
			message := m["properties"].(map[string]any)["detail"].(map[string]any)["properties"].(map[string]any)
			message["message"] = map[string]any{"oneOf": []any{true}}
			plantOneOf(d, "/v1/identities/social", "post", "400", m)
		}, true},
		"a boolean anyOf member": {func(d map[string]any) {
			m := closedGoAPIMember()
			message := m["properties"].(map[string]any)["detail"].(map[string]any)["properties"].(map[string]any)
			message["message"] = map[string]any{"anyOf": []any{map[string]any{"type": "string"}, true}}
			plantOneOf(d, "/v1/identities/social", "post", "400", m)
		}, true},
		"a boolean allOf member": {func(d map[string]any) {
			m := closedGoAPIMember()
			message := m["properties"].(map[string]any)["detail"].(map[string]any)["properties"].(map[string]any)
			message["message"] = map[string]any{"allOf": []any{false}}
			plantOneOf(d, "/v1/identities/social", "post", "400", m)
		}, true},
		"a null allOf member": {func(d map[string]any) {
			m := closedGoAPIMember()
			message := m["properties"].(map[string]any)["detail"].(map[string]any)["properties"].(map[string]any)
			message["message"] = map[string]any{"allOf": []any{nil}}
			plantOneOf(d, "/v1/identities/social", "post", "400", m)
		}, true},
		"a oneOf that is an object": {func(d map[string]any) {
			m := closedGoAPIMember()
			message := m["properties"].(map[string]any)["detail"].(map[string]any)["properties"].(map[string]any)
			message["message"] = map[string]any{"oneOf": map[string]any{"type": "string"}}
			plantOneOf(d, "/v1/identities/social", "post", "400", m)
		}, true},
		"a boolean property schema": {func(d map[string]any) {
			m := closedGoAPIMember()
			message := m["properties"].(map[string]any)["detail"].(map[string]any)["properties"].(map[string]any)
			message["message"] = true
			plantOneOf(d, "/v1/identities/social", "post", "400", m)
		}, true},
		"a null property schema": {func(d map[string]any) {
			m := closedGoAPIMember()
			message := m["properties"].(map[string]any)["detail"].(map[string]any)["properties"].(map[string]any)
			message["message"] = nil
			plantOneOf(d, "/v1/identities/social", "post", "400", m)
		}, true},
		"a string property schema": {func(d map[string]any) {
			m := closedGoAPIMember()
			message := m["properties"].(map[string]any)["detail"].(map[string]any)["properties"].(map[string]any)
			message["message"] = "string"
			plantOneOf(d, "/v1/identities/social", "post", "400", m)
		}, true},
		"properties that is a list": {func(d map[string]any) {
			m := closedGoAPIMember()
			m["properties"].(map[string]any)["detail"].(map[string]any)["properties"] = []any{}
			plantOneOf(d, "/v1/identities/social", "post", "400", m)
		}, true},
		"additionalProperties as an empty schema": {func(d map[string]any) {
			m := closedGoAPIMember()
			m["additionalProperties"] = map[string]any{}
			plantOneOf(d, "/v1/identities/social", "post", "400", m)
		}, true},
		"additionalProperties as a typed schema": {func(d map[string]any) {
			m := closedGoAPIMember()
			m["properties"].(map[string]any)["detail"].(map[string]any)["additionalProperties"] = map[string]any{"type": "string"}
			plantOneOf(d, "/v1/identities/social", "post", "400", m)
		}, true},
		"an open object under items": {func(d map[string]any) {
			m := closedGoAPIMember()
			m["properties"].(map[string]any)["detail"].(map[string]any)["properties"].(map[string]any)["message"] = map[string]any{"type": "array", "items": map[string]any{"type": "object"}}
			plantOneOf(d, "/v1/identities/social", "post", "400", m)
		}, true},
		"a closed object under items": {func(d map[string]any) {
			m := closedGoAPIMember()
			m["properties"].(map[string]any)["detail"].(map[string]any)["properties"].(map[string]any)["message"] = map[string]any{"type": "array", "items": map[string]any{"type": "object", "additionalProperties": false}}
			plantOneOf(d, "/v1/identities/social", "post", "400", m)
		}, false},
		"an envelope member with a $ref sibling": {func(d map[string]any) {
			op := d["paths"].(map[string]any)["/v1/identities/social"].(map[string]any)["post"].(map[string]any)
			schema := op["responses"].(map[string]any)["400"].(map[string]any)["content"].(map[string]any)["application/json"].(map[string]any)["schema"].(map[string]any)
			schema["oneOf"].([]any)[0].(map[string]any)["not"] = map[string]any{}
		}, true},
		"a plain transport schema with a $ref sibling": {func(d map[string]any) {
			op := d["paths"].(map[string]any)["/v1/sessions/revoke"].(map[string]any)["post"].(map[string]any)
			schema := op["responses"].(map[string]any)["413"].(map[string]any)["content"].(map[string]any)["application/json"].(map[string]any)["schema"].(map[string]any)
			schema["not"] = map[string]any{}
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

// Every allowlisted keyword has one accepted value shape, checked at every walked level; any other shape fails
// closed. The mutated schema is the member's detail.properties.message schema (a plain string schema at first).
func TestTheOpenAPIPathGateChecksEveryKeywordShape(t *testing.T) {
	rows := authServiceRows(t)
	cases := []struct {
		name   string
		schema map[string]any
		want   bool
	}{
		{"type as a number", map[string]any{"type": 5}, true},
		{"type as an empty list", map[string]any{"type": []any{}}, true},
		{"type as a list with a number", map[string]any{"type": []any{"string", 5}}, true},
		{"type as an object", map[string]any{"type": map[string]any{}}, true},
		{"type as a list of strings", map[string]any{"type": []any{"string", "null"}}, false},
		{"required as a string", map[string]any{"type": "object", "additionalProperties": false, "required": "error"}, true},
		{"required as an object", map[string]any{"type": "object", "additionalProperties": false, "required": map[string]any{}}, true},
		{"required as a list with a number", map[string]any{"type": "object", "additionalProperties": false, "required": []any{1}}, true},
		{"required with a duplicate", map[string]any{"type": "object", "additionalProperties": false, "required": []any{"a", "a"}}, true},
		{"required as a list of unique strings", map[string]any{"type": "object", "additionalProperties": false, "required": []any{"a", "b"}}, false},
		{"required as an empty list", map[string]any{"type": "object", "additionalProperties": false, "required": []any{}}, false},
		{"properties as a string", map[string]any{"type": "object", "additionalProperties": false, "properties": "x"}, true},
		{"items as a string", map[string]any{"type": "array", "items": "x"}, true},
		{"oneOf as an empty list", map[string]any{"oneOf": []any{}}, true},
		{"anyOf as an empty list", map[string]any{"anyOf": []any{}}, true},
		{"allOf as a string", map[string]any{"allOf": "x"}, true},
		{"anyOf of closed members", map[string]any{"anyOf": []any{map[string]any{"type": "string"}, map[string]any{"type": "null"}}}, false},
		{"additionalProperties as a string", map[string]any{"type": "object", "additionalProperties": "no"}, true},
		{"$ref as a number", map[string]any{"$ref": 5}, true},
		{"$ref as a string", map[string]any{"$ref": "#/components/schemas/Error"}, true},
		{"enum as a string", map[string]any{"type": "string", "enum": "a"}, true},
		{"enum as an empty list", map[string]any{"type": "string", "enum": []any{}}, true},
		{"enum as a list", map[string]any{"type": "string", "enum": []any{"a", "b"}}, false},
		{"description as a number", map[string]any{"type": "string", "description": 5}, true},
		{"description as a string", map[string]any{"type": "string", "description": "d"}, false},
		{"format as a list", map[string]any{"type": "string", "format": []any{"uuid"}}, true},
		{"format as a string", map[string]any{"type": "string", "format": "uuid"}, false},
		{"const as an object", map[string]any{"const": map[string]any{"a": 1}}, false},
		{"const as null", map[string]any{"const": nil}, false},
		{"const as a list", map[string]any{"const": []any{1, "x"}}, false},
	}
	for _, c := range cases {
		for _, level := range []string{"nested", "top"} {
			document := loadOpenAPIRaw(t)
			member := closedGoAPIMember()
			if level == "nested" {
				member["properties"].(map[string]any)["detail"].(map[string]any)["properties"].(map[string]any)["message"] = c.schema
			} else {
				if c.schema["type"] != nil && c.schema["type"] != "object" {
					continue
				}
				member = map[string]any{"type": "object", "additionalProperties": false}
				for keyword, value := range c.schema {
					if keyword == "type" {
						continue
					}
					member[keyword] = value
				}
			}
			plantOneOf(document, "/v1/identities/social", "post", "400", member)
			problems := openAPIPathProblems(document, rows)
			if c.want && len(problems) == 0 {
				t.Errorf("%s (%s level): the gate reported nothing", c.name, level)
			}
			if !c.want && len(problems) != 0 {
				t.Errorf("%s (%s level): the gate reported %v", c.name, level, problems)
			}
		}
	}
	for _, status := range []string{"400", "413", "429"} {
		document := loadOpenAPIRaw(t)
		route, method := "/v1/token/refresh", "post"
		if status == "413" {
			route = "/v1/sessions/revoke"
		}
		responses := document["paths"].(map[string]any)[route].(map[string]any)[method].(map[string]any)["responses"].(map[string]any)
		responses[status] = "not a response object"
		if len(openAPIPathProblems(document, rows)) == 0 {
			t.Errorf("a transport %s that is not an object: the gate reported nothing", status)
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
