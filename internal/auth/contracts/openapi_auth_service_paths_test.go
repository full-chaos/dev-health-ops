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
			if len(refs) == 0 {
				problems = append(problems, fmt.Sprintf("OpenAPI operation %s references no schema", key))
			}
			responses, _ := operation["responses"].(map[string]any)
			for _, status := range transportStatuses {
				response, declared := responses[status].(map[string]any)
				if !declared {
					continue
				}
				content, _ := response["content"].(map[string]any)
				body, _ := content["application/json"].(map[string]any)
				schema, _ := body["schema"].(map[string]any)
				if schema["$ref"] != transportErrorRef {
					problems = append(problems, fmt.Sprintf("OpenAPI operation %s: its %s is the transport's answer and must reference %s", key, status, transportErrorRef))
				}
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
