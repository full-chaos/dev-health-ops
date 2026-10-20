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

// openAPIPathProblems checks the OpenAPI document against the auth-service API
// rows: one operation per API row and no other, every $ref inside an operation
// names an existing component schema, and every security requirement names an
// existing security scheme.
func openAPIPathProblems(document map[string]any, rows []map[string]any) []string {
	var problems []string
	want := map[string]bool{}
	bearer := map[string]bool{}
	for _, row := range rows {
		id, _ := row["id"].(string)
		if !strings.HasSuffix(id, "@api]") {
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
	}
	for name, plant := range cases {
		document := loadOpenAPIRaw(t)
		plant(document)
		if len(openAPIPathProblems(document, rows)) == 0 {
			t.Errorf("%s: the gate reported nothing", name)
		}
	}
}
