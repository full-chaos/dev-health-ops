package authapi

import (
	"encoding/json"
	"net/http"
	"os"
	"path/filepath"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/auth/contracts"
)

const (
	declaredErrorRef     = "#/components/schemas/Error"
	declaredTransportRef = "#/components/schemas/TransportError"
)

// declaredStatusSchemaMembers returns the $ref of every member of the schema an
// OpenAPI operation declares for one status (a bare $ref is one member).
func declaredStatusSchemaMembers(t *testing.T, openapiPath, method, status string) []string {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join(repoRoot(t), "contracts", "auth", "v1", "openapi", "auth-v1.openapi.json"))
	if err != nil {
		t.Fatal(err)
	}
	var document struct {
		Paths map[string]map[string]struct {
			Responses map[string]struct {
				Content map[string]struct {
					Schema struct {
						Ref   string `json:"$ref"`
						OneOf []struct {
							Ref string `json:"$ref"`
						} `json:"oneOf"`
					} `json:"schema"`
				} `json:"content"`
			} `json:"responses"`
		} `json:"paths"`
	}
	if err := json.Unmarshal(raw, &document); err != nil {
		t.Fatal(err)
	}
	schema := document.Paths[openapiPath][method].Responses[status].Content["application/json"].Schema
	if len(schema.OneOf) == 0 {
		return []string{schema.Ref}
	}
	var refs []string
	for _, member := range schema.OneOf {
		refs = append(refs, member.Ref)
	}
	return refs
}

// TestAuthorizeMalformedBodyIsADeclaredOpenAPI400 drives each authorize route
// with bodies that are not exactly one request (or one batch) and checks the
// 400 body against the schema the OpenAPI document declares for that status:
// it must validate against exactly one declared member, and that member must
// be error.v1 with status 400 and reason malformed.
func TestAuthorizeMalformedBodyIsADeclaredOpenAPI400(t *testing.T) {
	h := newAuthorizeHandlerFixture(t)
	routes := []struct{ pattern, openapiPath string }{
		{PatternAuthorize, "/v1/authorize"},
		{PatternAuthorizeBatch, "/v1/authorize:batch"},
	}
	for _, route := range routes {
		for _, body := range []string{`{}`, `[]`, `not json`} {
			t.Run(route.openapiPath+" "+body, func(t *testing.T) {
				rec := h.post(route.pattern, h.workloadToken(nil), []byte(body))
				if rec.Code != http.StatusBadRequest {
					t.Fatalf("status %d, want 400; body %s", rec.Code, rec.Body)
				}
				var document any
				if err := json.Unmarshal(rec.Body.Bytes(), &document); err != nil {
					t.Fatal(err)
				}
				var matched []string
				for _, ref := range declaredStatusSchemaMembers(t, route.openapiPath, "post", "400") {
					surface := map[string]string{declaredErrorRef: contracts.ErrorSurface, declaredTransportRef: "transport-error.v1"}[ref]
					if surface == "" {
						t.Fatalf("the declared 400 holds a member %q this cell cannot validate", ref)
					}
					if contracts.Validate(repoRoot(t), surface, document) == nil {
						matched = append(matched, ref)
					}
				}
				if len(matched) != 1 || matched[0] != declaredErrorRef {
					t.Fatalf("the 400 body %s validates against %v of the declared members, want exactly [%s]", rec.Body, matched, declaredErrorRef)
				}
				assertError(t, rec, http.StatusBadRequest, "malformed")
			})
		}
	}
}
