package contracts

import (
	"encoding/json"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
)

// accessTokenSchemaBinding is the aud/scope part of the access-token.v1
// schema: the two enums and every allOf if aud == X then scope == Y.
type accessTokenSchemaBinding struct {
	Properties struct {
		Claims struct {
			Properties struct {
				Aud struct {
					Enum []string `json:"enum"`
				} `json:"aud"`
				Scope struct {
					Enum []string `json:"enum"`
				} `json:"scope"`
			} `json:"properties"`
			AllOf []struct {
				If struct {
					Properties struct {
						Aud struct {
							Const string `json:"const"`
						} `json:"aud"`
					} `json:"properties"`
				} `json:"if"`
				Then struct {
					Properties struct {
						Scope struct {
							Const string `json:"const"`
						} `json:"scope"`
					} `json:"properties"`
				} `json:"then"`
			} `json:"allOf"`
		} `json:"claims"`
	} `json:"properties"`
}

// TestTheSchemaBindsEachUserTokenAudienceToTheScopeOfItsScopeMap: the schema's
// allOf binding is exactly the session-token map plus the ACR map, the scope
// enum is their scopes, and the two maps share no audience.
func TestTheSchemaBindsEachUserTokenAudienceToTheScopeOfItsScopeMap(t *testing.T) {
	raw, err := os.ReadFile(SchemaPath(testRoot(t), AccessTokenSurface))
	if err != nil {
		t.Fatal(err)
	}
	var schema accessTokenSchemaBinding
	if err := json.Unmarshal(raw, &schema); err != nil {
		t.Fatal(err)
	}
	bound := map[string]string{}
	for _, rule := range schema.Properties.Claims.AllOf {
		aud, scope := rule.If.Properties.Aud.Const, rule.Then.Properties.Scope.Const
		if aud == "" || scope == "" {
			t.Fatalf("an allOf rule binds %q to %q", aud, scope)
		}
		if _, repeated := bound[aud]; repeated {
			t.Fatalf("audience %q is bound twice", aud)
		}
		bound[aud] = scope
	}
	want := map[string]string{}
	for aud, scope := range accessTokenScopes {
		want[aud] = scope
	}
	for aud, scope := range acrAccessTokenScopes {
		if _, shared := accessTokenScopes[aud]; shared {
			t.Fatalf("audience %q is in both scope maps", aud)
		}
		want[aud] = scope
	}
	if len(bound) != len(want) {
		t.Fatalf("schema binds %v, the scope maps are %v", bound, want)
	}
	var scopes []string
	for aud, scope := range want {
		if bound[aud] != scope {
			t.Errorf("schema binds %q to %q, the scope map to %q", aud, bound[aud], scope)
		}
		scopes = append(scopes, scope)
	}
	enum := append([]string{}, schema.Properties.Claims.Properties.Scope.Enum...)
	sort.Strings(enum)
	sort.Strings(scopes)
	if strings.Join(enum, ",") != strings.Join(scopes, ",") {
		t.Errorf("schema scope enum %v, scope maps %v", enum, scopes)
	}
}

// TestNoSessionTokenAudienceLookupKnowsAnACRAudience: AccessTokenScope is what
// the session minter (issuer_eddsa.go) and its startup check (authconfig) take
// an audience from, so it must refuse acr-api and acr-mcp, and the ACR lookup
// must refuse the session audiences.
func TestNoSessionTokenAudienceLookupKnowsAnACRAudience(t *testing.T) {
	for aud, scope := range map[string]string{ACRAPIAudience: "acr:api", ACRMCPAudience: "acr:mcp"} {
		if got, ok := AccessTokenScope(aud); ok {
			t.Errorf("AccessTokenScope(%q) = %q: a session token could carry an ACR audience", aud, got)
		}
		if got, ok := ACRAccessTokenScope(aud); !ok || got != scope {
			t.Errorf("ACRAccessTokenScope(%q) = %q, %v; want %q", aud, got, ok, scope)
		}
	}
	for _, aud := range []string{PlatformAPIAudience, SessionRouteAudience, "query-api", "", "acr"} {
		if got, ok := ACRAccessTokenScope(aud); ok {
			t.Errorf("ACRAccessTokenScope(%q) = %q", aud, got)
		}
	}
}

// TestEachACRAudienceIsAUserTokenRowServedByItsOwnACRService: the vocabulary
// rows of acr-api and acr-mcp carry the user access token class only (never a
// workload token) and name the one ACR service that accepts each.
func TestEachACRAudienceIsAUserTokenRowServedByItsOwnACRService(t *testing.T) {
	raw, err := os.ReadFile(filepath.Join(ContractsDir(testRoot(t)), "vocabulary", "audiences.json"))
	if err != nil {
		t.Fatal(err)
	}
	var vocabulary struct {
		Audiences []struct {
			Audience string   `json:"audience"`
			Classes  []string `json:"classes"`
			Services []string `json:"services"`
		} `json:"audiences"`
	}
	if err := json.Unmarshal(raw, &vocabulary); err != nil {
		t.Fatal(err)
	}
	want := map[string]string{ACRAPIAudience: "dev-health-acr-api", ACRMCPAudience: "dev-health-acr-mcp"}
	seen := map[string]bool{}
	for _, row := range vocabulary.Audiences {
		service, acr := want[row.Audience]
		if !acr {
			continue
		}
		seen[row.Audience] = true
		if strings.Join(row.Classes, ",") != UserAccessTokenClass || strings.Join(row.Services, ",") != service {
			t.Errorf("row %q = classes %v services %v; want [%s] [%s]", row.Audience, row.Classes, row.Services, UserAccessTokenClass, service)
		}
	}
	if len(seen) != len(want) {
		t.Fatalf("vocabulary rows %v, want %v", seen, want)
	}
}
