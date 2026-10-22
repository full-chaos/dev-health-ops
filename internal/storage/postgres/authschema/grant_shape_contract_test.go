package authschema

import (
	"encoding/json"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"strings"
	"testing"
)

func grantShapeMigration(t *testing.T) string {
	t.Helper()
	migrations, err := Migrations()
	if err != nil {
		t.Fatal(err)
	}
	for _, m := range migrations {
		if strings.Contains(m.SQL, "resource_grants_repository_key_canonical") {
			return m.SQL
		}
	}
	t.Fatal("no migration defines resource_grants_repository_key_canonical")
	return ""
}

func schemaPattern(t *testing.T, file string, path ...string) string {
	t.Helper()
	_, here, _, _ := runtime.Caller(0)
	raw, err := os.ReadFile(filepath.Join(filepath.Dir(here), "..", "..", "..", "..", "contracts", "auth", "v1", "jsonschema", file))
	if err != nil {
		t.Fatal(err)
	}
	var node any
	if err := json.Unmarshal(raw, &node); err != nil {
		t.Fatal(err)
	}
	for _, key := range path {
		m, ok := node.(map[string]any)
		if !ok {
			t.Fatalf("%s: %v is not an object", file, path)
		}
		node = m[key]
	}
	pattern, ok := node.(string)
	if !ok {
		t.Fatalf("%s: %v is not a string", file, path)
	}
	return pattern
}

// The database refuses a repository grant key that the authorization request
// would not accept: the CHECK pattern is the authorization_request.v1 pattern.
func TestRepositoryGrantKeyCheckIsTheContractPattern(t *testing.T) {
	want := schemaPattern(t, "authorization_request.v1.schema.json", "then", "properties", "resource", "properties", "key", "pattern")
	match := regexp.MustCompile(`(?s)resource_grants_repository_key_canonical\s+CHECK\s*\(\s*resource_kind <> 'repository' OR resource_id ~ '([^']*)'\s*\)`).FindStringSubmatch(grantShapeMigration(t))
	if match == nil {
		t.Fatal("resource_grants_repository_key_canonical is not the expected CHECK shape")
	}
	if match[1] != want {
		t.Fatalf("grant key CHECK pattern = %q, authorization_request.v1 repository key pattern = %q", match[1], want)
	}
}

// Workload allowed_actions elements use the action syntax of principal.v1.
func TestWorkloadAllowedActionPatternIsTheContractPattern(t *testing.T) {
	want := schemaPattern(t, "principal.v1.schema.json", "properties", "actor_chain", "items", "properties", "permitted_actions", "items", "pattern")
	match := regexp.MustCompile(`(?s)allowed_actions_are_valid.*?a !~ '([^']*)'`).FindStringSubmatch(grantShapeMigration(t))
	if match == nil {
		t.Fatal("allowed_actions_are_valid has no element pattern")
	}
	if match[1] != want {
		t.Fatalf("allowed_actions pattern = %q, principal.v1 action pattern = %q", match[1], want)
	}
}
