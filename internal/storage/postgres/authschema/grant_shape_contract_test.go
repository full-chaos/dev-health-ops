package authschema

import (
	"encoding/json"
	"os"
	"path/filepath"
	"regexp"
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
	dir, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	for {
		if _, statErr := os.Stat(filepath.Join(dir, "contracts", "auth", "v1", "jsonschema")); statErr == nil {
			break
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			t.Fatal("no ancestor holds contracts/auth/v1/jsonschema")
		}
		dir = parent
	}
	raw, err := os.ReadFile(filepath.Join(dir, "contracts", "auth", "v1", "jsonschema", file))
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

// The table CHECK keeps the two-segment key until the migration that widens it
// rolls; the authorization_request.v1 key (2-20 segments) is a superset of it,
// so the database never accepts a key the request would refuse.
func TestRepositoryGrantKeyCheckIsTheContractPattern(t *testing.T) {
	const tableFloor = `^([a-z0-9][a-z0-9._-]{0,99}/[a-z0-9][a-z0-9._-]{0,99}|\*)$`
	match := regexp.MustCompile(`(?s)resource_grants_repository_key_canonical\s+CHECK\s*\(\s*resource_kind <> 'repository' OR resource_id ~ '([^']*)'\s*\)`).FindStringSubmatch(grantShapeMigration(t))
	if match == nil {
		t.Fatal("resource_grants_repository_key_canonical is not the expected CHECK shape")
	}
	if match[1] != tableFloor {
		t.Fatalf("grant key CHECK pattern = %q, want the table floor %q", match[1], tableFloor)
	}
	request := regexp.MustCompile(schemaPattern(t, "authorization_request.v1.schema.json", "then", "properties", "resource", "properties", "key", "pattern"))
	floor := regexp.MustCompile(tableFloor)
	for _, key := range []string{"*", "acme/api", "a/b", "acme/api.v2", "0a/9z"} {
		if floor.MatchString(key) && !request.MatchString(key) {
			t.Errorf("the table accepts %q, the authorization_request.v1 key pattern refuses it", key)
		}
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
