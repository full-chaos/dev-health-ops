package authschema

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/auth/repokey"
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

// repositoryKeyCheck is the newest definition of the repository key CHECK: the
// pattern and the byte bound it holds.
func repositoryKeyCheck(t *testing.T) (pattern string, maxBytes string) {
	t.Helper()
	migrations, err := Migrations()
	if err != nil {
		t.Fatal(err)
	}
	form := regexp.MustCompile(`(?s)ADD CONSTRAINT resource_grants_repository_key_canonical\s+CHECK \(resource_kind <> 'repository' OR \(resource_id ~ '([^']*)' AND octet_length\(resource_id\) <= (\d+)\)\) NOT VALID`)
	named := regexp.MustCompile(`ADD CONSTRAINT resource_grants_repository_key_canonical`)
	var last *Migration
	for i := range migrations {
		if named.MatchString(migrations[i].SQL) {
			last = &migrations[i]
		}
	}
	if last == nil {
		t.Fatal("no migration adds resource_grants_repository_key_canonical")
	}
	match := form.FindStringSubmatch(last.SQL)
	if match == nil {
		t.Fatalf("migration %04d_%s does not add resource_grants_repository_key_canonical in the pattern + octet_length form", last.Version, last.Name)
	}
	return match[1], match[2]
}

// The table CHECK is the one repository key rule: the repokey pattern, the
// authorization_request.v1 key pattern, and the 255-byte bound.
func TestRepositoryGrantKeyCheckIsTheContractPattern(t *testing.T) {
	pattern, maxBytes := repositoryKeyCheck(t)
	if pattern != repokey.SchemaPattern {
		t.Fatalf("grant key CHECK pattern = %q, repokey.SchemaPattern = %q", pattern, repokey.SchemaPattern)
	}
	if want := fmt.Sprint(repokey.MaxBytes); maxBytes != want {
		t.Fatalf("grant key CHECK byte bound = %s, repokey.MaxBytes = %s", maxBytes, want)
	}
	if request := schemaPattern(t, "authorization_request.v1.schema.json", "then", "properties", "resource", "properties", "key", "pattern"); pattern != request {
		t.Fatalf("grant key CHECK pattern = %q, authorization_request.v1 key pattern = %q", pattern, request)
	}
}

// The CHECK pattern, run as written, accepts what the key rule accepts and
// refuses what it refuses.
func TestRepositoryGrantKeyCheckRefusesAndAccepts(t *testing.T) {
	pattern, maxBytes := repositoryKeyCheck(t)
	re := regexp.MustCompile(pattern)
	limit, _ := strconv.Atoi(maxBytes)
	seg := func(n int) string { return strings.Repeat("a", n) }
	deep := func(n int) string { return strings.TrimSuffix(strings.Repeat("a/", n), "/") }
	accept := []string{"*", "acme/widgets", "a/b", "acme/api.v2", "grp/sub/web", "a/b/c/d/e", "acme/.github", deep(20), seg(100) + "/" + seg(100)}
	refuse := []string{"Acme/widgets", "a", "a/", "/a", "a//b", "a/b/", "a./b", "a/b-/c", "a_/b/c", "a/b/c.", "a/b/c-", "a/b/c_", "a/.b/c", "a/b/..", "a/b/.", "a b/c", "**", "a/b*", deep(21)}
	for _, key := range accept {
		if !re.MatchString(key) || len(key) > limit {
			t.Errorf("CHECK refuses %q", key)
		}
	}
	for _, key := range refuse {
		if re.MatchString(key) && len(key) <= limit {
			t.Errorf("CHECK accepts %q", key)
		}
	}
	long := seg(100) + "/" + seg(100) + "/" + seg(60)
	if !re.MatchString(long) || len(long) <= limit {
		t.Errorf("a %d-byte key must match the pattern and exceed the %d-byte bound", len(long), limit)
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
