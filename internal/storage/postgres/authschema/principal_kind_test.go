package authschema

import (
	"encoding/json"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"testing"
)

const principalKindMigration = "principal_kind_external_source"

func principalKindMigrationSQL(t *testing.T) string {
	t.Helper()
	migrations, err := Migrations()
	if err != nil {
		t.Fatal(err)
	}
	for _, m := range migrations {
		if m.Name == principalKindMigration {
			return m.SQL
		}
	}
	t.Fatalf("the lineage lacks the migration named %s", principalKindMigration)
	return ""
}

// The kind CHECK on principals lists exactly the principal.v1 principal types
// that name a stored principal: human is stored as user, and the two types that
// are credential-only (client_credential, infrastructure) have no principal row.
func TestPrincipalKindCheckListsThePrincipalContractKinds(t *testing.T) {
	raw, err := os.ReadFile(filepath.Join("..", "..", "..", "..", "contracts", "auth", "v1", "jsonschema", "principal.v1.schema.json"))
	if err != nil {
		t.Fatal(err)
	}
	var schema struct {
		Properties struct {
			Type struct {
				Enum []string `json:"enum"`
			} `json:"principal_type"`
		} `json:"properties"`
	}
	if err := json.Unmarshal(raw, &schema); err != nil {
		t.Fatal(err)
	}
	if len(schema.Properties.Type.Enum) == 0 {
		t.Fatal("principal.v1 declares no principal_type enum; the comparison would be vacuous")
	}
	stored := map[string]string{"human": "user", "service_account": "service_account", "workload": "workload", "external_source": "external_source"}
	var want []string
	for _, kind := range schema.Properties.Type.Enum {
		if mapped, ok := stored[kind]; ok {
			want = append(want, mapped)
		}
	}
	sort.Strings(want)
	match := regexp.MustCompile(`(?s)ADD CONSTRAINT principals_kind_check\s+CHECK\s*\(\s*kind IN \(([^)]*)\)\s*\)`).FindStringSubmatch(principalKindMigrationSQL(t))
	if match == nil {
		t.Fatal("the migration does not redefine principals_kind_check as a closed IN list")
	}
	var got []string
	for _, item := range strings.Split(match[1], ",") {
		got = append(got, strings.Trim(strings.TrimSpace(item), "'"))
	}
	sort.Strings(got)
	if strings.Join(got, ",") != strings.Join(want, ",") {
		t.Fatalf("principals_kind_check lists %v, principal.v1 stored kinds are %v", got, want)
	}
}

// The first statement takes principals in SHARE ROW EXCLUSIVE, the lock order of
// 0041, and the CHECK is added NOT VALID and validated in a statement of its own
// so the scan does not run under the ALTER's lock.
func TestPrincipalKindMigrationLocksFirstAndValidatesApart(t *testing.T) {
	sql := principalKindMigrationSQL(t)
	body := regexp.MustCompile(`(?m)^\s*--.*$`).ReplaceAllString(sql, "")
	body = strings.TrimSpace(body)
	if !strings.HasPrefix(body, "LOCK TABLE principals IN SHARE ROW EXCLUSIVE MODE;") {
		t.Fatalf("the first statement is not LOCK TABLE principals IN SHARE ROW EXCLUSIVE MODE: %.80s", body)
	}
	if !strings.Contains(body, ") NOT VALID;") || !strings.Contains(body, "VALIDATE CONSTRAINT principals_kind_check;") {
		t.Fatal("principals_kind_check is not added NOT VALID and validated in its own statement")
	}
}

// An external_source principal holds no session, no membership and no
// platform-class credential: the three tables refuse it in the database, not in
// each writer.
func TestPrincipalKindMigrationGuardsThreeTables(t *testing.T) {
	sql := principalKindMigrationSQL(t)
	for _, table := range []string{"sessions", "memberships", "credential_registry"} {
		pattern := regexp.MustCompile(`(?s)CREATE TRIGGER \w+\s+BEFORE INSERT OR UPDATE OF principal_id ON ` + table + `\b.*?EXECUTE FUNCTION principal_not_external_source\(\)`)
		if !pattern.MatchString(sql) {
			t.Errorf("no guard trigger on %s", table)
		}
	}
	if !regexp.MustCompile(`(?s)ON credential_registry\s+FOR EACH ROW WHEN \(NEW\.domain_owner = 'platform'\)`).MatchString(sql) {
		t.Error("the credential_registry guard does not apply to platform-owned rows only")
	}
	if !strings.Contains(sql, "kind = 'external_source'") {
		t.Error("the guard function does not test kind = 'external_source'")
	}
}
