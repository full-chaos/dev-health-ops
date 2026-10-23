package authschema

import (
	"regexp"
	"strings"
	"testing"
)

var validatedConstraints = []string{
	"memberships_active_has_role",
	"resource_grants_kind_closed",
	"resource_grants_repository_key_canonical",
	"service_accounts_named_owner",
}

func migration24(t *testing.T) string {
	t.Helper()
	migrations, err := Migrations()
	if err != nil {
		t.Fatal(err)
	}
	if len(migrations) < 24 || migrations[23].Version != 24 {
		t.Fatal("the lineage has no migration 0024")
	}
	return migrations[23].SQL
}

// TestMigration24ValidatesExactlyTheNotValidConstraints: 0024 names each
// constraint of 0019 and 0020 that was added NOT VALID, validates it, and
// adds nothing else.
func TestMigration24ValidatesExactlyTheNotValidConstraints(t *testing.T) {
	sql := migration24(t)
	migrations, err := Migrations()
	if err != nil {
		t.Fatal(err)
	}
	var notValid []string
	for _, m := range migrations[:23] {
		for _, name := range regexp.MustCompile(`(?m)ADD CONSTRAINT (\w+)\s+CHECK \(.*\) NOT VALID`).FindAllStringSubmatch(m.SQL, -1) {
			notValid = append(notValid, name[1])
		}
	}
	if strings.Join(notValid, ",") != strings.Join(validatedConstraints, ",") {
		t.Fatalf("NOT VALID constraints in 0001-0023 = %v, want %v", notValid, validatedConstraints)
	}
	for _, name := range validatedConstraints {
		if !regexp.MustCompile(`VALIDATE CONSTRAINT ` + name + `;`).MatchString(sql) {
			t.Errorf("0024 does not validate %s", name)
		}
		if !strings.Contains(sql, "'"+name+"'") && !strings.Contains(sql, name+" has") {
			t.Errorf("0024 pre-check does not name %s", name)
		}
	}
	code := regexp.MustCompile(`(?m)^--.*$`).ReplaceAllString(sql, "")
	if got := strings.Count(code, "VALIDATE CONSTRAINT"); got != len(validatedConstraints) {
		t.Errorf("0024 holds %d VALIDATE statements, want %d", got, len(validatedConstraints))
	}
	for _, forbidden := range []string{"ADD CONSTRAINT", "DROP ", "CREATE ", "UPDATE ", "DELETE ", "INSERT "} {
		if strings.Contains(code, forbidden) {
			t.Errorf("0024 contains %q", forbidden)
		}
	}
}

// TestMigration24RepositoryKeyPatternEqualsMigration20: the pre-check counts
// rows with the same pattern the constraint holds.
func TestMigration24RepositoryKeyPatternEqualsMigration20(t *testing.T) {
	pattern := regexp.MustCompile(`resource_id ~ '([^']+)'`)
	want := pattern.FindStringSubmatch(grantShapeMigration(t))
	got := pattern.FindStringSubmatch(migration24(t))
	if want == nil || got == nil || want[1] != got[1] {
		t.Fatalf("0024 pattern %v, 0020 pattern %v", got, want)
	}
}

// acceptValidation returns the constraint snapshot taken before a step with
// the " NOT VALID" suffix removed from the four constraints migration 0024
// validates. Only at version 24 and only for those four; every other byte of
// each definition must still match the snapshot after the step.
func acceptValidation(version int, constraints map[string]string) map[string]string {
	if version != 24 {
		return constraints
	}
	out := make(map[string]string, len(constraints))
	for key, definition := range constraints {
		out[key] = definition
		for _, name := range validatedConstraints {
			if strings.HasSuffix(key, "."+name) {
				out[key] = strings.TrimSuffix(definition, " NOT VALID")
			}
		}
	}
	return out
}

// TestAcceptValidationIsNarrow: the allowance covers the four constraints at
// version 24 and nothing else; any other suffix removal or edit stays a
// difference.
func TestAcceptValidationIsNarrow(t *testing.T) {
	const listed, unlisted = "memberships.memberships_active_has_role", "memberships.memberships_role_closed"
	before := map[string]string{listed: "CHECK (a) NOT VALID", unlisted: "CHECK (b) NOT VALID"}
	got := acceptValidation(24, before)
	if got[listed] != "CHECK (a)" {
		t.Fatal("a listed constraint losing NOT VALID was not accepted at version 24")
	}
	if got[unlisted] == "CHECK (b)" {
		t.Fatal("an unlisted constraint losing NOT VALID was accepted")
	}
	if acceptValidation(23, before)[listed] == "CHECK (a)" {
		t.Fatal("the allowance applied at another version")
	}
	if acceptValidation(24, map[string]string{listed: "CHECK (c) NOT VALID"})[listed] == "CHECK (a)" {
		t.Fatal("an edited definition was accepted")
	}
	if before[listed] != "CHECK (a) NOT VALID" {
		t.Fatal("the input snapshot was mutated")
	}
}
