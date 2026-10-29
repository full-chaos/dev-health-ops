package authschema

import (
	"regexp"
	"strings"
	"testing"
)

var sqlLineComment = regexp.MustCompile(`--[^\n]*`)

func code(sql string) string { return sqlLineComment.ReplaceAllString(sql, "") }

// restoreFenceMigration finds the fence's migration by its name, not its
// number: the number is provisional while another lane also asks for it.
func restoreFenceMigration(t *testing.T) string {
	t.Helper()
	migrations, err := Migrations()
	if err != nil {
		t.Fatal(err)
	}
	for _, m := range migrations {
		if m.Name == "restore_fence" {
			return code(m.SQL)
		}
	}
	t.Fatal("the restore_fence migration is missing")
	return ""
}

// The auth schema has no SECURITY DEFINER function: the runtime role reads the
// timeline itself (readiness calls the WAL built-ins), so no function lends the
// owner's authority to another role.
func TestTheLineageHasNoSecurityDefinerFunction(t *testing.T) {
	migrations, err := Migrations()
	if err != nil {
		t.Fatal(err)
	}
	for _, m := range migrations {
		if strings.Contains(strings.ToUpper(code(m.SQL)), "SECURITY DEFINER") {
			t.Errorf("migration %04d_%s declares a SECURITY DEFINER function", m.Version, m.Name)
		}
	}
}

func TestTheRestoreFenceMigrationCreatesOneTableAndNoFunction(t *testing.T) {
	sql := restoreFenceMigration(t)
	if n := strings.Count(strings.ToUpper(sql), "CREATE FUNCTION"); n != 0 {
		t.Errorf("the migration creates %d function(s); it creates only the state table", n)
	}
	if !regexp.MustCompile(`(?i)CREATE TABLE restore_fence_state\b`).MatchString(sql) {
		t.Error("restore_fence_state is not created")
	}
	if regexp.MustCompile(`(?i)\bGRANT\b`).MatchString(sql) {
		t.Error("the migration grants a privilege; grants live in grants.go with the rest of the posture")
	}
	if strings.Contains(sql, "restore_fence_timeline") {
		t.Error("the migration still refers to the removed timeline function")
	}
}

// The runtime role reads the state table and writes nothing of it.
func TestTheRuntimeRoleMayReadTheStateTableOnly(t *testing.T) {
	var entry *TablePrivilege
	for _, p := range RuntimePosture() {
		if p.Table == "restore_fence_state" {
			p := p
			entry = &p
		}
	}
	if entry == nil || !entry.Select || entry.Insert || entry.Update || entry.Delete {
		t.Fatalf("restore_fence_state posture = %+v, want SELECT only", entry)
	}
}
