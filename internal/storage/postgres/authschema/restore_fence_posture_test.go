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

// The auth schema has no SECURITY DEFINER function, with ONE named exception:
// append_external_push_lifecycle_event (migration 0038, CHAOS-10895, ruled by
// the Auth Control Plane design authority). go-api's login may not write the
// guarded outbox tables, and the commit guard is a setting any role can set, so
// the only safe door is a function that runs as a dedicated NOLOGIN owner with
// exactly the outbox and revision privileges. The exception is pinned: the one
// migration, the one function, EXECUTE revoked from PUBLIC in the same
// migration, and the owner's closed-world posture row (pushWriterViolations).
// The runtime role reads the timeline itself, so no function lends the owner's
// authority to another role.
func TestTheLineageHasNoSecurityDefinerFunction(t *testing.T) {
	migrations, err := Migrations()
	if err != nil {
		t.Fatal(err)
	}
	const allowedMigration = "aggregate_revisions_external_push_credential"
	for _, m := range migrations {
		sql := strings.ToUpper(code(m.SQL))
		count := strings.Count(sql, "SECURITY DEFINER")
		switch {
		case count == 0:
		case m.Name != allowedMigration:
			t.Errorf("migration %04d_%s declares a SECURITY DEFINER function", m.Version, m.Name)
		case count != 1 || strings.Count(sql, "CREATE FUNCTION") != 1 ||
			!strings.Contains(sql, strings.ToUpper(PushLifecycleFunction)) ||
			!strings.Contains(sql, "REVOKE ALL ON FUNCTION") || !strings.Contains(sql, "FROM PUBLIC"):
			t.Errorf("migration %04d_%s must declare exactly the one %s function and revoke EXECUTE from PUBLIC",
				m.Version, m.Name, PushLifecycleFunction)
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

// The migration writes no fence row: a database is ready only after an operator
// ran `auth-migrate restore-fence init` on it, so a restored database never
// inherits a row from a migration that ran before the backup was taken.
func TestTheRestoreFenceMigrationWritesNoRow(t *testing.T) {
	sql := restoreFenceMigration(t)
	if regexp.MustCompile(`(?i)INSERT\s+INTO\s+restore_fence_state`).MatchString(sql) {
		t.Error("the migration inserts a restore_fence_state row; the operator's init verb writes it")
	}
	if !strings.Contains(sql, "'init'") {
		t.Error("source does not allow the init verb's value")
	}
}
