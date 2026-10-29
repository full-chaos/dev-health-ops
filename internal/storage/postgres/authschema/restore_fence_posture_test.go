package authschema

import (
	"os"
	"regexp"
	"strings"
	"testing"
)

var sqlLineComment = regexp.MustCompile(`--[^\n]*`)

func code(sql string) string { return sqlLineComment.ReplaceAllString(sql, "") }

func migration0032(t *testing.T) string {
	t.Helper()
	migrations, err := Migrations()
	if err != nil {
		t.Fatal(err)
	}
	for _, m := range migrations {
		if m.Version == 32 {
			return code(m.SQL)
		}
	}
	t.Fatal("migration 0032 is missing")
	return ""
}

// The timeline probe is the only SECURITY DEFINER function of the lineage and
// it is as small as a definer function can be: no arguments, it returns the
// timeline id and nothing else, its search_path is pg_catalog, it reads no
// table, and PUBLIC cannot execute it.
func TestTheTimelineProbeIsTheOnlyDefinerFunctionAndNarrow(t *testing.T) {
	migrations, err := Migrations()
	if err != nil {
		t.Fatal(err)
	}
	definers := 0
	for _, m := range migrations {
		definers += strings.Count(strings.ToUpper(code(m.SQL)), "SECURITY DEFINER")
	}
	if definers != 1 {
		t.Fatalf("the lineage holds %d SECURITY DEFINER clauses, want exactly the one of restore_fence_timeline", definers)
	}
	sql := migration0032(t)
	create := regexp.MustCompile(`(?is)CREATE FUNCTION restore_fence_timeline\(\)\s+RETURNS bigint\b(.*?)AS \$\$(.*?)\$\$;`).FindStringSubmatch(sql)
	if create == nil {
		t.Fatal("restore_fence_timeline() with no arguments returning bigint is not created")
	}
	header, body := strings.ToUpper(create[1]), strings.ToUpper(create[2])
	if !strings.Contains(header, "SECURITY DEFINER") {
		t.Error("restore_fence_timeline is not SECURITY DEFINER")
	}
	if !regexp.MustCompile(`(?i)SET search_path = pg_catalog\b`).MatchString(create[1]) {
		t.Error("restore_fence_timeline does not pin search_path to pg_catalog")
	}
	for _, forbidden := range []string{"FROM", "INSERT", "UPDATE", "DELETE", "EXECUTE", "SET_CONFIG", "COPY", "SELECT"} {
		if regexp.MustCompile(`\b` + forbidden + `\b`).MatchString(body) {
			t.Errorf("the definer body holds %q: it may only compute the timeline id", forbidden)
		}
	}
	// pg_walfile_name raises on a standby: the probe answers NULL there, before
	// it reads the WAL, and a NULL timeline is never "fenced".
	recovery := strings.Index(body, "PG_IS_IN_RECOVERY()")
	wal := strings.Index(body, "PG_WALFILE_NAME")
	if recovery < 0 || wal < 0 || recovery > wal || !strings.Contains(body, "RETURN NULL") {
		t.Error("the probe must return NULL while the server is in recovery, before it reads the WAL position")
	}
	if strings.Count(strings.ToUpper(sql), "CREATE FUNCTION") != 1 {
		t.Error("migration 0032 creates another function")
	}
	if !regexp.MustCompile(`(?i)REVOKE ALL ON FUNCTION restore_fence_timeline\(\) FROM PUBLIC;`).MatchString(sql) {
		t.Error("EXECUTE is not revoked from PUBLIC")
	}
	if regexp.MustCompile(`(?i)\bGRANT\b`).MatchString(sql) {
		t.Error("the migration grants a privilege; the runtime grant lives in grants.go with the rest of the posture")
	}
}

// The runtime role gets EXECUTE on the probe and SELECT on the state table, and
// nothing that writes the state.
func TestTheRuntimeRoleMayCallTheProbeAndReadTheStateOnly(t *testing.T) {
	source, err := os.ReadFile("grants.go")
	if err != nil {
		t.Fatal(err)
	}
	text := string(source)
	if !strings.Contains(text, "GRANT EXECUTE ON FUNCTION %s.restore_fence_timeline() TO %s") {
		t.Error("grants.go does not grant EXECUTE on the probe to the runtime role")
	}
	if !strings.Contains(text, "REVOKE ALL ON FUNCTION %s.restore_fence_timeline() FROM %s") {
		t.Error("grants.go does not reset the probe's privileges for the runtime role first")
	}
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
