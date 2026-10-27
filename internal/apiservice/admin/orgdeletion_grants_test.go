package admin

import (
	"strings"
	"testing"
)

// The lines are SHOW GRANTS FOR CURRENT_USER FINAL output read from a live
// clickhouse-server 26.7 (the test container image).
func TestOrgDeletionGrantProblem(t *testing.T) {
	for _, tc := range []struct {
		name  string
		lines []string
		want  string // "" = may purge; else a substring of the error
	}{
		{"container admin", []string{
			"GRANT SOURCES ON *.* TO worker_test WITH GRANT OPTION",
			"GRANT TABLE ENGINE ON * TO worker_test WITH GRANT OPTION",
			"GRANT CHECK, SHOW, SELECT, INSERT, ALTER, CREATE, DROP, UNDROP TABLE, TRUNCATE, OPTIMIZE ON *.* TO worker_test WITH GRANT OPTION",
			"GRANT CREATE USER, ALTER USER, DROP USER, CREATE ROLE, ALTER ROLE, DROP ROLE, IMPERSONATE ON * TO worker_test WITH GRANT OPTION",
		}, ""},
		{"select and alter delete on the database", []string{"GRANT SELECT, ALTER DELETE ON worker_test.* TO u_del"}, ""},
		{"a role's grants (FINAL merges them)", []string{"GRANT SELECT, ALTER DELETE ON worker_test.* TO u_role"}, ""},
		{"select and alter on everything", []string{"GRANT SELECT, ALTER ON *.* TO u_alter"}, ""},
		{"all", []string{"GRANT ALL ON worker_test.* TO u_all"}, ""},
		{"backtick-quoted database", []string{"GRANT SELECT, ALTER DELETE ON `worker_test`.* TO u"}, ""},
		{"lower case", []string{"grant select, alter delete on worker_test.* to u"}, ""},
		{"select only", []string{"GRANT SELECT ON worker_test.* TO u_sel"}, "no ALTER DELETE on worker_test.*"},
		{"no grant at all", nil, "no SELECT on worker_test.*"},
		{"role membership line only", []string{"GRANT r_del TO u_role"}, "no SELECT on worker_test.*"},
		{"another database", []string{"GRANT SELECT, ALTER DELETE ON other.* TO u"}, "no SELECT on worker_test.*"},
		{"one table only", []string{"GRANT SELECT, ALTER DELETE ON worker_test.t1 TO u"}, "no SELECT on worker_test.*"},
		{"column-level select", []string{"GRANT ALTER DELETE, SELECT(org_id) ON worker_test.* TO u"}, "no SELECT on worker_test.*"},
		{"wildcard table pattern", []string{"GRANT SELECT, ALTER DELETE ON worker_test.tab* TO u"}, "no SELECT on worker_test.*"},
		{"alter update is not alter delete", []string{"GRANT SELECT, ALTER UPDATE ON worker_test.* TO u"}, "no ALTER DELETE on worker_test.*"},
		{"partial revoke of alter delete", []string{
			"GRANT SELECT, ALTER DELETE ON worker_test.* TO u_rev",
			"REVOKE ALTER DELETE ON worker_test.t1 FROM u_rev",
		}, "partial revoke: REVOKE ALTER DELETE ON worker_test.t1 FROM u_rev"},
		{"partial revoke of a column", []string{
			"GRANT SELECT, ALTER DELETE ON worker_test.* TO u",
			"REVOKE SELECT(org_id, x) ON worker_test.t1 FROM u",
		}, "partial revoke"},
		{"revoke in another database", []string{
			"GRANT SELECT, ALTER DELETE ON *.* TO u",
			"REVOKE ALTER DELETE ON other.* FROM u",
		}, ""},
		{"revoke of an unrelated privilege", []string{
			"GRANT SELECT, ALTER DELETE ON worker_test.* TO u",
			"REVOKE INSERT ON worker_test.t1 FROM u",
		}, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := orgDeletionGrantProblem("worker_test", tc.lines)
			switch {
			case tc.want == "" && err != nil:
				t.Errorf("err = %v, want nil", err)
			case tc.want != "" && (err == nil || !strings.Contains(err.Error(), tc.want)):
				t.Errorf("err = %v, want one with %q", err, tc.want)
			}
		})
	}
}

func TestGrantPrivilegesKeepsColumnListsWhole(t *testing.T) {
	got := strings.Join(grantPrivileges("ALTER  DELETE, SELECT(a, b),insert"), "|")
	if want := "ALTER DELETE|SELECT(A, B)|INSERT"; got != want {
		t.Fatalf("grantPrivileges = %q, want %q", got, want)
	}
}
