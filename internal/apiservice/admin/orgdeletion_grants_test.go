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

// showGrantsFinal26_6ProdDhoApp is the prod text, verbatim: SHOW GRANTS FOR
// dho_app FINAL on ClickHouse Cloud 26.6 (service "production"), read by the
// fcs lead 2026-10-27T22:47:15Z through the read-only clickhouse MCP (FOR
// dho_app by the MCP identity, not FOR CURRENT_USER; same output shape). Source
// file lead-ch-showgrants-final-dho_app-26.6.txt, sha256
// e7c30d37aec2e9cebfd6a723652499f9b1a536528384302e6d1f22665380fbcf.
var showGrantsFinal26_6ProdDhoApp = []string{
	"GRANT SELECT, INSERT, ALTER DELETE, OPTIMIZE ON default.* TO dho_app",
	"GRANT SELECT ON system.columns TO dho_app",
	"GRANT SELECT ON system.parts TO dho_app",
	"GRANT SELECT ON system.tables TO dho_app",
}

// The prod login's real grants let it purge its database, and every fail-closed
// variant of the same text refuses.
func TestOrgDeletionGrantProblemOnTheProd26_6Text(t *testing.T) {
	if err := orgDeletionGrantProblem("default", showGrantsFinal26_6ProdDhoApp); err != nil {
		t.Fatalf("prod 26.6 text: %v, want the purge allowed", err)
	}
	without := func(old, new string) []string {
		out := append([]string(nil), showGrantsFinal26_6ProdDhoApp...)
		out[0] = strings.Replace(out[0], old, new, 1)
		return out
	}
	for name, tc := range map[string]struct {
		database string
		lines    []string
	}{
		"another database":   {"analytics", showGrantsFinal26_6ProdDhoApp},
		"no ALTER DELETE":    {"default", without("ALTER DELETE, ", "")},
		"no SELECT":          {"default", without("SELECT, ", "")},
		"partial revoke":     {"default", append(append([]string(nil), showGrantsFinal26_6ProdDhoApp...), "REVOKE ALTER DELETE ON default.teams FROM dho_app")},
		"role line only":     {"default", []string{"GRANT dho_app_role TO dho_app"}},
		"unparseable":        {"default", []string{"GRANTS FOR dho_app FINAL", "SELECT, INSERT, ALTER DELETE, OPTIMIZE ON default.*"}},
		"grant on one table": {"default", without("ON default.*", "ON default.teams")},
		"system tables only": {"default", showGrantsFinal26_6ProdDhoApp[1:]},
	} {
		if err := orgDeletionGrantProblem(tc.database, tc.lines); err == nil {
			t.Errorf("%s: the purge was allowed, want a refusal", name)
		}
	}
}
