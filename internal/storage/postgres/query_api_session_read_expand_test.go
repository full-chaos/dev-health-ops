package postgres

import (
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/storage/roleacl"
)

// widenedSessionTables is the session read the session rule needs (sessionrule.Revoked: the row's
// created_at and web_session_id, and the principal's password stamp), in the order auth-migrate grants it.
func widenedSessionTables(owned bool) []QueryAPIAuthTable {
	tables := []QueryAPIAuthTable{
		{Table: "sessions", Columns: []string{"id", "principal_id", "revoked_at", "legacy_token_version", "created_at", "web_session_id"}},
		{Table: "users", Columns: []string{"principal_id", "password_updated_at"}},
	}
	if owned {
		tables = append(tables,
			QueryAPIAuthTable{Table: "platform_role_assignments", Columns: []string{"principal_id", "role_key", "expires_at"}},
			QueryAPIAuthTable{Table: "principals", Columns: []string{"id", "kind", "disabled_at"}})
	}
	return tables
}

func grantsOfTables(schema string, tables []QueryAPIAuthTable) []roleacl.Grant {
	grants := []roleacl.Grant{{Class: "schema", Object: schema, Privilege: "USAGE"}}
	for _, table := range tables {
		for _, column := range table.Columns {
			grants = append(grants, roleacl.Grant{Class: "column", Object: schema + "." + table.Table + "." + column, Privilege: "SELECT"})
		}
	}
	return grants
}

func withBase(t *testing.T, extra []roleacl.Grant) []roleacl.Grant {
	return append(baseGrantsFor(t), extra...)
}

// During the expand step query-api's readiness holds the session read to one of two named sets,
// exactly: the read of today (own revoked_at) and the read the session rule needs.
func TestQueryAPIReadinessAcceptsTheOldSessionReadExactly(t *testing.T) {
	t.Parallel()
	for _, owned := range []bool{false, true} {
		read := QueryAPISessionReadFor("auth", owned)
		if got := diffQueryAPIGrantsWithSessions("db", QueryAPIPosture(), &read, withBase(t, sessionGrantsFor(read))); got != "" {
			t.Errorf("owned=%v: the old set is refused: %q", owned, got)
		}
	}
}

func TestQueryAPIReadinessAcceptsTheWidenedSessionReadExactly(t *testing.T) {
	t.Parallel()
	for _, owned := range []bool{false, true} {
		read := QueryAPISessionReadFor("auth", owned)
		if got := diffQueryAPIGrantsWithSessions("db", QueryAPIPosture(), &read, withBase(t, grantsOfTables("auth", widenedSessionTables(owned)))); got != "" {
			t.Errorf("owned=%v: the widened set is refused: %q", owned, got)
		}
	}
}

// Any third set is refused: a part of the widened read, the widened read plus one column, or the
// two named sets of different platform-role states.
func TestQueryAPIReadinessRefusesAThirdSessionRead(t *testing.T) {
	t.Parallel()
	drop := func(tables []QueryAPIAuthTable, table, column string) []QueryAPIAuthTable {
		var out []QueryAPIAuthTable
		for _, tb := range tables {
			var cols []string
			for _, c := range tb.Columns {
				if tb.Table != table || c != column {
					cols = append(cols, c)
				}
			}
			if len(cols) > 0 {
				out = append(out, QueryAPIAuthTable{Table: tb.Table, Columns: cols})
			}
		}
		return out
	}
	for _, owned := range []bool{false, true} {
		read := QueryAPISessionReadFor("auth", owned)
		widened := widenedSessionTables(owned)
		thirds := map[string][]roleacl.Grant{
			"old plus created_at only":         append(sessionGrantsFor(read), roleacl.Grant{Class: "column", Object: "auth.sessions.created_at", Privilege: "SELECT"}),
			"old plus the users read only":     append(sessionGrantsFor(read), grantsOfTables("auth", []QueryAPIAuthTable{{Table: "users", Columns: []string{"principal_id", "password_updated_at"}}})[1:]...),
			"widened without web_session_id":   grantsOfTables("auth", drop(widened, "sessions", "web_session_id")),
			"widened without password stamp":   grantsOfTables("auth", drop(widened, "users", "password_updated_at")),
			"widened plus users.password_hash": append(grantsOfTables("auth", widened), roleacl.Grant{Class: "column", Object: "auth.users.password_hash", Privilege: "SELECT"}),
			"widened plus sessions.token_hash": append(grantsOfTables("auth", widened), roleacl.Grant{Class: "column", Object: "auth.sessions.token_hash", Privilege: "SELECT"}),
			"widened plus users table SELECT":  append(grantsOfTables("auth", widened), roleacl.Grant{Class: "relation", Object: "auth.users", Privilege: "SELECT"}),
			"widened plus users UPDATE":        append(grantsOfTables("auth", widened), roleacl.Grant{Class: "column", Object: "auth.users.password_updated_at", Privilege: "UPDATE"}),
			"widened of the other roles state": grantsOfTables("auth", widenedSessionTables(!owned)),
			"widened in another schema":        grantsOfTables("auth2", widened),
		}
		for name, session := range thirds {
			if got := diffQueryAPIGrantsWithSessions("db", QueryAPIPosture(), &read, withBase(t, session)); got == "" {
				t.Errorf("owned=%v: %s is accepted", owned, name)
			} else if !strings.HasPrefix(got, "holds ") && !strings.HasPrefix(got, "lacks ") {
				t.Errorf("owned=%v: %s: %q", owned, name, got)
			}
		}
	}
}

// The widened read is accepted in the auth schema the deployment names, not a fixed one.
func TestQueryAPIReadinessAcceptsTheWidenedSessionReadInItsSchema(t *testing.T) {
	t.Parallel()
	for _, owned := range []bool{false, true} {
		read := QueryAPISessionReadFor("tenant_auth", owned)
		if got := diffQueryAPIGrantsWithSessions("db", QueryAPIPosture(), &read, withBase(t, grantsOfTables("tenant_auth", widenedSessionTables(owned)))); got != "" {
			t.Errorf("owned=%v: the widened set in tenant_auth is refused: %q", owned, got)
		}
		other := QueryAPISessionReadFor("auth", owned)
		if got := diffQueryAPIGrantsWithSessions("db", QueryAPIPosture(), &other, withBase(t, grantsOfTables("tenant_auth", widenedSessionTables(owned)))); got == "" {
			t.Errorf("owned=%v: the widened set in tenant_auth is accepted for schema auth", owned)
		}
	}
}

// A refusal names the difference against the old read and against the widened read;
// a difference outside the session read is named once.
func TestQueryAPIReadinessNamesTheDifferenceAgainstBothSessionReads(t *testing.T) {
	t.Parallel()
	read := QueryAPISessionReadFor("auth", false)
	partial := append(sessionGrantsFor(read), roleacl.Grant{Class: "column", Object: "auth.sessions.created_at", Privilege: "SELECT"})
	got := diffQueryAPIGrantsWithSessions("db", QueryAPIPosture(), &read, withBase(t, partial))
	want := "holds SELECT on column auth.sessions.created_at, which is outside the manifest" +
		"; against the widened session read: lacks SELECT on column auth.sessions.web_session_id" +
		"; expected the old session read or the widened session read"
	if got != want {
		t.Errorf("partial widen: %q, want %q", got, want)
	}
	outside := append(sessionGrantsFor(read), roleacl.Grant{Class: "relation", Object: "public.audit_log", Privilege: "SELECT"})
	got = diffQueryAPIGrantsWithSessions("db", QueryAPIPosture(), &read, withBase(t, outside))
	if want := "holds SELECT on relation public.audit_log, which is outside the manifest"; got != want {
		t.Errorf("a grant outside the session read: %q, want %q", got, want)
	}
}
