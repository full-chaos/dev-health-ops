package postgres

import (
	"reflect"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/storage/roleacl"
)

func baseGrantsFor(t *testing.T) []roleacl.Grant {
	t.Helper()
	grants := []roleacl.Grant{
		{Class: "database", Object: "db", Privilege: "CONNECT"},
		{Class: "schema", Object: "public", Privilege: "USAGE"},
	}
	for _, table := range QueryAPIPosture().RequiredTables {
		grants = append(grants, roleacl.Grant{Class: "relation", Object: "public." + table.TableName, Privilege: "SELECT"})
		if table.AllowInsert {
			grants = append(grants, roleacl.Grant{Class: "relation", Object: "public." + table.TableName, Privilege: "INSERT"})
		}
		if table.AllowUpdate {
			grants = append(grants, roleacl.Grant{Class: "relation", Object: "public." + table.TableName, Privilege: "UPDATE"})
		}
		if table.AllowDelete {
			grants = append(grants, roleacl.Grant{Class: "relation", Object: "public." + table.TableName, Privilege: "DELETE"})
		}
	}
	for _, column := range QueryAPIPosture().ColumnScoped {
		grants = append(grants, roleacl.Grant{Class: "column", Object: "public." + column.TableName + "." + column.ColumnName, Privilege: column.Privilege})
	}
	return grants
}

func sessionGrantsFor(read QueryAPISessionRead) []roleacl.Grant {
	grants := []roleacl.Grant{{Class: "schema", Object: read.Schema, Privilege: "USAGE"}}
	for _, table := range read.Tables() {
		for _, column := range table.Columns {
			grants = append(grants, roleacl.Grant{Class: "column", Object: read.Schema + "." + table.Table + "." + column, Privilege: "SELECT"})
		}
	}
	return grants
}

func TestDiffQueryAPIGrantsWithSessionsRequiresAndAllowsExactlyTheSessionRead(t *testing.T) {
	t.Parallel()
	posture := QueryAPIPosture()
	read := QueryAPISessionReadFor("auth", false)
	base := baseGrantsFor(t)
	with := append(append([]roleacl.Grant(nil), base...), sessionGrantsFor(read)...)

	if got := diffQueryAPIGrantsWithSessions("db", posture, &read, with); got != "" {
		t.Fatalf("exact set with the session read: %q", got)
	}
	if got := diffQueryAPIGrantsWithSessions("db", posture, nil, base); got != "" {
		t.Fatalf("exact set without the session read: %q", got)
	}
	if got := diffQueryAPIGrantsWithSessions("db", posture, nil, with); !strings.Contains(got, "outside the manifest") {
		t.Fatalf("session read held while the flag is off must be refused: %q", got)
	}
	if got := diffQueryAPIGrantsWithSessions("db", posture, &read, base); !strings.HasPrefix(got, "lacks ") || !strings.Contains(got, "auth") {
		t.Fatalf("session read missing while the flag is on must be named: %q", got)
	}
	for name, extra := range map[string]roleacl.Grant{
		"other column":  {Class: "column", Object: "auth.sessions.token_hash", Privilege: "SELECT"},
		"other table":   {Class: "relation", Object: "auth.users", Privilege: "SELECT"},
		"table select":  {Class: "relation", Object: "auth.sessions", Privilege: "SELECT"},
		"schema create": {Class: "schema", Object: "auth", Privilege: "CREATE"},
		"update":        {Class: "column", Object: "auth.sessions.revoked_at", Privilege: "UPDATE"},
	} {
		got := diffQueryAPIGrantsWithSessions("db", posture, &read, append(append([]roleacl.Grant(nil), with...), extra))
		if !strings.Contains(got, "outside the manifest") {
			t.Errorf("%s: %q", name, got)
		}
	}
	missing := append(append([]roleacl.Grant(nil), base...), sessionGrantsFor(read)[:2]...)
	if got := diffQueryAPIGrantsWithSessions("db", posture, &read, missing); !strings.HasPrefix(got, "lacks ") {
		t.Errorf("a missing session column must be named: %q", got)
	}
}

// While auth-service owns the platform roles the manifest is the session read
// plus the platform role read, exactly; off, the platform read is outside it.
func TestQueryAPIManifestHoldsThePlatformReadOnlyWhileOwned(t *testing.T) {
	t.Parallel()
	off, on := QueryAPISessionReadFor("auth", false), QueryAPISessionReadFor("auth", true)
	session := QueryAPIAuthTable{Table: "sessions", Columns: []string{"id", "principal_id", "revoked_at", "legacy_token_version"}}
	if got := off.Tables(); !reflect.DeepEqual(got, []QueryAPIAuthTable{session}) {
		t.Fatalf("off: %+v", got)
	}
	want := []QueryAPIAuthTable{session,
		{Table: "platform_role_assignments", Columns: []string{"principal_id", "role_key", "expires_at"}},
		{Table: "principals", Columns: []string{"id", "kind", "disabled_at"}}}
	if got := on.Tables(); !reflect.DeepEqual(got, want) {
		t.Fatalf("on: %+v, want %+v", got, want)
	}
	posture := QueryAPIPosture()
	base := baseGrantsFor(t)
	withOn := append(append([]roleacl.Grant(nil), base...), sessionGrantsFor(on)...)
	withOff := append(append([]roleacl.Grant(nil), base...), sessionGrantsFor(off)...)
	if got := diffQueryAPIGrantsWithSessions("db", posture, &on, withOn); got != "" {
		t.Errorf("on, exact: %q", got)
	}
	if got := diffQueryAPIGrantsWithSessions("db", posture, &on, withOff); !strings.HasPrefix(got, "lacks ") || !strings.Contains(got, "platform_role_assignments") {
		t.Errorf("on, platform read missing: %q", got)
	}
	if got := diffQueryAPIGrantsWithSessions("db", posture, &off, withOn); !strings.Contains(got, "outside the manifest") {
		t.Errorf("off, platform read held: %q", got)
	}
	for _, extra := range []roleacl.Grant{
		{Class: "column", Object: "auth.principals.display_name", Privilege: "SELECT"},
		{Class: "column", Object: "auth.platform_role_assignments.granted_by", Privilege: "SELECT"},
	} {
		if got := diffQueryAPIGrantsWithSessions("db", posture, &on, append(append([]roleacl.Grant(nil), withOn...), extra)); !strings.Contains(got, "outside the manifest") {
			t.Errorf("on, extra %s: %q", extra.Object, got)
		}
	}
}
