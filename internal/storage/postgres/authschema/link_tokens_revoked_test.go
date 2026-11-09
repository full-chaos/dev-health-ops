package authschema

import (
	"context"
	"log/slog"
	"strings"
	"testing"
)

// The grants pass revokes every privilege of the auth runtime role on go-api's
// two public link-token tables and grants nothing on them, with users owned or
// not, so a database migrated before the purge verb went loses the old grant.
func TestTheGrantsPassRevokesTheLegacyLinkTokenGrants(t *testing.T) {
	for _, owned := range []bool{true, false} {
		options := Options{Schema: "auth", RuntimeRole: "rt", UsersOwned: owned, RequirePublicTables: true}
		statements, err := legacyLinkPurgeStatements(context.Background(), presentQuerier{}, options, slog.New(slog.DiscardHandler))
		if err != nil {
			t.Fatal(err)
		}
		for _, name := range []string{"email_verification_tokens", "password_reset_tokens"} {
			table := `"public"."` + name + `"`
			revoked := false
			for _, statement := range statements {
				if statement == "REVOKE ALL ON TABLE "+table+` FROM "rt"` {
					revoked = true
				}
				if strings.HasPrefix(statement, "GRANT ") && strings.Contains(statement, " ON TABLE "+table+" ") {
					t.Errorf("owned=%v: the grants pass grants on %s: %s", owned, name, statement)
				}
			}
			if !revoked {
				t.Errorf("owned=%v: no REVOKE ALL on %s among %q", owned, table, statements)
			}
		}
	}
}

// No users-owned public grant names a legacy link-token table: a deferred
// grants pass keeps none of them.
func TestUsersOwnedPublicGrantRefusesLegacyLinkDelete(t *testing.T) {
	for _, name := range []string{"email_verification_tokens", "password_reset_tokens"} {
		for _, g := range []GrantRecord{
			{Kind: "relation", Object: name, Privilege: "DELETE"},
			{Kind: "column", Object: name, Privilege: "SELECT", Column: "id"},
		} {
			if usersOwnedPublicGrant(g) {
				t.Errorf("usersOwnedPublicGrant(%s %s %s %s) = true, want false", g.Kind, g.Object, g.Privilege, g.Column)
			}
		}
	}
	for _, privilege := range PublicPrivilegeSet() {
		if privilege.Table == "email_verification_tokens" || privilege.Table == "password_reset_tokens" {
			t.Errorf("PublicPrivilegeSet grants %s %v on %s, want no privilege", privilege.Privilege, privilege.Columns, privilege.Table)
		}
	}
}
