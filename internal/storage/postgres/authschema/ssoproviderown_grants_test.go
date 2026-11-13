package authschema

import (
	"context"
	"log/slog"
	"slices"
	"testing"
)

// SSOProviderOwn is the whole of what the SSO providers switch gives the runtime role on public.sso_providers. Each
// privilege is a column list, so a new column in any list is a decision made here.
func TestSSOProviderOwnIsExactlyTheDeclaredColumns(t *testing.T) {
	g := SSOProviderOwn()
	wantInsert := []string{"id", "org_id", "name", "protocol", "status", "default_role", "is_default", "allow_idp_initiated",
		"auto_provision_users", "config", "encrypted_secrets", "allowed_domains", "created_at", "updated_at"}
	wantUpdate := []string{"name", "status", "default_role", "is_default", "allow_idp_initiated", "auto_provision_users", "config",
		"encrypted_secrets", "allowed_domains", "updated_at"}
	wantSelect := []string{"name", "last_metadata_sync_at", "last_login_at", "last_error", "last_error_at", "updated_at"}
	if !slices.Equal(g.Insert, wantInsert) || !slices.Equal(g.Update, wantUpdate) || !slices.Equal(g.Select, wantSelect) || !g.Delete {
		t.Fatalf("SSOProviderOwn = %+v", g)
	}
}

func TestSSOProviderOwnUpdateNeverTouchesTheRowIdentity(t *testing.T) {
	for _, column := range []string{"id", "org_id", "protocol", "created_at"} {
		if isSSOProviderOwnColumn(ssoProvidersTable, "UPDATE", column) {
			t.Errorf("UPDATE %s is granted: it is set by the one INSERT", column)
		}
	}
	for _, column := range SSOProviderOwn().Update {
		if !isSSOProviderOwnColumn(ssoProvidersTable, "UPDATE", column) {
			t.Errorf("UPDATE %s is not recognised", column)
		}
	}
}

func TestSSOProviderOwnSelectAddsOnlyTheProviderAnswerColumns(t *testing.T) {
	var read []string
	for _, table := range PublicReadColumns() {
		if table.Table == ssoProvidersTable {
			read = table.Columns
		}
	}
	for _, column := range SSOProviderOwn().Select {
		if slices.Contains(read, column) && column != "name" && column != "updated_at" {
			t.Errorf("SELECT %s is already in the public read pass", column)
		}
	}
	for _, column := range []string{"id", "org_id", "encrypted_secrets", "config", "status"} {
		if isSSOProviderOwnColumn(ssoProvidersTable, "SELECT", column) {
			t.Errorf("SELECT %s is declared again by the switch", column)
		}
	}
	if isSSOProviderOwnColumn("users", "SELECT", "name") || isSSOProviderOwnColumn(ssoProvidersTable, "REFERENCES", "name") ||
		isSSOProviderOwnColumn(ssoProvidersTable, "TRUNCATE", "name") {
		t.Fatal("isSSOProviderOwnColumn does not follow the table and the privilege")
	}
}

func TestSSOProviderOwnStatementsAreNothingWhileTheSwitchIsOff(t *testing.T) {
	statements, err := ssoProviderOwnStatements(context.Background(), nil, Options{RuntimeRole: "auth_rt"}, slog.Default())
	if err != nil || len(statements) != 0 {
		t.Fatalf("switch off: %v, %v; want no statement", statements, err)
	}
}

func TestSSOProviderOwnIsInThePrivilegeSetOnlyWithTheSwitch(t *testing.T) {
	has := func(set []PublicPrivilege, privilege string) bool {
		for _, entry := range set {
			if entry.Table == ssoProvidersTable && entry.Privilege == privilege && (entry.Columns == nil || slices.Contains(entry.Columns, "config")) {
				return true
			}
		}
		return false
	}
	on, off := publicPrivilegeSetFor(false, true), publicPrivilegeSetFor(false, false)
	for _, privilege := range []string{"INSERT", "DELETE"} {
		if !has(on, privilege) || has(off, privilege) {
			t.Errorf("%s on sso_providers: on=%v off=%v, want on only", privilege, has(on, privilege), has(off, privilege))
		}
	}
	if !has(on, "UPDATE") || has(off, "UPDATE") {
		t.Errorf("UPDATE of config: on=%v off=%v, want on only", has(on, "UPDATE"), has(off, "UPDATE"))
	}
}

func TestSSOProviderOwnDeleteIsExactlyTheProviderTable(t *testing.T) {
	if !isSSOProviderOwnDelete(ssoProvidersTable) {
		t.Fatal("DELETE on sso_providers is not the declared one")
	}
	for _, table := range []string{"users", "memberships", "organizations", "sso_provider", "sso_providers_x", ""} {
		if isSSOProviderOwnDelete(table) {
			t.Errorf("DELETE on %q is declared", table)
		}
	}
}

// The deferred narrowing of a switch turned off keeps the SSO own grants like the users-owned ones: the old owned pods still
// serve until the later release narrows them.
func TestDeferredNarrowingKeepsTheSSOProviderOwnGrants(t *testing.T) {
	own := SSOProviderOwn()
	if !usersOwnedPublicGrant(GrantRecord{Kind: "relation", Privilege: "DELETE", Object: ssoProvidersTable}) {
		t.Error("the table DELETE on sso_providers is not kept")
	}
	if usersOwnedPublicGrant(GrantRecord{Kind: "relation", Privilege: "DELETE", Object: ssoProvidersTable, Grantable: true}) {
		t.Error("a DELETE held WITH GRANT OPTION is kept")
	}
	for privilege, columns := range map[string][]string{"INSERT": own.Insert, "UPDATE": own.Update, "SELECT": own.Select} {
		for _, column := range columns {
			if !usersOwnedPublicGrant(GrantRecord{Kind: "column", Privilege: privilege, Object: ssoProvidersTable, Column: column}) {
				t.Errorf("%s (%s) on sso_providers is not kept", privilege, column)
			}
		}
	}
	for _, column := range []string{"id", "org_id", "protocol", "created_at"} {
		if usersOwnedPublicGrant(GrantRecord{Kind: "column", Privilege: "UPDATE", Object: ssoProvidersTable, Column: column}) {
			t.Errorf("UPDATE (%s) on sso_providers is kept: it is no SSO own grant", column)
		}
	}
}
