package authschema

import (
	"slices"
	"testing"
)

// The runtime role records a sign-in as go-api does: last_error, last_error_at (recordSSOFailure), last_login_at
// (finishSSOLogin) and updated_at of public.sso_providers. Never the status, nothing else there, no INSERT or DELETE.
func TestSSOProviderWriteColumnsAreTheRecordColumns(t *testing.T) {
	want := []string{"last_error", "last_error_at", "last_login_at", "updated_at"}
	if got := SSOProviderWriteColumns(); !slices.Equal(got, want) {
		t.Fatalf("SSOProviderWriteColumns = %v, want %v", got, want)
	}
	for _, name := range append([]string{ssoProvidersTable}, want...) {
		if err := ValidateIdentifier(name); err != nil {
			t.Errorf("identifier %q: %v", name, err)
		}
	}
	for _, column := range want {
		if !isSSOProviderWriteColumn(ssoProvidersTable, "UPDATE", column) {
			t.Errorf("UPDATE %s is not declared", column)
		}
		for _, privilege := range []string{"SELECT", "INSERT", "DELETE", "REFERENCES"} {
			if isSSOProviderWriteColumn(ssoProvidersTable, privilege, column) {
				t.Errorf("%s %s is declared", privilege, column)
			}
		}
	}
	for _, column := range []string{"status", "id", "org_id", "config", "encrypted_secrets", "allowed_domains"} {
		if isSSOProviderWriteColumn(ssoProvidersTable, "UPDATE", column) {
			t.Errorf("UPDATE %s is declared", column)
		}
	}
	if isSSOProviderWriteColumn("users", "UPDATE", "updated_at") {
		t.Fatal("isSSOProviderWriteColumn does not follow the table")
	}
}

// The record needs the provider id to find its row: the public read pass grants SELECT (id), and no switch-gated
// grant set holds sso_providers.
func TestTheProviderFailureRecordReadsOnlyThePublicReadColumns(t *testing.T) {
	var read []string
	for _, table := range PublicReadColumns() {
		if table.Table == ssoProvidersTable {
			read = table.Columns
		}
	}
	if !slices.Contains(read, "id") {
		t.Fatalf("sso_providers read columns %v lack id", read)
	}
	for _, g := range InviteWriteGrants() {
		if g.Table == ssoProvidersTable {
			t.Fatalf("InviteWriteGrants holds %s", ssoProvidersTable)
		}
	}
	if MembershipWriteGrants().Table == ssoProvidersTable {
		t.Fatalf("MembershipWriteGrants holds %s", ssoProvidersTable)
	}
}
