package authschema

import (
	"slices"
	"testing"
)

// The runtime role inserts go-api's audit_logs row (internal/api/audit.PGWriter's
// columns, error_message included): the invite rows and the OAuth sign-in
// denial row. Nothing to read, update or delete there.
func TestPublicAuditWriteColumnsAreGoAPIsAuditRowColumns(t *testing.T) {
	want := []string{"id", "org_id", "user_id", "action", "resource_type", "resource_id", "description",
		"changes", "request_metadata", "status", "error_message", "created_at"}
	if got := PublicAuditWriteColumns(); !slices.Equal(got, want) {
		t.Fatalf("PublicAuditWriteColumns = %v, want %v", got, want)
	}
	for _, name := range append([]string{publicAuditTable}, want...) {
		if err := ValidateIdentifier(name); err != nil {
			t.Errorf("identifier %q: %v", name, err)
		}
	}
	for _, privilege := range []string{"SELECT", "UPDATE", "DELETE", "REFERENCES"} {
		for _, column := range want {
			if isPublicAuditWriteColumn(publicAuditTable, privilege, column) {
				t.Errorf("%s %s is declared", privilege, column)
			}
		}
	}
	if !isPublicAuditWriteColumn(publicAuditTable, "INSERT", "error_message") || isPublicAuditWriteColumn("org_invites", "INSERT", "id") {
		t.Fatal("isPublicAuditWriteColumn does not follow the declaration")
	}
}

// No grant set but PublicAuditWriteColumns touches audit_logs, so the users-owned
// switch never revokes or narrows it.
func TestNoSwitchGatedGrantSetHoldsAuditLogs(t *testing.T) {
	for _, g := range InviteWriteGrants() {
		if g.Table == publicAuditTable {
			t.Fatalf("InviteWriteGrants holds %s", publicAuditTable)
		}
	}
	if MembershipWriteGrants().Table == publicAuditTable {
		t.Fatalf("MembershipWriteGrants holds %s", publicAuditTable)
	}
}
