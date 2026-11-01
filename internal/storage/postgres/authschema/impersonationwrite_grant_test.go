package authschema

import (
	"slices"
	"testing"
)

func TestImpersonationWriteColumns(t *testing.T) {
	end, start, read := ImpersonationEndColumns(), ImpersonationStartColumns(), ImpersonationLegacyReadColumns()
	if want := []string{"ended_at"}; !slices.Equal(end, want) {
		t.Fatalf("ImpersonationEndColumns = %v, want %v", end, want)
	}
	if want := []string{"id", "admin_user_id", "target_user_id", "target_org_id", "target_role", "created_at", "expires_at"}; !slices.Equal(start, want) {
		t.Fatalf("ImpersonationStartColumns = %v, want %v", start, want)
	}
	if want := []string{"target_user_id", "target_org_id"}; !slices.Equal(read, want) {
		t.Fatalf("ImpersonationLegacyReadColumns = %v, want %v", read, want)
	}
	for _, tc := range []struct {
		owned             bool
		privilege, column string
		want              bool
	}{
		{false, "UPDATE", "ended_at", true},
		{true, "UPDATE", "ended_at", true},
		{false, "INSERT", "id", false},
		{true, "INSERT", "id", true},
		{false, "SELECT", "target_org_id", false},
		{true, "SELECT", "target_org_id", true},
		{true, "UPDATE", "expires_at", false},
		{true, "INSERT", "ip_address", false},
		{true, "SELECT", "ip_address", false},
		{true, "DELETE", "id", false},
	} {
		if got := isImpersonationWriteColumn(tc.owned, impersonationTable, tc.privilege, tc.column); got != tc.want {
			t.Errorf("owned %v %s %s = %v, want %v", tc.owned, tc.privilege, tc.column, got, tc.want)
		}
	}
	if isImpersonationWriteColumn(true, "users", "UPDATE", "ended_at") {
		t.Fatal("isImpersonationWriteColumn does not follow the table")
	}
}

// A security end needs the row's id with the switch off: the public read pass
// grants it, not a switch-gated pass.
func TestImpersonationEndFindsItsRowByAReadColumn(t *testing.T) {
	for _, grant := range PublicReadColumns() {
		if grant.Table == impersonationTable {
			if !slices.Contains(grant.Columns, "id") {
				t.Fatalf("PublicReadColumns %s = %v, want id", impersonationTable, grant.Columns)
			}
			return
		}
	}
	t.Fatal("no PublicReadColumns row for the table")
}

// The flip-back release's deferred run keeps what an owned pod's Start needs,
// and nothing of the end, which no switch gates.
func TestDeferNarrowingKeepsTheStartGrants(t *testing.T) {
	for _, tc := range []struct {
		g    GrantRecord
		want bool
	}{
		{GrantRecord{Kind: "column", Object: impersonationTable, Column: "target_role", Privilege: "INSERT"}, true},
		{GrantRecord{Kind: "column", Object: impersonationTable, Column: "target_org_id", Privilege: "SELECT"}, true},
		{GrantRecord{Kind: "column", Object: impersonationTable, Column: "ended_at", Privilege: "UPDATE"}, false},
		{GrantRecord{Kind: "column", Object: impersonationTable, Column: "ip_address", Privilege: "INSERT"}, false},
	} {
		if got := usersOwnedPublicGrant(tc.g); got != tc.want {
			t.Errorf("usersOwnedPublicGrant(%s) = %v, want %v", tc.g, got, tc.want)
		}
	}
}
