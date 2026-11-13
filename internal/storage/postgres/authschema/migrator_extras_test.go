package authschema

import (
	"slices"
	"strings"
	"testing"
)

func TestMigratorRowsOutsideTheSetNamesTheProdLeftoversAndNothingOfTheSet(t *testing.T) {
	var rows []MigratorPublicRow
	for _, entry := range PublicPrivilegeSet() {
		if entry.Columns == nil {
			rows = append(rows, MigratorPublicRow{Table: entry.Table, Privilege: entry.Privilege})
		}
		for _, column := range entry.Columns {
			rows = append(rows, MigratorPublicRow{Table: entry.Table, Privilege: entry.Privilege, Column: column})
		}
	}
	for _, table := range PublicAnchorTables() {
		rows = append(rows, MigratorPublicRow{Table: table, Privilege: PublicAnchorPrivilege})
	}
	if got := MigratorRowsOutsideTheSet(rows); len(got) != 0 {
		t.Fatalf("the set and the anchors are reported as outside the set: %v", got)
	}
	leftovers := []MigratorPublicRow{
		{Table: "users", Privilege: "INSERT", Column: "is_superuser"},
		{Table: "refresh_tokens", Privilege: "REFERENCES"},
		{Table: "refresh_tokens", Privilege: "UPDATE", Column: "revoked_at"},
		{Table: "refresh_tokens", Privilege: "SELECT", Column: "token_hash"},
		{Table: "users", Privilege: "DELETE"},
		{Table: "memberships", Privilege: "REFERENCES", Column: "id"},
	}
	got := MigratorRowsOutsideTheSet(append(rows, leftovers...))
	if len(got) != len(leftovers) {
		t.Fatalf("outside the set: %d rows %v, want %d", len(got), got, len(leftovers))
	}
	for i, row := range got {
		if row.Key() != leftovers[i].Key() {
			t.Errorf("row %d = %s, want %s", i, row.Key(), leftovers[i].Key())
		}
	}
}

func TestMigratorExtraRevokeNeverCascadesAndReadbackIsReadOnly(t *testing.T) {
	for name, text := range map[string]string{"rows": MigratorPublicRowsSQL, "dependents": migratorDependentRowsSQL, "readback": ReadbackQueryText("m")} {
		upper := strings.ToUpper(text)
		for _, word := range []string{"CASCADE", "REVOKE", "GRANT ", "INSERT ", "UPDATE ", "DELETE "} {
			if strings.Contains(upper, word) {
				t.Errorf("%s query contains %q", name, word)
			}
		}
	}
	if strings.Contains(ReadbackQueryText("a'b"), "$1") || !strings.Contains(ReadbackQueryText("a'b"), "'a''b'") {
		t.Error("the read-back text does not carry the role as an escaped literal")
	}
}

func TestADependentRowBlocksTheMatchingPrivilegeOnly(t *testing.T) {
	row := MigratorPublicRow{Table: "users", Privilege: "INSERT", Column: "is_superuser", quotedCol: "is_superuser"}
	cases := []struct {
		d    dependentRow
		want bool
	}{
		{dependentRow{"users", "INSERT", "is_superuser"}, true},
		{dependentRow{"users", "INSERT", ""}, true},
		{dependentRow{"users", "INSERT", "email"}, false},
		{dependentRow{"users", "UPDATE", "is_superuser"}, false},
		{dependentRow{"memberships", "INSERT", "is_superuser"}, false},
	}
	for _, c := range cases {
		if got := c.d.blocks(row); got != c.want {
			t.Errorf("%+v blocks = %v, want %v", c.d, got, c.want)
		}
	}
	if !slices.Contains([]string{"INSERT"}, row.Privilege) {
		t.Fatal("unreachable")
	}
}
