package postgres

import (
	"fmt"
	"slices"
	"strings"
	"testing"
)

// RetiredTableUpdates marks exactly the RequiredTables entries it names ($14); a name that is not exactly one entry that
// declares UPDATE, is named twice, is a retired table or is column-scoped is a posture error, never a silent pass.
func TestRetiredTableUpdateFlagsMarkExactlyTheNamedTable(t *testing.T) {
	required := []TablePrivilege{{TableName: "live"}, {TableName: "providers", AllowUpdate: true, AllowDelete: true}}
	args, err := rolePostureArgs("role_a", "river", RolePosture{RequiredTables: required, RetiredTableUpdates: []string{"providers"}})
	if err != nil {
		t.Fatal(err)
	}
	if len(args) != 14 {
		t.Fatalf("%d query arguments, want 14", len(args))
	}
	if flags, ok := args[13].([]bool); !ok || fmt.Sprint(flags) != "[false true]" {
		t.Fatalf("$14 = %#v, want [false true] (parallel to $3 %v)", args[13], args[2])
	}
	for name, posture := range map[string]RolePosture{
		"not declared":        {RequiredTables: required, RetiredTableUpdates: []string{"gone"}},
		"named twice":         {RequiredTables: required, RetiredTableUpdates: []string{"providers", "providers"}},
		"update not declared": {RequiredTables: []TablePrivilege{{TableName: "providers", AllowDelete: true}}, RetiredTableUpdates: []string{"providers"}},
		"required twice":      {RequiredTables: append(slices.Clone(required), TablePrivilege{TableName: "providers"}), RetiredTableUpdates: []string{"providers"}},
		"retired table":       {RequiredTables: required, RetiredTables: []string{"providers"}, RetiredTableUpdates: []string{"providers"}},
		"column-scoped":       {RequiredTables: required, ColumnScoped: []ColumnPrivilege{{"providers", "status", "UPDATE"}}, RetiredTableUpdates: []string{"providers"}},
	} {
		if _, err := rolePostureArgs("role_a", "river", posture); err == nil {
			t.Errorf("%s: rolePostureArgs accepted RetiredTableUpdates %v", name, posture.RetiredTableUpdates)
		}
	}
}

// The query reads $14 in the three UPDATE checks of a required table: the plain flag, the column-only refusal and the
// grant option. Losing one silently widens or narrows readiness.
func TestRolePostureQueryReadsTheRetiredTableUpdateFlag(t *testing.T) {
	for _, clause := range []string{
		"$14::boolean[]",
		"OR (NOT required.update_retired AND ('UPDATE' = ANY (held.table_level)) <> required.allow_update)",
		"OR (required.update_retired AND 'UPDATE' = ANY (held.any_column) AND NOT 'UPDATE' = ANY (held.table_level))",
		"(required.update_retired OR required.allow_update)",
	} {
		if !strings.Contains(rolePostureQuery, clause) {
			t.Errorf("rolePostureQuery omits %q", clause)
		}
	}
}

// CHAOS-14131 step 1: go-api's posture, under every switch, keeps sso_providers' RequiredTables flags as main declares
// them (SELECT + UPDATE + DELETE, so the migration grants as before) and retires the table-wide UPDATE only in
// RetiredTableUpdates; nothing else is retired for UPDATE.
func TestAPIPostureRetiresOnlyTheSSOProvidersUpdate(t *testing.T) {
	for _, switches := range [][2]bool{{false, false}, {true, false}, {false, true}, {true, true}} {
		posture := APIPostureFor(switches[0], switches[1])
		if got := fmt.Sprint(posture.RetiredTableUpdates); got != "[sso_providers]" {
			t.Errorf("%v: RetiredTableUpdates = %s, want [sso_providers]", switches, got)
		}
		for _, table := range posture.RequiredTables {
			if table.TableName == "sso_providers" && (table.AllowInsert || !table.AllowUpdate || !table.AllowDelete) {
				t.Errorf("%v: sso_providers = %+v, want main's SELECT + UPDATE + DELETE", switches, table)
			}
		}
		if _, err := rolePostureArgs("role_a", "river", posture); err != nil {
			t.Errorf("%v: %v", switches, err)
		}
	}
}
