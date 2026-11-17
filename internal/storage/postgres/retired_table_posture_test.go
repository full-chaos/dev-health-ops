package postgres

import (
	"errors"
	"fmt"
	"strings"
	"testing"
)

func TestRetiredTableFlagsMarkExactlyTheNamedRequiredTable(t *testing.T) {
	posture := RolePosture{
		RequiredTables: []TablePrivilege{{TableName: "live"}, {TableName: "gone", AllowUpdate: true}, {TableName: "other"}},
		RetiredTables:  []string{"gone"},
	}
	args, err := rolePostureArgs("role_a", "river", posture)
	if err != nil {
		t.Fatal(err)
	}
	if len(args) != 13 {
		t.Fatalf("rolePostureArgs gave %d parameters, want 13", len(args))
	}
	flags, ok := args[11].([]bool)
	if !ok || len(flags) != 3 || flags[0] || !flags[1] || flags[2] {
		t.Fatalf("$12 = %#v, want [false true false] (parallel to $3 %v)", args[11], args[2])
	}
	none, err := rolePostureArgs("role_a", "river", RolePosture{RequiredTables: posture.RequiredTables})
	if err != nil {
		t.Fatal(err)
	}
	if flags := none[11].([]bool); len(flags) != 3 || flags[0] || flags[1] || flags[2] {
		t.Fatalf("$12 with no retired table = %v, want all false", flags)
	}
}

func TestRetiredTableFlagsRefuseAnInconsistentPosture(t *testing.T) {
	required := []TablePrivilege{{TableName: "live"}, {TableName: "gone"}}
	for name, posture := range map[string]RolePosture{
		"not a required table": {RequiredTables: required, RetiredTables: []string{"absent"}},
		"named twice":          {RequiredTables: required, RetiredTables: []string{"gone", "gone"}},
		"required twice":       {RequiredTables: append(append([]TablePrivilege{}, required...), TablePrivilege{TableName: "gone"}), RetiredTables: []string{"gone"}},
		"column-scoped":        {RequiredTables: required, ColumnScoped: []ColumnPrivilege{{"gone", "id", "UPDATE"}}, RetiredTables: []string{"gone"}},
	} {
		_, err := rolePostureArgs("role_a", "river", posture)
		if err == nil || errors.Is(err, ErrUnavailable) || !strings.Contains(err.Error(), `"gone"`) && !strings.Contains(err.Error(), `"absent"`) {
			t.Errorf("%s: rolePostureArgs error = %v, want a construction error naming the table", name, err)
		}
	}
}

// The api and coordinator roles keep the exact internal_service_credentials
// grant while the table is retired: no code of theirs uses it any more.
func TestInternalServiceCredentialsIsRetiredForTheAPIAndCoordinatorRoles(t *testing.T) {
	for name, posture := range map[string]RolePosture{
		"api":                APIPostureFor(false, false),
		"api sessions owned": APIPostureFor(true, true),
		"coordinator":        CoordinatorPosture(),
	} {
		want := map[string]TablePrivilege{
			"api":                {"internal_service_credentials", false, true, false},
			"api sessions owned": {"internal_service_credentials", false, true, false},
			"coordinator":        {"internal_service_credentials", false, true, false},
		}[name]
		var found []TablePrivilege
		for _, table := range posture.RequiredTables {
			if table.TableName == "internal_service_credentials" {
				found = append(found, table)
			}
		}
		if len(found) != 1 || found[0] != want {
			t.Errorf("%s: RequiredTables rows for the table = %v, want exactly %v", name, found, want)
		}
		// The api role also retires go-api's SAML replay grant (CHAOS-13949); the coordinator only this table.
		wantRetired := map[string]string{"api": "[internal_service_credentials saml_assertion_replays]",
			"api sessions owned": "[internal_service_credentials saml_assertion_replays]", "coordinator": "[internal_service_credentials]"}[name]
		if got := fmt.Sprint(posture.RetiredTables); got != wantRetired {
			t.Errorf("%s: RetiredTables = %v, want %s", name, got, wantRetired)
		}
		if _, err := rolePostureArgs("role_a", "river", posture); err != nil {
			t.Errorf("%s: %v", name, err)
		}
	}
	for name, posture := range map[string]RolePosture{"domain": DomainPosture(), "queue": QueuePosture(), "query-api": QueryAPIPosture()} {
		if len(posture.RetiredTables) != 0 {
			t.Errorf("%s: RetiredTables = %v, want none", name, posture.RetiredTables)
		}
	}
}

// The retired rule is two clauses of rolePostureQuery; the live cells are in
// retired_table_posture_integration_test.go.
func TestRolePostureQueryCarriesTheRetiredTableClauses(t *testing.T) {
	for _, clause := range []string{
		"SELECT count(*) FROM required_tables WHERE NOT retired",
		"SELECT count(*) FROM required_table_privileges WHERE NOT retired",
		"WHERE NOT (required.retired AND cardinality(held.any_column) = 0)",
	} {
		if !strings.Contains(rolePostureQuery, clause) {
			t.Errorf("rolePostureQuery omits %q", clause)
		}
	}
}

// RetiredColumns marks exactly the ColumnScoped entries it names ($13); a name that is not exactly one entry, or is named twice,
// is a posture error, never a silent pass.
func TestRetiredColumnFlagsMarkExactlyTheNamedColumnPrivilege(t *testing.T) {
	scoped := []ColumnPrivilege{{"users", "email", "INSERT"}, {"users", "saml_nameid_format", "INSERT"}, {"users", "saml_nameid_format", "UPDATE"}}
	args, err := rolePostureArgs("role_a", "river", RolePosture{ColumnScoped: scoped, RetiredColumns: []ColumnPrivilege{{"users", "saml_nameid_format", "UPDATE"}}})
	if err != nil {
		t.Fatal(err)
	}
	if flags, ok := args[12].([]bool); !ok || fmt.Sprint(flags) != "[false false true]" {
		t.Fatalf("$13 = %#v, want [false false true] (parallel to $6 %v)", args[12], args[5])
	}
	for name, retired := range map[string][]ColumnPrivilege{
		"not declared":    {{"users", "saml_sp_name_qualifier", "INSERT"}},
		"other privilege": {{"users", "email", "UPDATE"}},
		"named twice":     {{"users", "email", "INSERT"}, {"users", "email", "INSERT"}},
	} {
		if _, err := rolePostureArgs("role_a", "river", RolePosture{ColumnScoped: scoped, RetiredColumns: retired}); err == nil {
			t.Errorf("%s: rolePostureArgs accepted RetiredColumns %v", name, retired)
		}
	}
	for name, posture := range map[string]RolePosture{"api owned": APIPostureFor(true, true), "api not owned": APIPostureFor(true, false)} {
		if _, err := rolePostureArgs("role_a", "river", posture); err != nil {
			t.Errorf("%s: %v", name, err)
		}
	}
	if got := fmt.Sprint(APIPostureFor(true, true).RetiredColumns); got != "[{users saml_nameid_format INSERT} {users saml_nameid_format UPDATE} {users saml_sp_name_qualifier INSERT} {users saml_sp_name_qualifier UPDATE}]" {
		t.Errorf("api owned RetiredColumns = %s", got)
	}
	if got := APIPostureFor(true, false).RetiredColumns; len(got) != 0 {
		t.Errorf("api not owned RetiredColumns = %v, want none (users grants are table-wide there)", got)
	}
}
