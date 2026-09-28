package rivermigrate

import (
	"reflect"
	"testing"

	postgresstore "github.com/full-chaos/dev-health-ops/internal/storage/postgres"
	riverstore "github.com/full-chaos/dev-health-ops/internal/storage/river"
)

func TestQueryAPILegIsOptInAndDerivesFromTheDeclaredPosture(t *testing.T) {
	t.Parallel()
	env := func(values map[string]string) func(string) (string, bool) {
		return func(key string) (string, bool) { value, ok := values[key]; return value, ok }
	}
	for name, values := range map[string]map[string]string{
		"unset": {}, "empty": {"QUERY_API_DATABASE_ROLE": ""}, "blank": {"QUERY_API_DATABASE_ROLE": " \t"},
	} {
		role, grants, columns := queryAPILeg(env(values))
		if role != "" || grants != nil || columns != nil {
			t.Errorf("%s: role %q grants %v columns %v, want the leg skipped", name, role, grants, columns)
		}
	}

	role, grants, columns := queryAPILeg(env(map[string]string{"QUERY_API_DATABASE_ROLE": "dev_health_query_api"}))
	if role != "dev_health_query_api" {
		t.Fatalf("role %q", role)
	}
	want := make([]riverstore.TableGrant, 0)
	for _, table := range postgresstore.QueryAPIPosture().RequiredTables {
		want = append(want, riverstore.TableGrant{
			TableName: table.TableName, AllowInsert: table.AllowInsert,
			AllowUpdate: table.AllowUpdate, AllowDelete: table.AllowDelete,
		})
	}
	if len(want) == 0 || !reflect.DeepEqual(grants, want) {
		t.Fatalf("grants %+v, want the declared posture %+v", grants, want)
	}
	wantColumns := make([]riverstore.ColumnGrant, 0)
	for _, column := range postgresstore.QueryAPIPosture().ColumnScoped {
		wantColumns = append(wantColumns, riverstore.ColumnGrant{
			TableName: column.TableName, ColumnName: column.ColumnName, Privilege: column.Privilege,
		})
	}
	if len(wantColumns) == 0 || !reflect.DeepEqual(columns, wantColumns) {
		t.Fatalf("columns %+v, want the declared posture %+v", columns, wantColumns)
	}
	// The options the leg produces must pass the migration's own validation.
	options := riverstore.MigrationOptions{
		Schema: "river", DomainRole: "d", QueueRole: "q",
		QueryAPIRole: role, QueryAPIGrants: grants, QueryAPIColumnGrants: columns,
	}
	if err := riverstore.ValidateMigrationOptions(options); err != nil {
		t.Fatalf("the derived leg does not validate: %v", err)
	}
}
