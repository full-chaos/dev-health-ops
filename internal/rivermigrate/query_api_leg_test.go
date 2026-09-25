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
		role, grants := queryAPILeg(env(values))
		if role != "" || grants != nil {
			t.Errorf("%s: role %q grants %v, want the leg skipped", name, role, grants)
		}
	}

	role, grants := queryAPILeg(env(map[string]string{"QUERY_API_DATABASE_ROLE": "dev_health_query_api"}))
	if role != "dev_health_query_api" {
		t.Fatalf("role %q", role)
	}
	want := make([]riverstore.TableGrant, 0)
	for _, table := range postgresstore.QueryAPIWritePosture().RequiredTables {
		want = append(want, riverstore.TableGrant{
			TableName: table.TableName, AllowInsert: table.AllowInsert,
			AllowUpdate: table.AllowUpdate, AllowDelete: table.AllowDelete,
		})
	}
	if len(want) == 0 || !reflect.DeepEqual(grants, want) {
		t.Fatalf("grants %+v, want the declared posture %+v", grants, want)
	}
	// The options the leg produces must pass the migration's own validation.
	options := riverstore.MigrationOptions{
		Schema: "river", DomainRole: "d", QueueRole: "q",
		QueryAPIRole: role, QueryAPIWriteGrants: grants,
	}
	if err := riverstore.ValidateMigrationOptions(options); err != nil {
		t.Fatalf("the derived leg does not validate: %v", err)
	}
}
