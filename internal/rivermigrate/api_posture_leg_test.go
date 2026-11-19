package rivermigrate

import (
	"testing"

	postgresstore "github.com/full-chaos/dev-health-ops/internal/storage/postgres"
	riverstore "github.com/full-chaos/dev-health-ops/internal/storage/river"
)

// The api role's grants are derived from the declared postgres.APIPosture(), and the migration's own validation must accept
// them: a posture the migrator refuses fails EVERY migrate run (the api role defaults to a name, so the leg is always built).
func TestTheDeclaredAPIPostureValidatesAsMigrationOptions(t *testing.T) {
	t.Parallel()
	grants, columns, sequences := postureGrants(postgresstore.APIPosture(false))
	if len(grants) == 0 || len(columns) == 0 {
		t.Fatalf("the api posture yielded %d table grants and %d column grants, want both", len(grants), len(columns))
	}
	options := riverstore.MigrationOptions{
		Schema: "river", DomainRole: "d", QueueRole: "q",
		APIRole: "a", APIGrants: grants, APIColumnGrants: columns, APISequences: sequences,
	}
	if err := riverstore.ValidateMigrationOptions(options); err != nil {
		t.Fatalf("the migrator refuses the declared api posture: %v", err)
	}
	for _, role := range []struct {
		name    string
		posture postgresstore.RolePosture
	}{{"coordinator", postgresstore.CoordinatorPosture()}, {"query-api", postgresstore.QueryAPIPosture()}} {
		g, c, s := postureGrants(role.posture)
		probe := riverstore.MigrationOptions{Schema: "river", DomainRole: "d", QueueRole: "q"}
		if role.name == "coordinator" {
			probe.CoordinatorRole, probe.CoordinatorGrants, probe.CoordinatorColumnGrants, probe.CoordinatorSequences = "c", g, c, s
		} else {
			probe.QueryAPIRole, probe.QueryAPIGrants, probe.QueryAPIColumnGrants = "qa", g, c
		}
		if err := riverstore.ValidateMigrationOptions(probe); err != nil {
			t.Errorf("the migrator refuses the declared %s posture: %v", role.name, err)
		}
	}
}
