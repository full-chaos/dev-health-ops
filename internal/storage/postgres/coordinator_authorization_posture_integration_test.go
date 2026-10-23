//go:build integration

package postgres

import (
	"context"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

// The coordinator role runs the same posture query as the domain role against
// its own manifest. These cases pin, for the coordinator login, the ownership
// and grant-option predicates the domain suite pins for the domain login.
func TestCoordinatorAuthorizationRefusesOwnershipAndGrantableHeldPrivileges(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	admin, uri, roles := startGrantHarness(t, ctx)
	coordinator := connectAs(t, ctx, uri, roles.coordinator, grantCoordinatorPass)
	dbName, err := containers.DatabaseName(uri)
	if err != nil {
		t.Fatal(err)
	}
	role := roles.coordinator
	posture := coordinatorPosture()
	table := posture.RequiredTables[0].TableName
	column := posture.ColumnScoped[0]

	runPlantedPostures(t, ctx, admin,
		func() error { return CheckCoordinatorAuthorization(ctx, coordinator, role, grantSchema) },
		nil,
		[]plantedPosture{
			{
				name:    "owned schema with every privilege revoked",
				plant:   []string{"CREATE SCHEMA owned_probe AUTHORIZATION " + role, "REVOKE ALL ON SCHEMA owned_probe FROM " + role},
				restore: []string{"DROP SCHEMA owned_probe"},
			},
			{
				name: "owned table with every privilege revoked",
				plant: []string{
					"GRANT CREATE ON SCHEMA public TO " + role,
					"CREATE TABLE public.owned_probe_table (id integer)",
					"ALTER TABLE public.owned_probe_table OWNER TO " + role,
					"REVOKE CREATE ON SCHEMA public FROM " + role,
					"REVOKE ALL ON TABLE public.owned_probe_table FROM " + role,
				},
				restore: []string{"DROP TABLE public.owned_probe_table"},
			},
			{
				name:    "SELECT with grant option on a required table",
				plant:   []string{"GRANT SELECT ON TABLE public." + table + " TO " + role + " WITH GRANT OPTION"},
				restore: []string{"REVOKE GRANT OPTION FOR SELECT ON TABLE public." + table + " FROM " + role},
			},
			{
				name: "column privilege with grant option on a column-scoped table",
				plant: []string{
					"GRANT " + column.Privilege + " (" + column.ColumnName + ") ON TABLE public." + column.TableName + " TO " + role + " WITH GRANT OPTION",
				},
				restore: []string{
					"REVOKE GRANT OPTION FOR " + column.Privilege + " (" + column.ColumnName + ") ON TABLE public." + column.TableName + " FROM " + role,
				},
			},
			{
				name:    "CONNECT with grant option on the database",
				plant:   []string{"GRANT CONNECT ON DATABASE " + dbName + " TO " + role + " WITH GRANT OPTION"},
				restore: []string{"REVOKE GRANT OPTION FOR CONNECT ON DATABASE " + dbName + " FROM " + role},
			},
			{
				name:    "USAGE with grant option on the public schema",
				plant:   []string{"GRANT USAGE ON SCHEMA public TO " + role + " WITH GRANT OPTION"},
				restore: []string{"REVOKE GRANT OPTION FOR USAGE ON SCHEMA public FROM " + role},
			},
		},
	)
}
