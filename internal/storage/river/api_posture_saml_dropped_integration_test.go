//go:build integration

package riverstore_test

import (
	"context"
	"slices"
	"testing"
	"time"

	"github.com/google/uuid"

	postgresstore "github.com/full-chaos/dev-health-ops/internal/storage/postgres"
	riverstore "github.com/full-chaos/dev-health-ops/internal/storage/river"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/pgschema"
)

var samlUserColumns = []string{"saml_nameid_format", "saml_sp_name_qualifier"}

// stepOnePosture is p with go-api's 3 SAML grants declared and retired, as the image that retired them (CHAOS-13949) declares
// them: the posture the pods still running during the drop's roll check. A grant already declared is not added twice.
func stepOnePosture(p postgresstore.RolePosture) postgresstore.RolePosture {
	out := p
	out.RequiredTables = slices.Clone(p.RequiredTables)
	out.RetiredTables = slices.Clone(p.RetiredTables)
	out.ColumnScoped = slices.Clone(p.ColumnScoped)
	out.RetiredColumns = slices.Clone(p.RetiredColumns)
	replays := postgresstore.TablePrivilege{TableName: "saml_assertion_replays", AllowInsert: true}
	if !slices.Contains(out.RequiredTables, replays) {
		out.RequiredTables = append(out.RequiredTables, replays)
	}
	if !slices.Contains(out.RetiredTables, "saml_assertion_replays") {
		out.RetiredTables = append(out.RetiredTables, "saml_assertion_replays")
	}
	for _, column := range samlUserColumns {
		for _, privilege := range []string{"INSERT", "UPDATE"} {
			grant := postgresstore.ColumnPrivilege{TableName: "users", ColumnName: column, Privilege: privilege}
			if !slices.Contains(out.ColumnScoped, grant) {
				out.ColumnScoped = append(out.ColumnScoped, grant)
			}
			if !slices.Contains(out.RetiredColumns, grant) {
				out.RetiredColumns = append(out.RetiredColumns, grant)
			}
		}
	}
	return out
}

// After the real migration (the prod switches) a LOGIN NOSUPERUSER api role holds none of go-api's 3 SAML grants: no privilege on
// saml_assertion_replays and no INSERT or UPDATE on the 2 users SAML columns. go-api's posture passes on it and refuses a role that
// still holds them, and the step-1 posture (the pods still running during the roll) passes on it, so readiness stays green.
func TestAPIRoleHoldsNoSAMLGrantAfterTheMigration(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Minute)
	defer cancel()
	instance, err := containers.StartPostgres(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer closeInstance(t, instance)
	adminPool := openPool(t, ctx, instance.URI)
	defer adminPool.Close()
	pgschema.Apply(ctx, t, adminPool)
	posture := postgresstore.APIPostureFor(true, true)

	domainRole, err := containers.RoleName("api_saml_drop_domain", instance)
	if err != nil {
		t.Fatal(err)
	}
	queueRole, err := containers.RoleName("api_saml_drop_queue", instance)
	if err != nil {
		t.Fatal(err)
	}
	apiRole, err := containers.RoleName("api_saml_drop_api", instance)
	if err != nil {
		t.Fatal(err)
	}
	defer containers.DropRole(adminPool, domainRole, t.Logf)
	defer containers.DropRole(adminPool, queueRole, t.Logf)
	defer containers.DropRole(adminPool, apiRole, t.Logf)
	createRuntimeRoles(t, ctx, adminPool, domainRole, queueRole)
	const password = "api_saml_drop_test_password"
	if _, err := adminPool.Exec(ctx, "CREATE ROLE "+apiRole+" LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS PASSWORD '"+password+"'"); err != nil {
		t.Fatal(err)
	}
	options := riverstore.MigrationOptions{Schema: "river", DomainRole: domainRole, QueueRole: queueRole, APIRole: apiRole}
	for _, table := range posture.RequiredTables {
		options.APIGrants = append(options.APIGrants, riverstore.TableGrant{
			TableName: table.TableName, AllowInsert: table.AllowInsert, AllowUpdate: table.AllowUpdate, AllowDelete: table.AllowDelete,
		})
	}
	for _, column := range posture.ColumnScoped {
		options.APIColumnGrants = append(options.APIColumnGrants, riverstore.ColumnGrant{
			TableName: column.TableName, ColumnName: column.ColumnName, Privilege: column.Privilege,
		})
	}
	options.APISequences = append(options.APISequences, posture.RequiredSequences...)
	// The step-1 grants first (today's prod state), then the migration with this posture: what the drop's migrate hook does.
	stepOne := stepOnePosture(posture)
	first := options
	first.APIGrants, first.APIColumnGrants = nil, nil
	for _, table := range stepOne.RequiredTables {
		first.APIGrants = append(first.APIGrants, riverstore.TableGrant{
			TableName: table.TableName, AllowInsert: table.AllowInsert, AllowUpdate: table.AllowUpdate, AllowDelete: table.AllowDelete,
		})
	}
	for _, column := range stepOne.ColumnScoped {
		first.APIColumnGrants = append(first.APIColumnGrants, riverstore.ColumnGrant{
			TableName: column.TableName, ColumnName: column.ColumnName, Privilege: column.Privilege,
		})
	}
	if _, err := riverstore.ApplyPinnedMigrations(ctx, adminPool, first); err != nil {
		t.Fatalf("ApplyPinnedMigrations with the step-1 posture: %v", err)
	}
	if _, err := riverstore.ApplyPinnedMigrations(ctx, adminPool, options); err != nil {
		t.Fatalf("ApplyPinnedMigrations with the declared api posture: %v", err)
	}
	apiPool := openPool(t, ctx, roleURI(t, instance.URI, apiRole, password, mustDatabaseName(t, instance.URI)))
	defer apiPool.Close()

	held := func(sql string, args ...any) bool {
		t.Helper()
		var v bool
		if err := adminPool.QueryRow(ctx, sql, args...).Scan(&v); err != nil {
			t.Fatalf("%s: %v", sql, err)
		}
		return v
	}
	if held(`SELECT has_any_column_privilege($1, 'public.saml_assertion_replays', 'SELECT') OR
		has_table_privilege($1, 'public.saml_assertion_replays', 'INSERT') OR has_table_privilege($1, 'public.saml_assertion_replays', 'UPDATE') OR
		has_table_privilege($1, 'public.saml_assertion_replays', 'DELETE')`, apiRole) {
		t.Errorf("D1 the api role holds a privilege on saml_assertion_replays after the migration, want none")
	}
	for _, column := range samlUserColumns {
		for _, privilege := range []string{"INSERT", "UPDATE"} {
			if held(`SELECT has_column_privilege($1, 'public.users', $2, $3)`, apiRole, column, privilege) {
				t.Errorf("D1 the api role holds %s on users.%s after the migration, want none", privilege, column)
			}
		}
	}
	if err := postgresstore.CheckRolePosture(ctx, apiPool, apiRole, "river", posture); err != nil {
		t.Errorf("D2 the declared posture after its migration: %v, want pass", err)
	}
	if err := postgresstore.CheckRolePosture(ctx, apiPool, apiRole, "river", stepOne); err != nil {
		t.Errorf("D3 the step-1 posture (pods still running during the roll) after the drop's migration: %v, want pass", err)
	}
	id := uuid.New()
	if _, err := apiPool.Exec(ctx, `INSERT INTO saml_assertion_replays (provider_id, assertion_id, expires_at) VALUES ($1, $2, now())`, id, "_a"); !isInsufficientPrivilege(err) {
		t.Errorf("D5 a replay INSERT as the api role: %v, want 42501", err)
	}
	if _, err := adminPool.Exec(ctx, `GRANT SELECT, INSERT ON saml_assertion_replays TO `+apiRole+`;
GRANT INSERT (saml_nameid_format, saml_sp_name_qualifier), UPDATE (saml_nameid_format, saml_sp_name_qualifier) ON users TO `+apiRole); err != nil {
		t.Fatal(err)
	}
	if err := postgresstore.CheckRolePosture(ctx, apiPool, apiRole, "river", posture); err == nil {
		t.Errorf("D4 the declared posture with the 3 SAML grants held again: pass, want refused")
	}
}
