//go:build integration

package riverstore_test

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"

	postgresstore "github.com/full-chaos/dev-health-ops/internal/storage/postgres"
	riverstore "github.com/full-chaos/dev-health-ops/internal/storage/river"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

// The api posture declares billing_audit_log as a MIXED relation: table-wide INSERT and DELETE plus a column-scoped UPDATE of
// actor_id (CHAOS-10019: a permanent user delete nulls the actor of the kept audit rows). This applies the REAL posture through
// ApplyPinnedMigrations (the migrator's own validation and statements), then checks, as the api role, that it can set actor_id
// to null and update no other column, and that the readiness diagnostic and the migrator agree on that grant.
func TestAPIPostureBillingAuditActorGrantIsAppliedAndReadyThroughTheMigrator(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Minute)
	defer cancel()
	instance, err := containers.StartPostgres(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer closeInstance(t, instance)

	domainRole, err := containers.RoleName("api_posture_domain", instance)
	if err != nil {
		t.Fatal(err)
	}
	queueRole, err := containers.RoleName("api_posture_queue", instance)
	if err != nil {
		t.Fatal(err)
	}
	apiRole, err := containers.RoleName("api_posture_api", instance)
	if err != nil {
		t.Fatal(err)
	}
	adminPool := openPool(t, ctx, instance.URI)
	defer adminPool.Close()
	defer containers.DropRole(adminPool, domainRole, t.Logf)
	defer containers.DropRole(adminPool, queueRole, t.Logf)
	defer containers.DropRole(adminPool, apiRole, t.Logf)
	createRuntimeRoles(t, ctx, adminPool, domainRole, queueRole)
	const apiPassword = "api_posture_test_password"
	for _, statement := range []string{
		"CREATE ROLE " + apiRole + " LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS PASSWORD '" + apiPassword + "'",
		"CREATE TABLE public.alembic_version (version_num varchar(32) PRIMARY KEY)",
		`CREATE TABLE public.billing_audit_log (id uuid PRIMARY KEY, org_id uuid NOT NULL, actor_id uuid, action text NOT NULL,
description text NOT NULL, local_state json, reconciliation_status text)`,
	} {
		if _, err := adminPool.Exec(ctx, statement); err != nil {
			t.Fatalf("%s: %v", statement, err)
		}
	}
	rowID := uuid.New()
	if _, err := adminPool.Exec(ctx, `INSERT INTO public.billing_audit_log (id, org_id, actor_id, action, description) VALUES ($1, $2, $3, 'a', 'd')`,
		rowID, uuid.New(), uuid.New()); err != nil {
		t.Fatal(err)
	}

	posture := postgresstore.APIPosture()
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
	if err := riverstore.ValidateMigrationOptions(options); err != nil {
		t.Fatalf("the migrator refuses the declared api posture: %v", err)
	}
	if _, err := riverstore.ApplyPinnedMigrations(ctx, adminPool, options); err != nil {
		t.Fatalf("ApplyPinnedMigrations with the declared api posture: %v", err)
	}

	// readiness over the one relation this fixture holds: the same declaration, filtered to billing_audit_log
	ready := postgresstore.RolePosture{}
	for _, table := range posture.RequiredTables {
		if table.TableName == "billing_audit_log" {
			ready.RequiredTables = append(ready.RequiredTables, table)
		}
	}
	for _, column := range posture.ColumnScoped {
		if column.TableName == "billing_audit_log" {
			ready.ColumnScoped = append(ready.ColumnScoped, column)
		}
	}
	gaps, err := postgresstore.DiagnoseRolePosture(ctx, adminPool, apiRole, ready)
	if err != nil {
		t.Fatal(err)
	}
	if len(gaps) != 0 {
		t.Fatalf("the readiness check disagrees with what the migrator granted: %+v", gaps)
	}

	apiPool := openPool(t, ctx, roleURI(t, instance.URI, apiRole, apiPassword, mustDatabaseName(t, instance.URI)))
	defer apiPool.Close()
	for _, column := range []string{"description", "action", "org_id", "local_state", "reconciliation_status"} {
		value := "'x'"
		switch column {
		case "org_id":
			value = "'" + uuid.NewString() + "'::uuid"
		case "local_state":
			value = "'{}'::json"
		}
		_, err := apiPool.Exec(ctx, `UPDATE public.billing_audit_log SET `+column+` = `+value+` WHERE id = $1`, rowID)
		if !isInsufficientPrivilege(err) {
			t.Errorf("UPDATE of %s as the api role: %v, want 42501", column, err)
		}
	}
	if _, err := apiPool.Exec(ctx, `UPDATE public.billing_audit_log SET description = 'x', actor_id = NULL WHERE id = $1`, rowID); !isInsufficientPrivilege(err) {
		t.Errorf("an UPDATE of actor_id together with description as the api role: %v, want 42501", err)
	}
	tag, err := apiPool.Exec(ctx, `UPDATE public.billing_audit_log SET actor_id = NULL WHERE id = $1`, rowID)
	if err != nil || tag.RowsAffected() != 1 {
		t.Fatalf("UPDATE of actor_id as the api role: %v rows %d, want it allowed", err, tag.RowsAffected())
	}
}
