//go:build integration

package riverstore_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgconn"

	postgresstore "github.com/full-chaos/dev-health-ops/internal/storage/postgres"
	riverstore "github.com/full-chaos/dev-health-ops/internal/storage/river"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/pgschema"
)

// go-api's SAML ACS consumes an assertion with one INSERT into saml_assertion_replays (internal/api/sso/saml.go) as the api login.
// This applies the REAL api posture through ApplyPinnedMigrations on the real migrated schema, then runs that exact statement as
// the api role: the insert must work, a second insert of the same pair must be the 23505 the handler maps to a replay, and the
// role must hold no UPDATE and no DELETE on the table.
func TestAPIPostureLetsTheAPIRoleConsumeASAMLAssertion(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Minute)
	defer cancel()
	instance, err := containers.StartPostgres(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer closeInstance(t, instance)

	domainRole, err := containers.RoleName("api_saml_domain", instance)
	if err != nil {
		t.Fatal(err)
	}
	queueRole, err := containers.RoleName("api_saml_queue", instance)
	if err != nil {
		t.Fatal(err)
	}
	apiRole, err := containers.RoleName("api_saml_api", instance)
	if err != nil {
		t.Fatal(err)
	}
	adminPool := openPool(t, ctx, instance.URI)
	defer adminPool.Close()
	defer containers.DropRole(adminPool, domainRole, t.Logf)
	defer containers.DropRole(adminPool, queueRole, t.Logf)
	defer containers.DropRole(adminPool, apiRole, t.Logf)
	createRuntimeRoles(t, ctx, adminPool, domainRole, queueRole)
	const apiPassword = "api_saml_test_password"
	if _, err := adminPool.Exec(ctx, "CREATE ROLE "+apiRole+" LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS PASSWORD '"+apiPassword+"'"); err != nil {
		t.Fatal(err)
	}
	pgschema.Apply(ctx, t, adminPool)

	posture := postgresstore.APIPosture(false)
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
	if _, err := riverstore.ApplyPinnedMigrations(ctx, adminPool, options); err != nil {
		t.Fatalf("ApplyPinnedMigrations with the declared api posture: %v", err)
	}

	apiPool := openPool(t, ctx, roleURI(t, instance.URI, apiRole, apiPassword, mustDatabaseName(t, instance.URI)))
	defer apiPool.Close()
	providerID := uuid.New()
	const acsInsert = `INSERT INTO saml_assertion_replays (provider_id, assertion_id, expires_at)
VALUES ($1::uuid, $2, $3)`
	expires := time.Now().UTC().Add(time.Hour)
	if _, err := apiPool.Exec(ctx, acsInsert, providerID, "_assertion-1", expires); err != nil {
		t.Fatalf("the ACS replay insert as the api role: %v, want it allowed", err)
	}
	_, err = apiPool.Exec(ctx, acsInsert, providerID, "_assertion-1", expires)
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) || pgErr.Code != "23505" {
		t.Fatalf("a second insert of the same (provider, assertion) as the api role: %v, want 23505", err)
	}
	if _, err := apiPool.Exec(ctx, `UPDATE saml_assertion_replays SET expires_at = now() WHERE provider_id = $1`, providerID); !isInsufficientPrivilege(err) {
		t.Errorf("UPDATE as the api role: %v, want 42501", err)
	}
	if _, err := apiPool.Exec(ctx, `DELETE FROM saml_assertion_replays WHERE provider_id = $1`, providerID); !isInsufficientPrivilege(err) {
		t.Errorf("DELETE as the api role: %v, want 42501", err)
	}
}
