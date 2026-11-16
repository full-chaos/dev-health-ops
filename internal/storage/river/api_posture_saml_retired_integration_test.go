//go:build integration

package riverstore_test

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	postgresstore "github.com/full-chaos/dev-health-ops/internal/storage/postgres"
	riverstore "github.com/full-chaos/dev-health-ops/internal/storage/river"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/pgschema"
)

// The 3 go-api grants only go-api's retired SAML sign-in code used: INSERT on saml_assertion_replays and INSERT + UPDATE on the users
// columns saml_nameid_format and saml_sp_name_qualifier. Revoking them is what the later step's migration does: REVOKE ALL on the
// table (its SELECT goes with it: the table leaves the manifest) and the 4 column grants.
const retiredSAMLGrantsSQL = `REVOKE ALL ON saml_assertion_replays FROM %[1]s;
REVOKE INSERT (saml_nameid_format, saml_sp_name_qualifier), UPDATE (saml_nameid_format, saml_sp_name_qualifier) ON users FROM %[1]s`

// stepTwoPosture is the posture with the 3 retired grants removed (the later step that lets the migration revoke them).
func stepTwoPosture(p postgresstore.RolePosture) postgresstore.RolePosture {
	out := p
	out.RequiredTables = nil
	for _, table := range p.RequiredTables {
		if table.TableName != "saml_assertion_replays" {
			out.RequiredTables = append(out.RequiredTables, table)
		}
	}
	out.RetiredTables = nil
	for _, name := range p.RetiredTables {
		if name != "saml_assertion_replays" {
			out.RetiredTables = append(out.RetiredTables, name)
		}
	}
	out.ColumnScoped = nil
	for _, column := range p.ColumnScoped {
		if column.TableName == "users" && (column.ColumnName == "saml_nameid_format" || column.ColumnName == "saml_sp_name_qualifier") {
			continue
		}
		out.ColumnScoped = append(out.ColumnScoped, column)
	}
	out.RetiredColumns = nil
	for _, column := range p.RetiredColumns {
		if column.TableName == "users" && (column.ColumnName == "saml_nameid_format" || column.ColumnName == "saml_sp_name_qualifier") {
			continue
		}
		out.RetiredColumns = append(out.RetiredColumns, column)
	}
	return out
}

// Through the real migration (the prod switches: sessions and platform roles owned by auth-service) as a LOGIN NOSUPERUSER api role:
// go-api's readiness posture accepts the 3 retired SAML grants held exactly (today's state) AND revoked (the state after the later
// step), refuses a wider grant on them, and the later step's posture refuses a role that still holds them.
func TestAPIPostureAcceptsTheRetiredSAMLGrantsHeldOrRevoked(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
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

	// fresh migrates a new api role with the real posture and returns it with a pool logged in as it.
	fresh := func(name string) (string, *pgxpool.Pool) {
		t.Helper()
		domainRole, err := containers.RoleName(name+"_domain", instance)
		if err != nil {
			t.Fatal(err)
		}
		queueRole, err := containers.RoleName(name+"_queue", instance)
		if err != nil {
			t.Fatal(err)
		}
		apiRole, err := containers.RoleName(name+"_api", instance)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			containers.DropRole(adminPool, apiRole, t.Logf)
			containers.DropRole(adminPool, queueRole, t.Logf)
			containers.DropRole(adminPool, domainRole, t.Logf)
		})
		createRuntimeRoles(t, ctx, adminPool, domainRole, queueRole)
		const password = "api_saml_retired_test_password"
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
		if _, err := riverstore.ApplyPinnedMigrations(ctx, adminPool, options); err != nil {
			t.Fatalf("ApplyPinnedMigrations with the declared api posture: %v", err)
		}
		pool := openPool(t, ctx, roleURI(t, instance.URI, apiRole, password, mustDatabaseName(t, instance.URI)))
		t.Cleanup(pool.Close)
		return apiRole, pool
	}
	exec := func(sql string) {
		t.Helper()
		if _, err := adminPool.Exec(ctx, sql); err != nil {
			t.Fatalf("%s: %v", sql, err)
		}
	}
	check := func(cell string, pool *pgxpool.Pool, role string, p postgresstore.RolePosture, wantPass bool) {
		t.Helper()
		err := postgresstore.CheckRolePosture(ctx, pool, role, "river", p)
		if (err == nil) != wantPass {
			t.Errorf("%s: CheckRolePosture = %v, want pass=%v", cell, err, wantPass)
		}
	}
	sprintf := func(format, role string) string { return fmt.Sprintf(format, role) }

	role, pool := fresh("ret_held")
	check("C2 held exactly (today's grants)", pool, role, posture, true)
	check("C4a later-step posture vs the 3 still held", pool, role, stepTwoPosture(posture), false)

	role, pool = fresh("ret_gone")
	exec(sprintf(retiredSAMLGrantsSQL, role))
	check("C1 the 3 revoked (the state after the later step)", pool, role, posture, true)
	check("C4b later-step posture vs the 3 revoked", pool, role, stepTwoPosture(posture), true)
	// C5: go-api's users writes need neither column: as the narrowed role an INSERT and an UPDATE naming only the other granted
	// columns work, and the retired columns are refused (the revoke took effect).
	id := uuid.New()
	if _, err := pool.Exec(ctx, `INSERT INTO users (id, email, auth_provider, is_active, is_verified, token_version, created_at, updated_at)
VALUES ($1, $2, 'oidc', true, true, 0, now(), now())`, id, "retired-"+id.String()[:8]+"@example.com"); err != nil {
		t.Errorf("C5 users INSERT without the 2 columns as the narrowed role: %v", err)
	}
	if _, err := pool.Exec(ctx, `UPDATE users SET full_name = 'n', last_login_at = now(), updated_at = now() WHERE id = $1`, id); err != nil {
		t.Errorf("C5 users UPDATE without the 2 columns as the narrowed role: %v", err)
	}
	if _, err := pool.Exec(ctx, `UPDATE users SET saml_nameid_format = 'x' WHERE id = $1`, id); !isInsufficientPrivilege(err) {
		t.Errorf("C5 users UPDATE of saml_nameid_format as the narrowed role: %v, want 42501", err)
	}

	role, pool = fresh("ret_table")
	exec(sprintf(`REVOKE ALL ON saml_assertion_replays FROM %[1]s`, role))
	check("C1a only the table grant revoked", pool, role, posture, true)

	role, pool = fresh("ret_cols")
	exec(sprintf(`REVOKE INSERT (saml_nameid_format, saml_sp_name_qualifier), UPDATE (saml_nameid_format, saml_sp_name_qualifier) ON users FROM %[1]s`, role))
	check("C1b only the 2 column grants revoked", pool, role, posture, true)

	role, pool = fresh("ret_half")
	exec(sprintf(`REVOKE UPDATE (saml_sp_name_qualifier) ON users FROM %[1]s`, role))
	check("C1c one of the 4 column grants revoked", pool, role, posture, true)

	for cell, wider := range map[string]string{
		"C3a UPDATE on saml_assertion_replays":                   `GRANT UPDATE ON saml_assertion_replays TO %[1]s`,
		"C3b INSERT on saml_assertion_replays WITH GRANT OPTION": `GRANT INSERT ON saml_assertion_replays TO %[1]s WITH GRANT OPTION`,
		"C3c a retired column WITH GRANT OPTION":                 `GRANT INSERT (saml_nameid_format) ON users TO %[1]s WITH GRANT OPTION`,
		"C3d REFERENCES on a retired column":                     `GRANT REFERENCES (saml_nameid_format) ON users TO %[1]s`,
	} {
		role, pool = fresh("ret_wide_" + strings.ToLower(cell[:3]))
		exec(sprintf(wider, role))
		check(cell, pool, role, posture, false)
	}
}
