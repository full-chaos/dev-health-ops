//go:build integration

package postgres

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
	"github.com/jackc/pgx/v5/pgxpool"
)

// This suite exists because domain_authorization_integration_test.go and
// domain_grant_reconciliation_integration_test.go only ever exercise
// rolePostureQuery through domainPosture() — the one manifest it was
// extracted from when Phase 2 parameterized required_table_privileges and
// column_scoped_privileges into unnest-bound query parameters. That proves
// the refactor didn't regress the domain role's own checks, but it cannot
// prove the query is genuinely parameterized rather than coincidentally
// correct for the one shape it happened to be built against, and it cannot
// prove the cross-role attribution property a multi-role deployment
// depends on. That property is distributed, not a single predicate: one
// CheckRolePosture call only ever proves ITS OWN role is clean (see
// CheckRolePosture's doc comment), so a test exercising only one leak
// direction — role A wrongly holding role B's table — would itself be the
// same half-guarantee trap in test form. This suite exercises the query
// against synthetic, non-production table names, independently of whatever
// the real two-role partition turns out to be, and checks:
//
//   - genuine parameterization (an arbitrary posture, not domainPosture's),
//   - both leak directions (A-into-B and B-into-A), and
//   - that a table legitimately required by both roles is not mistaken for
//     a leak, without loosening the check for tables that actually are
//     exclusive to one role.

func TestCheckRolePostureAcceptsAnArbitrarySyntheticPosture(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	instance, err := containers.StartPostgres(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		closeCtx, closeCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer closeCancel()
		if err := instance.Close(closeCtx); err != nil {
			t.Errorf("terminate PostgreSQL: %v", err)
		}
	})
	admin, err := pgxpool.New(ctx, instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(admin.Close)

	const (
		role     = "synthetic_posture_role"
		password = "synthetic_posture_password"
		schema   = "synthetic_river"
	)
	for _, statement := range []string{
		"REVOKE TEMPORARY ON DATABASE worker_test FROM PUBLIC",
		"REVOKE CREATE ON SCHEMA public FROM PUBLIC",
		"CREATE ROLE " + role + " LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS PASSWORD '" + password + "'",
		"GRANT CONNECT ON DATABASE worker_test TO " + role,
		"GRANT USAGE ON SCHEMA public TO " + role,
		"CREATE SCHEMA " + schema,
		"CREATE TABLE public.synthetic_alpha (id uuid PRIMARY KEY)",
		"CREATE TABLE public.synthetic_beta (id uuid PRIMARY KEY, state text)",
		"GRANT SELECT ON TABLE public.synthetic_alpha TO " + role,
		"GRANT SELECT, INSERT ON TABLE public.synthetic_beta TO " + role,
	} {
		if _, err := admin.Exec(ctx, statement); err != nil {
			t.Fatalf("%s: %v", statement, err)
		}
	}

	// A manifest with no resemblance to domainPosture's table names or
	// flag shape: proves rolePostureQuery reads its required set from the
	// parameters, not from anything baked into the query text.
	posture := RolePosture{
		RequiredTables: []TablePrivilege{
			{TableName: "synthetic_alpha", AllowInsert: false, AllowUpdate: false},
			{TableName: "synthetic_beta", AllowInsert: true, AllowUpdate: false},
		},
	}

	roleConn := connectAs(t, ctx, instance.URI, role, password)

	if err := CheckRolePosture(ctx, roleConn, role, schema, posture); err != nil {
		t.Fatalf("CheckRolePosture rejected the exact declared synthetic posture: %v", err)
	}

	// Excess privilege beyond the declared posture must fail closed for an
	// arbitrary manifest, not only for domainPosture's.
	if _, err := admin.Exec(ctx, "GRANT UPDATE ON TABLE public.synthetic_beta TO "+role); err != nil {
		t.Fatal(err)
	}
	if err := CheckRolePosture(ctx, roleConn, role, schema, posture); err == nil {
		t.Fatal("CheckRolePosture unexpectedly authorized an undeclared UPDATE on synthetic_beta")
	}
	if _, err := admin.Exec(ctx, "REVOKE UPDATE ON TABLE public.synthetic_beta FROM "+role); err != nil {
		t.Fatal(err)
	}
	if err := CheckRolePosture(ctx, roleConn, role, schema, posture); err != nil {
		t.Fatalf("CheckRolePosture did not recover after revoking the excess UPDATE: %v", err)
	}
}

func TestCheckRolePostureAttributionRejectsTheOtherRolesPrivileges(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	instance, err := containers.StartPostgres(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		closeCtx, closeCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer closeCancel()
		if err := instance.Close(closeCtx); err != nil {
			t.Errorf("terminate PostgreSQL: %v", err)
		}
	})
	admin, err := pgxpool.New(ctx, instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(admin.Close)

	const (
		roleA     = "attribution_role_a"
		roleB     = "attribution_role_b"
		passwordA = "attribution_role_a_password"
		passwordB = "attribution_role_b_password"
		schema    = "attribution_river"
	)
	for _, statement := range []string{
		"REVOKE TEMPORARY ON DATABASE worker_test FROM PUBLIC",
		"REVOKE CREATE ON SCHEMA public FROM PUBLIC",
		"CREATE ROLE " + roleA + " LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS PASSWORD '" + passwordA + "'",
		"CREATE ROLE " + roleB + " LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS PASSWORD '" + passwordB + "'",
		"GRANT CONNECT ON DATABASE worker_test TO " + roleA + ", " + roleB,
		"GRANT USAGE ON SCHEMA public TO " + roleA + ", " + roleB,
		"CREATE SCHEMA " + schema,
		"CREATE TABLE public.attribution_a_only (id uuid PRIMARY KEY)",
		"CREATE TABLE public.attribution_b_only (id uuid PRIMARY KEY)",
		"GRANT SELECT ON TABLE public.attribution_a_only TO " + roleA,
		"GRANT SELECT ON TABLE public.attribution_b_only TO " + roleB,
	} {
		if _, err := admin.Exec(ctx, statement); err != nil {
			t.Fatalf("%s: %v", statement, err)
		}
	}

	postureA := RolePosture{RequiredTables: []TablePrivilege{{TableName: "attribution_a_only"}}}
	postureB := RolePosture{RequiredTables: []TablePrivilege{{TableName: "attribution_b_only"}}}

	connA := connectAs(t, ctx, instance.URI, roleA, passwordA)
	connB := connectAs(t, ctx, instance.URI, roleB, passwordB)

	if err := CheckRolePosture(ctx, connA, roleA, schema, postureA); err != nil {
		t.Fatalf("role A rejected its own exact posture: %v", err)
	}
	if err := CheckRolePosture(ctx, connB, roleB, schema, postureB); err != nil {
		t.Fatalf("role B rejected its own exact posture: %v", err)
	}

	// The central correctness property this suite exists to pin: role A
	// does not hold role B's exclusive privilege, and vice versa. A
	// partition that accidentally assigned a table to both roles' manifests
	// (or that this test evaluated against the wrong role's declared set)
	// must fail here rather than pass silently.
	if err := CheckRolePosture(ctx, connA, roleA, schema, postureB); err == nil {
		t.Fatal("role A unexpectedly satisfied role B's posture: it must not hold attribution_b_only")
	}
	if err := CheckRolePosture(ctx, connB, roleB, schema, postureA); err == nil {
		t.Fatal("role B unexpectedly satisfied role A's posture: it must not hold attribution_a_only")
	}

	// The failure mode this property exists to catch, direction one: a
	// privilege granted to the WRONG role by mistake. Grant role A's
	// exclusive table to role B too, and role B's own posture check (still
	// only declaring attribution_b_only) must now fail —
	// other_public_relations' catch-all makes any table outside a role's own
	// manifest illegal for that role to hold at all, so a misattributed
	// grant cannot hide behind "some role has this privilege somewhere."
	if _, err := admin.Exec(ctx, "GRANT SELECT ON TABLE public.attribution_a_only TO "+roleB); err != nil {
		t.Fatal(err)
	}
	if err := CheckRolePosture(ctx, connB, roleB, schema, postureB); err == nil {
		t.Fatal("role B unexpectedly authorized while also holding role A's exclusive table — misattributed privilege went undetected")
	}
	if _, err := admin.Exec(ctx, "REVOKE SELECT ON TABLE public.attribution_a_only FROM "+roleB); err != nil {
		t.Fatal(err)
	}
	if err := CheckRolePosture(ctx, connB, roleB, schema, postureB); err != nil {
		t.Fatalf("role B did not recover after revoking the misattributed privilege: %v", err)
	}

	// Direction two, the exact mirror: a check that only exercised A-into-B
	// would itself be the "half-guarantee" trap — it would prove role B's
	// check catches a leak but say nothing about whether role A's check
	// does. Grant role B's exclusive table to role A and confirm role A's
	// own (unchanged) posture check fails the same way.
	if _, err := admin.Exec(ctx, "GRANT SELECT ON TABLE public.attribution_b_only TO "+roleA); err != nil {
		t.Fatal(err)
	}
	if err := CheckRolePosture(ctx, connA, roleA, schema, postureA); err == nil {
		t.Fatal("role A unexpectedly authorized while also holding role B's exclusive table — misattributed privilege went undetected")
	}
	if _, err := admin.Exec(ctx, "REVOKE SELECT ON TABLE public.attribution_b_only FROM "+roleA); err != nil {
		t.Fatal(err)
	}
	if err := CheckRolePosture(ctx, connA, roleA, schema, postureA); err != nil {
		t.Fatalf("role A did not recover after revoking the misattributed privilege: %v", err)
	}
}

// A table two roles both legitimately need (the concrete production case:
// internal/syncreconciler/materializer.go's Materializer.Step runs one
// transaction, as the coordinator role, spanning tables the domain role also
// needs elsewhere, because a transaction cannot cross pools) is not a leak.
// Nothing in RolePosture needs a distinct "shared" flag for this to work:
// declaring the same table in both roles' own RequiredTables/ColumnScoped is
// sufficient, because each role's check only ever asks "is this table in MY
// manifest," never "does any other role also have it." This test proves
// that composability holds — both roles pass with the shared table present
// in both manifests — while confirming the exclusive-table leak check from
// TestCheckRolePostureAttributionRejectsTheOtherRolesPrivileges still fires
// for a table that is NOT shared, so sharing one table doesn't accidentally
// widen what counts as "the other role's, so it's fine."
func TestCheckRolePostureAllowsATableRequiredByBothRoles(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	instance, err := containers.StartPostgres(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		closeCtx, closeCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer closeCancel()
		if err := instance.Close(closeCtx); err != nil {
			t.Errorf("terminate PostgreSQL: %v", err)
		}
	})
	admin, err := pgxpool.New(ctx, instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(admin.Close)

	const (
		roleA     = "shared_table_role_a"
		roleB     = "shared_table_role_b"
		passwordA = "shared_table_role_a_password"
		passwordB = "shared_table_role_b_password"
		schema    = "shared_table_river"
	)
	for _, statement := range []string{
		"REVOKE TEMPORARY ON DATABASE worker_test FROM PUBLIC",
		"REVOKE CREATE ON SCHEMA public FROM PUBLIC",
		"CREATE ROLE " + roleA + " LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS PASSWORD '" + passwordA + "'",
		"CREATE ROLE " + roleB + " LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS PASSWORD '" + passwordB + "'",
		"GRANT CONNECT ON DATABASE worker_test TO " + roleA + ", " + roleB,
		"GRANT USAGE ON SCHEMA public TO " + roleA + ", " + roleB,
		"CREATE SCHEMA " + schema,
		"CREATE TABLE public.shared_dual_granted (id uuid PRIMARY KEY)",
		"CREATE TABLE public.exclusive_a_only (id uuid PRIMARY KEY)",
		"GRANT SELECT ON TABLE public.shared_dual_granted TO " + roleA + ", " + roleB,
		"GRANT SELECT ON TABLE public.exclusive_a_only TO " + roleA,
	} {
		if _, err := admin.Exec(ctx, statement); err != nil {
			t.Fatalf("%s: %v", statement, err)
		}
	}

	// Both manifests declare the shared table; only role A also declares its
	// own exclusive table.
	postureA := RolePosture{RequiredTables: []TablePrivilege{
		{TableName: "shared_dual_granted"},
		{TableName: "exclusive_a_only"},
	}}
	postureB := RolePosture{RequiredTables: []TablePrivilege{
		{TableName: "shared_dual_granted"},
	}}

	connA := connectAs(t, ctx, instance.URI, roleA, passwordA)
	connB := connectAs(t, ctx, instance.URI, roleB, passwordB)

	if err := CheckRolePosture(ctx, connA, roleA, schema, postureA); err != nil {
		t.Fatalf("role A rejected a posture including the legitimately shared table: %v", err)
	}
	if err := CheckRolePosture(ctx, connB, roleB, schema, postureB); err != nil {
		t.Fatalf("role B rejected a posture including the legitimately shared table: %v", err)
	}

	// The exclusive-table leak check must still fire even though a shared
	// table exists in this same deployment: granting role A's EXCLUSIVE
	// table to role B is still a real misattribution, not a second
	// legitimately shared table.
	if _, err := admin.Exec(ctx, "GRANT SELECT ON TABLE public.exclusive_a_only TO "+roleB); err != nil {
		t.Fatal(err)
	}
	if err := CheckRolePosture(ctx, connB, roleB, schema, postureB); err == nil {
		t.Fatal("role B unexpectedly authorized while also holding role A's exclusive (non-shared) table")
	}
	if _, err := admin.Exec(ctx, "REVOKE SELECT ON TABLE public.exclusive_a_only FROM "+roleB); err != nil {
		t.Fatal(err)
	}
	if err := CheckRolePosture(ctx, connB, roleB, schema, postureB); err != nil {
		t.Fatalf("role B did not recover after revoking the misattributed exclusive table: %v", err)
	}
}

// grantStatementsForPosture derives GRANT statements directly from a
// RolePosture's own data — never a hand-maintained parallel list — so this
// test cannot silently drift from whatever domainPosture()/coordinatorPosture()
// actually declare. Table-wide privileges only ever include SELECT plus
// whichever of INSERT/UPDATE/DELETE the posture allows, matching
// TablePrivilege's own contract that every other table-level privilege must
// stay absent.
func grantStatementsForPosture(role string, posture RolePosture) []string {
	statements := make([]string, 0, len(posture.RequiredTables)+len(posture.ColumnScoped))
	for _, table := range posture.RequiredTables {
		privileges := []string{"SELECT"}
		if table.AllowInsert {
			privileges = append(privileges, "INSERT")
		}
		if table.AllowUpdate {
			privileges = append(privileges, "UPDATE")
		}
		if table.AllowDelete {
			privileges = append(privileges, "DELETE")
		}
		statements = append(statements, fmt.Sprintf(
			"GRANT %s ON TABLE public.%s TO %s", strings.Join(privileges, ", "), table.TableName, role,
		))
	}
	for _, column := range posture.ColumnScoped {
		statements = append(statements, fmt.Sprintf(
			"GRANT %s (%s) ON TABLE public.%s TO %s", column.Privilege, column.ColumnName, column.TableName, role,
		))
	}
	return statements
}

// TestDomainAndCoordinatorPosturesSatisfyAttributionAgainstTheRealManifest
// proves the cross-role attribution property
// TestCheckRolePostureAttributionRejectsTheOtherRolesPrivileges and
// TestCheckRolePostureAllowsATableRequiredByBothRoles already established for
// arbitrary and synthetic postures holds for the ACTUAL production partition:
// domainPosture() and coordinatorPosture(), per
// .github/docs-legacy/architecture/chaos-3033-role-partition-manifest.md. Every
// one of the eight dual-grant ("both") tables — sync_dispatch_outbox,
// sync_run_units, sync_runs, worker_job_runs, sync_dispatch_transport_routes,
// organizations, sync_configurations and worker_job_outbox — is exercised here
// with each role's real flags side by side, not a single stand-in shared table.
// (The last two joined the partition after the manifest's first pass: both are
// LOCK/FOR-UPDATE-implied UPDATEs on the coordinator side, the privilege class a
// DML-verb reading of the code cannot see.) The two postures' own grant
// statements (derived from their own data, not copy-pasted) are what get
// applied, so a future manifest change that adds, removes, or reflags a
// table is exercised by this test without any hand-maintained list here to
// fall out of sync.
func TestDomainAndCoordinatorPosturesSatisfyAttributionAgainstTheRealManifest(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	instance, err := containers.StartPostgres(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		closeCtx, closeCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer closeCancel()
		if err := instance.Close(closeCtx); err != nil {
			t.Errorf("terminate PostgreSQL: %v", err)
		}
	})
	admin, err := pgxpool.New(ctx, instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(admin.Close)

	const (
		domainRole      = "manifest_domain_role"
		coordinatorRole = "manifest_coordinator_role"
		domainPassword  = "manifest_domain_role_password"
		coordinatorPass = "manifest_coordinator_role_password"
		schema          = "manifest_attribution_river"
		domainExclusive = "integrations"
		coordinatorOnly = "worker_job_routes"
	)
	domain := domainPosture()
	coordinator := coordinatorPosture()

	// Union of every table either posture requires, generically shaped: the
	// exact production column shapes are exercised separately in
	// domain_grant_reconciliation_integration_test.go. worker_job_completion_fences
	// is the one exception, since its ColumnScoped grant needs both columns
	// to actually exist.
	tableNames := map[string]struct{}{}
	for _, table := range domain.RequiredTables {
		tableNames[table.TableName] = struct{}{}
	}
	for _, table := range coordinator.RequiredTables {
		tableNames[table.TableName] = struct{}{}
	}
	for _, column := range domain.ColumnScoped {
		tableNames[column.TableName] = struct{}{}
	}

	setup := []string{
		"REVOKE TEMPORARY ON DATABASE worker_test FROM PUBLIC",
		"REVOKE CREATE ON SCHEMA public FROM PUBLIC",
		"CREATE ROLE " + domainRole + " LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS PASSWORD '" + domainPassword + "'",
		"CREATE ROLE " + coordinatorRole + " LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS PASSWORD '" + coordinatorPass + "'",
		"GRANT CONNECT ON DATABASE worker_test TO " + domainRole + ", " + coordinatorRole,
		"GRANT USAGE ON SCHEMA public TO " + domainRole + ", " + coordinatorRole,
		"CREATE SCHEMA " + schema,
	}
	for name := range tableNames {
		if name == "worker_job_completion_fences" {
			setup = append(setup, "CREATE TABLE public.worker_job_completion_fences "+
				"(completion_key text PRIMARY KEY, completed_at timestamptz NOT NULL DEFAULT now())")
			continue
		}
		setup = append(setup, "CREATE TABLE public."+name+" (id bigint PRIMARY KEY)")
	}
	setup = append(setup, grantStatementsForPosture(domainRole, domain)...)
	setup = append(setup, grantStatementsForPosture(coordinatorRole, coordinator)...)
	for _, statement := range setup {
		if _, err := admin.Exec(ctx, statement); err != nil {
			t.Fatalf("%s: %v", statement, err)
		}
	}

	domainConn := connectAs(t, ctx, instance.URI, domainRole, domainPassword)
	coordinatorConn := connectAs(t, ctx, instance.URI, coordinatorRole, coordinatorPass)

	if err := CheckRolePosture(ctx, domainConn, domainRole, schema, domain); err != nil {
		t.Fatalf("domain role rejected the real manifest's own declared posture: %v", err)
	}
	if err := CheckRolePosture(ctx, coordinatorConn, coordinatorRole, schema, coordinator); err != nil {
		t.Fatalf("coordinator role rejected the real manifest's own declared posture: %v", err)
	}

	// Direction one: a table exclusive to domain (never declared in
	// coordinatorPosture) wrongly granted to the coordinator role must be
	// caught by the coordinator's OWN check — the same distributed property
	// TestCheckRolePostureAttributionRejectsTheOtherRolesPrivileges proved for
	// synthetic tables, now against the actual manifest.
	if _, err := admin.Exec(ctx, "GRANT SELECT ON TABLE public."+domainExclusive+" TO "+coordinatorRole); err != nil {
		t.Fatal(err)
	}
	if err := CheckRolePosture(ctx, coordinatorConn, coordinatorRole, schema, coordinator); err == nil {
		t.Fatalf("coordinator role unexpectedly authorized while also holding domain-exclusive %s", domainExclusive)
	}
	if _, err := admin.Exec(ctx, "REVOKE SELECT ON TABLE public."+domainExclusive+" FROM "+coordinatorRole); err != nil {
		t.Fatal(err)
	}
	if err := CheckRolePosture(ctx, coordinatorConn, coordinatorRole, schema, coordinator); err != nil {
		t.Fatalf("coordinator role did not recover after revoking the misattributed %s: %v", domainExclusive, err)
	}

	// Direction two, the exact mirror: a table exclusive to the coordinator
	// (never declared in domainPosture) wrongly granted to domain must be
	// caught by domain's own check.
	if _, err := admin.Exec(ctx, "GRANT SELECT ON TABLE public."+coordinatorOnly+" TO "+domainRole); err != nil {
		t.Fatal(err)
	}
	if err := CheckRolePosture(ctx, domainConn, domainRole, schema, domain); err == nil {
		t.Fatalf("domain role unexpectedly authorized while also holding coordinator-exclusive %s", coordinatorOnly)
	}
	if _, err := admin.Exec(ctx, "REVOKE SELECT ON TABLE public."+coordinatorOnly+" FROM "+domainRole); err != nil {
		t.Fatal(err)
	}
	if err := CheckRolePosture(ctx, domainConn, domainRole, schema, domain); err != nil {
		t.Fatalf("domain role did not recover after revoking the misattributed %s: %v", coordinatorOnly, err)
	}
}
