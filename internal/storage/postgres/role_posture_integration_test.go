//go:build integration

package postgres

import (
	"context"
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
// depends on: this role holds exactly its declared set, and the other role
// does not hold it. Both are exercised here, independently of whatever the
// real two-role partition turns out to be.

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

	// The failure mode this property exists to catch: a privilege granted to
	// the WRONG role by mistake. Grant role A's exclusive table to role B
	// too, and role B's own posture check (still only declaring
	// attribution_b_only) must now fail — other_public_relations' catch-all
	// makes any table outside a role's own manifest illegal for that role to
	// hold at all, so a misattributed grant cannot hide behind "some role
	// has this privilege somewhere."
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
}
