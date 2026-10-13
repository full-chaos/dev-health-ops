//go:build integration

package postgres

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
	"github.com/jackc/pgx/v5/pgxpool"
)

// CHAOS-6747: a table named in BOTH RequiredTables and ColumnScoped is mixed.
// RequiredTables states its table-wide privileges (SELECT here) and
// ColumnScoped adds column privileges on top (UPDATE of two columns). The
// posture must hold for exactly that and refuse every other shape, so the
// secret column stays unwritable for the role.
func TestMixedTablePostureAcceptsOnlyTheDeclaredColumnUpdates(t *testing.T) {
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
	dbName, err := containers.DatabaseName(instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	const (
		password = "mixed_posture_password"
		schema   = "mixed_river"
	)
	role, err := containers.RoleName("mixed_posture_role", instance)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { containers.DropRole(admin, role, t.Logf) })
	for _, statement := range []string{
		"REVOKE TEMPORARY ON DATABASE " + dbName + " FROM PUBLIC",
		"REVOKE CREATE ON SCHEMA public FROM PUBLIC",
		"CREATE ROLE " + role + " LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS PASSWORD '" + password + "'",
		"GRANT CONNECT ON DATABASE " + dbName + " TO " + role,
		"GRANT USAGE ON SCHEMA public TO " + role,
		"CREATE SCHEMA " + schema,
		"CREATE TABLE public.synthetic_mixed (id uuid PRIMARY KEY, flag boolean, stamp timestamptz, secret text)",
		"GRANT SELECT ON TABLE public.synthetic_mixed TO " + role,
		"GRANT UPDATE (flag, stamp) ON TABLE public.synthetic_mixed TO " + role,
	} {
		if _, err := admin.Exec(ctx, statement); err != nil {
			t.Fatalf("%s: %v", statement, err)
		}
	}
	posture := RolePosture{
		RequiredTables: []TablePrivilege{{TableName: "synthetic_mixed"}},
		ColumnScoped: []ColumnPrivilege{
			{TableName: "synthetic_mixed", ColumnName: "flag", Privilege: "UPDATE"},
			{TableName: "synthetic_mixed", ColumnName: "stamp", Privilege: "UPDATE"},
		},
	}
	roleConn := connectAs(t, ctx, instance.URI, role, password)

	if err := CheckRolePosture(ctx, roleConn, role, schema, posture); err != nil {
		t.Fatalf("exact mixed posture refused: %v", err)
	}
	gaps, err := DiagnoseRolePosture(ctx, admin, role, posture)
	if err != nil || len(gaps) != 0 {
		t.Fatalf("diagnose on the exact mixed posture = %v, %v, want no gaps", gaps, err)
	}

	for _, scenario := range []struct {
		name          string
		apply, revert string
		// diagnoseNames, when set, is text the diagnostic must name.
		diagnoseNames string
	}{
		{name: "UPDATE on the secret column",
			apply:         "GRANT UPDATE (secret) ON TABLE public.synthetic_mixed TO " + role,
			revert:        "REVOKE UPDATE (secret) ON TABLE public.synthetic_mixed FROM " + role,
			diagnoseNames: "secret"},
		{name: "UPDATE on the secret column via PUBLIC",
			apply:         "GRANT UPDATE (secret) ON TABLE public.synthetic_mixed TO PUBLIC",
			revert:        "REVOKE UPDATE (secret) ON TABLE public.synthetic_mixed FROM PUBLIC",
			diagnoseNames: "secret"},
		{name: "table-wide UPDATE",
			apply:         "GRANT UPDATE ON TABLE public.synthetic_mixed TO " + role,
			revert:        "REVOKE UPDATE ON TABLE public.synthetic_mixed FROM " + role,
			diagnoseNames: "UPDATE"},
		{name: "table-wide INSERT",
			apply:         "GRANT INSERT ON TABLE public.synthetic_mixed TO " + role,
			revert:        "REVOKE INSERT ON TABLE public.synthetic_mixed FROM " + role,
			diagnoseNames: "INSERT"},
		{name: "table-wide DELETE",
			apply:         "GRANT DELETE ON TABLE public.synthetic_mixed TO " + role,
			revert:        "REVOKE DELETE ON TABLE public.synthetic_mixed FROM " + role,
			diagnoseNames: "DELETE"},
		{name: "INSERT on a declared column",
			apply:         "GRANT INSERT (flag) ON TABLE public.synthetic_mixed TO " + role,
			revert:        "REVOKE INSERT (flag) ON TABLE public.synthetic_mixed FROM " + role,
			diagnoseNames: "flag"},
		{name: "REFERENCES on a declared column",
			apply:         "GRANT REFERENCES (flag) ON TABLE public.synthetic_mixed TO " + role,
			revert:        "REVOKE REFERENCES (flag) ON TABLE public.synthetic_mixed FROM " + role,
			diagnoseNames: "flag"},
		{name: "declared column UPDATE with grant option",
			apply:  "GRANT UPDATE (flag) ON TABLE public.synthetic_mixed TO " + role + " WITH GRANT OPTION",
			revert: "REVOKE GRANT OPTION FOR UPDATE (flag) ON TABLE public.synthetic_mixed FROM " + role},
		{name: "table-wide SELECT with grant option",
			apply:  "GRANT SELECT ON TABLE public.synthetic_mixed TO " + role + " WITH GRANT OPTION",
			revert: "REVOKE GRANT OPTION FOR SELECT ON TABLE public.synthetic_mixed FROM " + role},
		{name: "declared column UPDATE revoked",
			apply:         "REVOKE UPDATE (stamp) ON TABLE public.synthetic_mixed FROM " + role,
			revert:        "GRANT UPDATE (stamp) ON TABLE public.synthetic_mixed TO " + role,
			diagnoseNames: "stamp"},
		{name: "table-wide SELECT revoked",
			apply:         "REVOKE SELECT ON TABLE public.synthetic_mixed FROM " + role,
			revert:        "GRANT SELECT ON TABLE public.synthetic_mixed TO " + role,
			diagnoseNames: "SELECT"},
	} {
		scenario := scenario
		t.Run(scenario.name, func(t *testing.T) {
			if _, err := admin.Exec(ctx, scenario.apply); err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if _, err := admin.Exec(ctx, scenario.revert); err != nil {
					t.Errorf("revert %q: %v", scenario.revert, err)
				}
			})
			if err := CheckRolePosture(ctx, roleConn, role, schema, posture); err == nil {
				t.Fatal("posture authorized a shape beyond the declared mixed manifest")
			}
			if scenario.diagnoseNames == "" {
				return
			}
			gaps, err := DiagnoseRolePosture(ctx, admin, role, posture)
			if err != nil {
				t.Fatal(err)
			}
			var rendered []string
			for _, gap := range gaps {
				rendered = append(rendered, gap.String())
			}
			if !strings.Contains(strings.Join(rendered, "\n"), scenario.diagnoseNames) {
				t.Fatalf("diagnostic gaps %q do not name %q", rendered, scenario.diagnoseNames)
			}
		})
	}
	if err := CheckRolePosture(ctx, roleConn, role, schema, posture); err != nil {
		t.Fatalf("posture did not recover after the scenarios were reverted: %v", err)
	}
}
