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

// CHAOS-9588: every shape rolePostureQuery refuses because a declared privilege carries WITH GRANT OPTION must be
// named by DiagnoseRolePosture, so the migrate gate and the readiness check cannot disagree on it.
func TestDiagnoseRolePostureNamesEveryGrantOptionReadinessRefuses(t *testing.T) {
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
		password = "grant_option_parity_password"
		schema   = "grant_option_river"
	)
	role, err := containers.RoleName("grant_option_role", instance)
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
		"CREATE TABLE public.synthetic_rw (id uuid PRIMARY KEY, flag boolean, note text)",
		"CREATE TABLE public.synthetic_ro (id uuid PRIMARY KEY, flag boolean)",
		"CREATE TABLE public.synthetic_cols (id uuid PRIMARY KEY, flag boolean, secret text)",
		"CREATE SEQUENCE public.synthetic_seq",
		"GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE public.synthetic_rw TO " + role,
		"GRANT SELECT ON TABLE public.synthetic_ro TO " + role,
		"GRANT UPDATE (flag) ON TABLE public.synthetic_cols TO " + role,
		"GRANT USAGE ON SEQUENCE public.synthetic_seq TO " + role,
	} {
		if _, err := admin.Exec(ctx, statement); err != nil {
			t.Fatalf("%s: %v", statement, err)
		}
	}
	posture := RolePosture{
		RequiredTables: []TablePrivilege{
			{TableName: "synthetic_rw", AllowInsert: true, AllowUpdate: true, AllowDelete: true},
			{TableName: "synthetic_ro"},
		},
		ColumnScoped:      []ColumnPrivilege{{TableName: "synthetic_cols", ColumnName: "flag", Privilege: "UPDATE"}},
		RequiredSequences: []string{"synthetic_seq"},
	}
	roleConn := connectAs(t, ctx, instance.URI, role, password)

	if err := CheckRolePosture(ctx, roleConn, role, schema, posture); err != nil {
		t.Fatalf("declared posture refused: %v", err)
	}
	if gaps, err := DiagnoseRolePosture(ctx, admin, role, posture); err != nil || len(gaps) != 0 {
		t.Fatalf("diagnose on the declared posture = %v, %v, want no gaps", gaps, err)
	}

	withOption := " WITH GRANT OPTION"
	for _, scenario := range []struct {
		name, apply, revert string
		// names are the texts the rendered diagnostic must contain.
		names []string
	}{
		{"table SELECT", "GRANT SELECT ON TABLE public.synthetic_rw TO " + role + withOption,
			"REVOKE GRANT OPTION FOR SELECT ON TABLE public.synthetic_rw FROM " + role, []string{"synthetic_rw", "SELECT WITH GRANT OPTION"}},
		{"table INSERT", "GRANT INSERT ON TABLE public.synthetic_rw TO " + role + withOption,
			"REVOKE GRANT OPTION FOR INSERT ON TABLE public.synthetic_rw FROM " + role, []string{"synthetic_rw", "INSERT WITH GRANT OPTION"}},
		{"table UPDATE", "GRANT UPDATE ON TABLE public.synthetic_rw TO " + role + withOption,
			"REVOKE GRANT OPTION FOR UPDATE ON TABLE public.synthetic_rw FROM " + role, []string{"synthetic_rw", "UPDATE WITH GRANT OPTION"}},
		{"table DELETE", "GRANT DELETE ON TABLE public.synthetic_rw TO " + role + withOption,
			"REVOKE GRANT OPTION FOR DELETE ON TABLE public.synthetic_rw FROM " + role, []string{"synthetic_rw", "DELETE WITH GRANT OPTION"}},
		{"read-only table SELECT", "GRANT SELECT ON TABLE public.synthetic_ro TO " + role + withOption,
			"REVOKE GRANT OPTION FOR SELECT ON TABLE public.synthetic_ro FROM " + role, []string{"synthetic_ro", "SELECT WITH GRANT OPTION"}},
		{"column SELECT on a required table", "GRANT SELECT (note) ON TABLE public.synthetic_rw TO " + role + withOption,
			"REVOKE GRANT OPTION FOR SELECT (note) ON TABLE public.synthetic_rw FROM " + role, []string{"synthetic_rw", "SELECT WITH GRANT OPTION"}},
		{"column INSERT on a required table", "GRANT INSERT (note) ON TABLE public.synthetic_rw TO " + role + withOption,
			"REVOKE GRANT OPTION FOR INSERT (note) ON TABLE public.synthetic_rw FROM " + role, []string{"synthetic_rw", "INSERT WITH GRANT OPTION"}},
		{"column UPDATE on a required table", "GRANT UPDATE (note) ON TABLE public.synthetic_rw TO " + role + withOption,
			"REVOKE GRANT OPTION FOR UPDATE (note) ON TABLE public.synthetic_rw FROM " + role, []string{"synthetic_rw", "UPDATE WITH GRANT OPTION"}},
		{"table SELECT via PUBLIC", "GRANT SELECT ON TABLE public.synthetic_rw TO PUBLIC" + withOption,
			"REVOKE SELECT ON TABLE public.synthetic_rw FROM PUBLIC", []string{"synthetic_rw", "SELECT WITH GRANT OPTION"}},
		{"column INSERT where the table disallows INSERT", "GRANT INSERT (flag) ON TABLE public.synthetic_ro TO " + role,
			"REVOKE INSERT (flag) ON TABLE public.synthetic_ro FROM " + role, []string{"synthetic_ro", "INSERT"}},
		{"column INSERT with the option where the table disallows INSERT", "GRANT INSERT (flag) ON TABLE public.synthetic_ro TO " + role + withOption,
			"REVOKE INSERT (flag) ON TABLE public.synthetic_ro FROM " + role, []string{"synthetic_ro", "INSERT"}},
		{"column UPDATE where the table disallows UPDATE", "GRANT UPDATE (flag) ON TABLE public.synthetic_ro TO " + role,
			"REVOKE UPDATE (flag) ON TABLE public.synthetic_ro FROM " + role, []string{"synthetic_ro", "UPDATE"}},
		{"column UPDATE with the option where the table disallows UPDATE", "GRANT UPDATE (flag) ON TABLE public.synthetic_ro TO " + role + withOption,
			"REVOKE UPDATE (flag) ON TABLE public.synthetic_ro FROM " + role, []string{"synthetic_ro", "UPDATE"}},
		{"sequence USAGE", "GRANT USAGE ON SEQUENCE public.synthetic_seq TO " + role + withOption,
			"REVOKE GRANT OPTION FOR USAGE ON SEQUENCE public.synthetic_seq FROM " + role, []string{"synthetic_seq", "USAGE WITH GRANT OPTION"}},
		{"declared column UPDATE of a column-scoped table", "GRANT UPDATE (flag) ON TABLE public.synthetic_cols TO " + role + withOption,
			"REVOKE GRANT OPTION FOR UPDATE (flag) ON TABLE public.synthetic_cols FROM " + role, []string{"synthetic_cols", "flag", "UPDATE WITH GRANT OPTION"}},
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
				t.Fatal("readiness authorized a privilege held WITH GRANT OPTION")
			}
			gaps, err := DiagnoseRolePosture(ctx, admin, role, posture)
			if err != nil {
				t.Fatal(err)
			}
			var rendered []string
			for _, gap := range gaps {
				rendered = append(rendered, gap.String())
			}
			joined := strings.Join(rendered, "\n")
			if len(gaps) == 0 {
				t.Fatal("readiness refuses this role but DiagnoseRolePosture returned no gap")
			}
			for _, want := range scenario.names {
				if !strings.Contains(joined, want) {
					t.Fatalf("diagnostic gaps %q do not name %q", rendered, want)
				}
			}
		})
	}
	if err := CheckRolePosture(ctx, roleConn, role, schema, posture); err != nil {
		t.Fatalf("posture did not recover after the scenarios were reverted: %v", err)
	}
	if gaps, err := DiagnoseRolePosture(ctx, admin, role, posture); err != nil || len(gaps) != 0 {
		t.Fatalf("diagnose after the scenarios = %v, %v, want no gaps", gaps, err)
	}
}
