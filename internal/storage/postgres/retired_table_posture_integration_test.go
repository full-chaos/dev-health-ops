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

// RolePosture.RetiredTables: a retired table passes when it is dropped, when
// the role holds nothing on it, or when the role holds exactly its declared
// flags. A partial grant and a wider grant are refused. The same cells run
// against the same posture without RetiredTables: there a dropped table and an
// empty grant are refused, so the retired rule is what admits them.
func TestRetiredTablePostureAcceptsAbsentOrExactAndRefusesPartialOrExcess(t *testing.T) {
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
		password = "retired_posture_password"
		schema   = "retired_river"
	)
	role, err := containers.RoleName("retired_posture_role", instance)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { containers.DropRole(admin, role, t.Logf) })
	exec := func(t *testing.T, statements ...string) {
		t.Helper()
		for _, statement := range statements {
			if _, err := admin.Exec(ctx, statement); err != nil {
				t.Fatalf("%s: %v", statement, err)
			}
		}
	}
	exec(t,
		"REVOKE TEMPORARY ON DATABASE "+dbName+" FROM PUBLIC",
		"REVOKE CREATE ON SCHEMA public FROM PUBLIC",
		"CREATE ROLE "+role+" LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS PASSWORD '"+password+"'",
		"GRANT CONNECT ON DATABASE "+dbName+" TO "+role,
		"GRANT USAGE ON SCHEMA public TO "+role,
		"CREATE SCHEMA "+schema,
		"CREATE TABLE public.synthetic_live (id uuid PRIMARY KEY)",
		"GRANT SELECT ON TABLE public.synthetic_live TO "+role,
	)
	const createRetired = "CREATE TABLE public.synthetic_retired (id uuid PRIMARY KEY, stamp timestamptz)"
	retired := RolePosture{
		RequiredTables: []TablePrivilege{{TableName: "synthetic_live"}, {TableName: "synthetic_retired", AllowUpdate: true}},
		RetiredTables:  []string{"synthetic_retired"},
	}
	legacy := RolePosture{RequiredTables: retired.RequiredTables}
	roleConn := connectAs(t, ctx, instance.URI, role, password)

	const exact = "GRANT SELECT, UPDATE ON TABLE public.synthetic_retired TO "
	for _, cell := range []struct {
		name string
		// setup runs on a fresh synthetic_retired (created, nothing granted).
		setup []string
		// retiredWant and legacyWant: true = the posture holds.
		retiredWant, legacyWant bool
		// diagnoseNames, when set, is text the retired posture's diagnostic must name.
		diagnoseNames string
	}{
		{name: "absent: table dropped", setup: []string{"DROP TABLE public.synthetic_retired"}, retiredWant: true},
		{name: "absent: nothing granted", retiredWant: true},
		{name: "exact", setup: []string{exact + role}, retiredWant: true, legacyWant: true},
		{name: "excess: INSERT", setup: []string{exact + role, "GRANT INSERT ON TABLE public.synthetic_retired TO " + role},
			diagnoseNames: "INSERT"},
		{name: "excess: DELETE", setup: []string{exact + role, "GRANT DELETE ON TABLE public.synthetic_retired TO " + role},
			diagnoseNames: "DELETE"},
		{name: "excess: TRUNCATE", setup: []string{exact + role, "GRANT TRUNCATE ON TABLE public.synthetic_retired TO " + role},
			diagnoseNames: "TRUNCATE"},
		{name: "excess: SELECT with grant option", setup: []string{exact + role, "GRANT SELECT ON TABLE public.synthetic_retired TO " + role + " WITH GRANT OPTION"},
			diagnoseNames: "SELECT WITH GRANT OPTION"},
		{name: "excess: INSERT via PUBLIC", setup: []string{exact + role, "GRANT INSERT ON TABLE public.synthetic_retired TO PUBLIC"},
			diagnoseNames: "INSERT"},
		{name: "partial: SELECT only", setup: []string{"GRANT SELECT ON TABLE public.synthetic_retired TO " + role},
			diagnoseNames: "UPDATE"},
		{name: "partial: UPDATE only", setup: []string{"GRANT UPDATE ON TABLE public.synthetic_retired TO " + role},
			diagnoseNames: "SELECT"},
		{name: "partial: one column SELECT", setup: []string{"GRANT SELECT (id) ON TABLE public.synthetic_retired TO " + role},
			diagnoseNames: "SELECT"},
		{name: "partial: one column SELECT via PUBLIC", setup: []string{"GRANT SELECT (id) ON TABLE public.synthetic_retired TO PUBLIC"},
			diagnoseNames: "SELECT"},
		{name: "partial: REFERENCES only", setup: []string{"GRANT REFERENCES ON TABLE public.synthetic_retired TO " + role},
			diagnoseNames: "REFERENCES"},
	} {
		t.Run(cell.name, func(t *testing.T) {
			exec(t, createRetired)
			t.Cleanup(func() {
				if _, err := admin.Exec(ctx, "DROP TABLE IF EXISTS public.synthetic_retired"); err != nil {
					t.Errorf("drop synthetic_retired: %v", err)
				}
			})
			exec(t, cell.setup...)
			for _, side := range []struct {
				name    string
				posture RolePosture
				want    bool
			}{{"retired", retired, cell.retiredWant}, {"legacy", legacy, cell.legacyWant}} {
				err := CheckRolePosture(ctx, roleConn, role, schema, side.posture)
				if got := err == nil; got != side.want {
					t.Errorf("%s posture: holds = %v (%v), want %v", side.name, got, err, side.want)
				}
			}
			gaps, err := DiagnoseRolePosture(ctx, admin, role, retired)
			if err != nil {
				t.Fatal(err)
			}
			var rendered []string
			for _, gap := range gaps {
				rendered = append(rendered, gap.String())
			}
			switch {
			case cell.retiredWant && len(gaps) != 0:
				t.Errorf("retired posture holds but the diagnostic names gaps %q", rendered)
			case cell.diagnoseNames != "" && !strings.Contains(strings.Join(rendered, "\n"), cell.diagnoseNames):
				t.Errorf("diagnostic gaps %q do not name %q", rendered, cell.diagnoseNames)
			}
		})
	}
}
