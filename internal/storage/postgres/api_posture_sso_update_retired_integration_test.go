//go:build integration

package postgres

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

// apiSSOProvidersPosture is go-api's real posture (APIPostureFor, every switch on, as prod runs it) cut to its
// sso_providers entry and whatever it declares about that table, so the cells judge the shipped entry on a one-table
// catalog.
func apiSSOProvidersPosture(t *testing.T) RolePosture {
	t.Helper()
	posture := APIPostureFor(true, true)
	cut := posture
	cut.RequiredTables = nil
	for _, table := range posture.RequiredTables {
		if table.TableName == "sso_providers" {
			cut.RequiredTables = append(cut.RequiredTables, table)
		}
	}
	if len(cut.RequiredTables) != 1 {
		t.Fatalf("go-api's posture declares sso_providers %d times, want 1", len(cut.RequiredTables))
	}
	cut.ColumnScoped, cut.RetiredTables, cut.RetiredColumns, cut.RequiredSequences, cut.RequiredFunctions = nil, nil, nil, nil, nil
	return cut
}

// CHAOS-14131 step 1: go-api's readiness holds its login ready on public.sso_providers with UPDATE held (the migration of
// the running hook still grants it) or absent (the later drop), and refuses any other missing grant and any other
// excess, a grant option on UPDATE included. The grants are made by a NOSUPERUSER owner login, as the River hook makes
// them on prod; the api login is NOSUPERUSER. The diagnostic's excess (the River hook's api excess check,
// rivermigrate.checkAPIRoleExcess) names nothing for UPDATE held and still names every other excess.
func TestAPIPostureAcceptsSSOProvidersUpdateHeldOrAbsentAndNothingElse(t *testing.T) {
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
		apiPassword   = "sso_update_api_password"
		ownerPassword = "sso_update_owner_password"
		schema        = "sso_update_river"
	)
	api, err := containers.RoleName("sso_update_api", instance)
	if err != nil {
		t.Fatal(err)
	}
	owner, err := containers.RoleName("sso_update_owner", instance)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { containers.DropRole(admin, api, t.Logf) })
	t.Cleanup(func() { containers.DropRole(admin, owner, t.Logf) })
	exec := func(t *testing.T, pool *pgxpool.Pool, statements ...string) {
		t.Helper()
		for _, statement := range statements {
			if _, err := pool.Exec(ctx, statement); err != nil {
				t.Fatalf("%s: %v", statement, err)
			}
		}
	}
	exec(t, admin,
		"REVOKE TEMPORARY ON DATABASE "+dbName+" FROM PUBLIC",
		"REVOKE CREATE ON SCHEMA public FROM PUBLIC",
		"CREATE ROLE "+api+" LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS PASSWORD '"+apiPassword+"'",
		"CREATE ROLE "+owner+" LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS PASSWORD '"+ownerPassword+"'",
		"GRANT CONNECT ON DATABASE "+dbName+" TO "+api,
		"GRANT CONNECT ON DATABASE "+dbName+" TO "+owner,
		"GRANT USAGE ON SCHEMA public TO "+api,
		"GRANT USAGE, CREATE ON SCHEMA public TO "+owner,
		"CREATE SCHEMA "+schema,
	)
	ownerConn := connectAs(t, ctx, instance.URI, owner, ownerPassword)
	apiConn := connectAs(t, ctx, instance.URI, api, apiPassword)
	posture := apiSSOProvidersPosture(t)
	table := "public.sso_providers"
	for _, cell := range []struct {
		name   string
		grants []string
		ready  bool
		// excess and missing are text the diagnostic's excess or missing gaps must name; "" = none of that kind.
		excess, missing string
	}{
		{name: "UPDATE held (the running hook's grant)", grants: []string{"GRANT SELECT, UPDATE, DELETE ON TABLE " + table + " TO " + api}, ready: true},
		{name: "UPDATE absent (the drop)", grants: []string{"GRANT SELECT, DELETE ON TABLE " + table + " TO " + api}, ready: true},
		{name: "DELETE missing", grants: []string{"GRANT SELECT, UPDATE ON TABLE " + table + " TO " + api}, missing: "DELETE"},
		{name: "SELECT missing", grants: []string{"GRANT UPDATE, DELETE ON TABLE " + table + " TO " + api}, missing: "SELECT"},
		{name: "excess INSERT", grants: []string{"GRANT SELECT, UPDATE, DELETE, INSERT ON TABLE " + table + " TO " + api}, excess: "INSERT"},
		{name: "excess TRUNCATE", grants: []string{"GRANT SELECT, DELETE, TRUNCATE ON TABLE " + table + " TO " + api}, excess: "TRUNCATE"},
		{name: "excess UPDATE with grant option", grants: []string{"GRANT SELECT, DELETE ON TABLE " + table + " TO " + api,
			"GRANT UPDATE ON TABLE " + table + " TO " + api + " WITH GRANT OPTION"}, excess: "UPDATE WITH GRANT OPTION"},
		{name: "excess column UPDATE without the table UPDATE", grants: []string{"GRANT SELECT, DELETE ON TABLE " + table + " TO " + api,
			"GRANT UPDATE (status) ON TABLE " + table + " TO " + api}, excess: "UPDATE"},
		{name: "excess INSERT via PUBLIC", grants: []string{"GRANT SELECT, DELETE ON TABLE " + table + " TO " + api,
			"GRANT INSERT ON TABLE " + table + " TO PUBLIC"}, excess: "INSERT"},
	} {
		t.Run(cell.name, func(t *testing.T) {
			exec(t, ownerConn, "CREATE TABLE "+table+" (id uuid PRIMARY KEY, status text)")
			t.Cleanup(func() {
				if _, err := ownerConn.Exec(ctx, "DROP TABLE IF EXISTS "+table); err != nil {
					t.Errorf("drop %s: %v", table, err)
				}
			})
			exec(t, ownerConn, cell.grants...)
			err := CheckRolePosture(ctx, apiConn, api, schema, posture)
			if got := err == nil; got != cell.ready {
				t.Errorf("ready = %v (%v), want %v", got, err, cell.ready)
			}
			gaps, err := DiagnoseRolePosture(ctx, admin, api, posture)
			if err != nil {
				t.Fatal(err)
			}
			var excess, missing []string
			for _, gap := range gaps {
				if len(gap.Excess) > 0 {
					excess = append(excess, gap.String())
				} else {
					missing = append(missing, gap.String())
				}
			}
			if cell.excess == "" && len(excess) != 0 {
				t.Errorf("the hook excess check would fail the run: %q", excess)
			}
			if cell.excess != "" && !strings.Contains(strings.Join(excess, "\n"), cell.excess) {
				t.Errorf("excess %q does not name %q", excess, cell.excess)
			}
			if cell.missing == "" && len(missing) != 0 {
				t.Errorf("missing gaps %q, want none", missing)
			}
			if cell.missing != "" && !strings.Contains(strings.Join(missing, "\n"), cell.missing) {
				t.Errorf("missing %q does not name %q", missing, cell.missing)
			}
		})
	}
}
