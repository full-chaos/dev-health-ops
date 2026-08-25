//go:build integration

package postgres

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
	"github.com/jackc/pgx/v5/pgxpool"
)

// This suite exists because of the CHAOS-4261 prod incident:
// scripts/worker/provision_river_roles.sql used to REVOKE ALL PRIVILEGES on
// the domain and queue roles and re-GRANT a hand-maintained subset, a second
// copy of the grant manifest that drifted behind domainPosture()/
// coordinatorPosture() as tables were added over time. Any compose service
// that reached go-river-provision without then running go-river-migrate
// (pgbouncer startup, `docker compose run go-workerctl`, a deploy that
// stopped after pass 1) silently wiped whatever grants a prior
// go-river-migrate run had established down to that stale subset.
//
// It runs the REAL scripts/worker/provision_river_roles.sql through psql --
// not a Go re-implementation of it -- exactly as
// deploy/docker-compose/compose.go-workers.yml's go-river-provision service
// does, so a future edit that reintroduces a per-table GRANT there is
// caught by executing the actual file, not by re-reading it.

// provisionScriptPath resolves scripts/worker/provision_river_roles.sql
// relative to this test file's own location, not the working directory
// `go test` happens to run from.
func provisionScriptPath(t *testing.T) string {
	t.Helper()
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("could not resolve this test file's path via runtime.Caller")
	}
	// internal/storage/postgres -> repo root is three directories up.
	root := filepath.Join(filepath.Dir(file), "..", "..", "..")
	path := filepath.Join(root, "scripts", "worker", "provision_river_roles.sql")
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("scripts/worker/provision_river_roles.sql not found at %s: %v", path, err)
	}
	return path
}

// runProvisionScript runs the checked-in provisioning script through psql,
// passing the same variables deploy/docker-compose/compose.go-workers.yml's
// go-river-provision service does. It is idempotent by construction (role
// creation is guarded by WHERE NOT EXISTS), so calling it more than once
// must never error -- and, the property this suite exists to prove, must
// never remove a grant a prior go-river-migrate run established.
func runProvisionScript(t *testing.T, ctx context.Context, uri string) {
	t.Helper()
	cmd := exec.CommandContext(ctx, "psql",
		uri,
		"--set=ON_ERROR_STOP=1",
		"--set=domain_role="+grantDomainRole,
		"--set=queue_role="+grantQueueRole,
		"--set=coordinator_role="+grantCoordinatorRole,
		"--set=domain_password="+grantDomainPass,
		"--set=queue_password="+grantQueuePass,
		"--set=coordinator_password="+grantCoordinatorPass,
		"--file="+provisionScriptPath(t),
	)
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("psql --file=provision_river_roles.sql failed: %v\n%s", err, output)
	}
}

// tableGrantCount reports how many information_schema.role_table_grants
// rows PostgreSQL has recorded for the given role. Zero means the role
// holds no direct table-level privilege at all -- exactly the property
// provision_river_roles.sql must uphold now that it is bootstrap-only.
func tableGrantCount(t *testing.T, ctx context.Context, pool *pgxpool.Pool, role string) int {
	t.Helper()
	var count int
	if err := pool.QueryRow(
		ctx, "SELECT count(*) FROM information_schema.role_table_grants WHERE grantee = $1", role,
	).Scan(&count); err != nil {
		t.Fatalf("count table grants for %s: %v", role, err)
	}
	return count
}

// TestProvisionScriptGrantsNoTablePrivileges proves provision is bootstrap
// only: it creates the three runtime roles and grants them nothing beyond
// database CONNECT and schema-level USAGE. No semantic table needs to exist
// for this -- the whole point is that the script no longer names any.
func TestProvisionScriptGrantsNoTablePrivileges(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	instance, err := containers.StartPostgres(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		closeCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if err := instance.Close(closeCtx); err != nil {
			t.Errorf("terminate PostgreSQL: %v", err)
		}
	})
	admin, err := pgxpool.New(ctx, instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(admin.Close)

	runProvisionScript(t, ctx, instance.URI)

	for _, role := range []string{grantDomainRole, grantQueueRole, grantCoordinatorRole} {
		if count := tableGrantCount(t, ctx, admin, role); count != 0 {
			t.Errorf(
				"provision_river_roles.sql left %s holding %d table-level grant(s); "+
					"it must be role-creation-and-connectivity bootstrap only -- every "+
					"per-table/sequence grant belongs solely in "+
					"internal/storage/river/migrate.go, applied by go-river-migrate",
				role, count,
			)
		}
	}
}

// TestProvisionScriptNeverWipesMigrateGrants is the direct CHAOS-4261
// regression test: once go-river-migrate has established the full runtime
// posture, re-running go-river-provision -- simulating any compose service
// that reaches it without also running go-river-migrate, exactly the prod
// incident's pgbouncer/go-workerctl chain -- must leave that posture
// completely intact. Against the pre-fix script this fails immediately: its
// REVOKE ALL PRIVILEGES ON ALL TABLES/SEQUENCES IN SCHEMA public strips the
// domain and coordinator roles back down to a stale hand-maintained subset.
func TestProvisionScriptNeverWipesMigrateGrants(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	// startGrantHarness (domain_grant_reconciliation_integration_test.go)
	// builds the full domainPosture()/coordinatorPosture() schema, creates
	// the three roles, and applies the REAL go-river-migrate grants via
	// ApplyPinnedMigrations -- the same setup every other test in this
	// package's grant suite trusts, so reusing it here means this test
	// starts from a state already independently proven correct.
	admin, uri := startGrantHarness(t, ctx)

	assertFullRuntimePostureHolds(t, ctx, admin, uri)

	// The regression: run the checked-in provisioning script again, as if a
	// pgbouncer or go-workerctl invocation had reached go-river-provision
	// without go-river-migrate.
	runProvisionScript(t, ctx, uri)

	assertFullRuntimePostureHolds(t, ctx, admin, uri)
}

// assertFullRuntimePostureHolds runs the same three readiness checks the
// runtime binaries themselves gate on at startup
// (CheckDomainAuthorization/CheckQueueAuthorization/CheckCoordinatorAuthorization,
// internal/storage/postgres/domain_authorization.go). Each one asserts
// `current_user = expectedRole` (rolePostureQuery), so -- unlike
// DiagnoseRolePosture, which takes a role NAME and works from any
// connection -- these must be run over a connection actually authenticated
// as that role, never the admin/owner connection every other helper in this
// file uses.
func assertFullRuntimePostureHolds(t *testing.T, ctx context.Context, admin *pgxpool.Pool, uri string) {
	t.Helper()
	domain := connectAs(t, ctx, uri, grantDomainRole, grantDomainPass)
	if err := CheckDomainAuthorization(ctx, domain, grantDomainRole, grantSchema); err != nil {
		gaps, _ := DiagnoseRolePosture(ctx, admin, grantDomainRole, domainPosture())
		t.Fatalf("domain role posture check failed: %v (gaps: %v)", err, gaps)
	}
	queue := connectAs(t, ctx, uri, grantQueueRole, grantQueuePass)
	if err := CheckQueueAuthorization(ctx, queue, grantQueueRole, grantSchema); err != nil {
		t.Fatalf("queue role posture check failed: %v", err)
	}
	coordinator := connectAs(t, ctx, uri, grantCoordinatorRole, grantCoordinatorPass)
	if err := CheckCoordinatorAuthorization(ctx, coordinator, grantCoordinatorRole, grantSchema); err != nil {
		gaps, _ := DiagnoseRolePosture(ctx, admin, grantCoordinatorRole, coordinatorPosture())
		t.Fatalf("coordinator role posture check failed: %v (gaps: %v)", err, gaps)
	}
}

// TestQueuePostureMatchesTheGrantsItIsPairedWith is queuePosture's
// counterpart to TestDomainAuthorizationAcceptsTheGrantsItIsPairedWith /
// TestCoordinatorReadinessAcceptsTheGrantsThePostureDescribes: it proves
// queuePosture() (added for CHAOS-4261 so the admin-only executed-proof
// gate in cmd/dev-health-worker-migrate can name a queue gap via
// DiagnoseRolePosture) agrees with what go-river-migrate's real
// runtimeGrantStatements actually grants the queue role, through the real
// production grant path (ApplyPinnedMigrations via startGrantHarness), not
// a hand-rederivation of the same list. Zero gaps here is the whole claim:
// a table added to queuePosture without a matching GRANT in migrate.go, or
// granted there without a matching entry here, fails this test.
func TestQueuePostureMatchesTheGrantsItIsPairedWith(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	admin, _ := startGrantHarness(t, ctx)

	gaps, err := DiagnoseRolePosture(ctx, admin, grantQueueRole, QueuePosture())
	if err != nil {
		t.Fatalf("DiagnoseRolePosture: %v", err)
	}
	if len(gaps) != 0 {
		t.Fatalf("queuePosture() disagrees with the real queue grants: %v", gaps)
	}
}

// TestExecutedProofCatchesAMissingCoordinatorSequenceGrant is the direct
// regression test for the codex round-1 finding on CHAOS-4261's executed-
// proof gate: DiagnoseRolePosture used to check only RequiredTables and
// ColumnScoped, never RolePosture.RequiredSequences, so a coordinator role
// missing USAGE on worker_operator_audits_id_seq -- with every table and
// column requirement otherwise satisfied -- reported zero gaps here while
// CheckCoordinatorAuthorization (the real, current_user-bound readiness
// check every coordinator-role process gates on at startup) still failed.
// posture_diagnostics.go's diagnoseSequencePosture closes that gap; this
// proves it actually fires.
func TestExecutedProofCatchesAMissingCoordinatorSequenceGrant(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	admin, uri := startGrantHarness(t, ctx)
	assertFullRuntimePostureHolds(t, ctx, admin, uri)

	if _, err := admin.Exec(
		ctx, "REVOKE USAGE ON SEQUENCE public.worker_operator_audits_id_seq FROM "+grantCoordinatorRole,
	); err != nil {
		t.Fatalf("revoke sequence usage: %v", err)
	}

	gaps, err := DiagnoseRolePosture(ctx, admin, grantCoordinatorRole, coordinatorPosture())
	if err != nil {
		t.Fatalf("DiagnoseRolePosture: %v", err)
	}
	found := false
	for _, gap := range gaps {
		if gap.TableName == "worker_operator_audits_id_seq" && len(gap.Missing) == 1 && gap.Missing[0] == "USAGE" {
			found = true
		}
	}
	if !found {
		t.Fatalf("DiagnoseRolePosture did not report the revoked sequence grant as a gap: %v", gaps)
	}

	coordinator := connectAs(t, ctx, uri, grantCoordinatorRole, grantCoordinatorPass)
	if err := CheckCoordinatorAuthorization(ctx, coordinator, grantCoordinatorRole, grantSchema); err == nil {
		t.Fatal("CheckCoordinatorAuthorization must fail once the sequence grant is revoked")
	}
}
