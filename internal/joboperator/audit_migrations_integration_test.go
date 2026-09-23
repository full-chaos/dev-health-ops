//go:build integration

package joboperator

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/pyoracle"
)

// applyAuditMigrations runs the REAL alembic migration code that shapes
// worker_operator_audits -- 0047 (create), 0136 (principal check) and 0137
// (action check) -- against the container, through alembic's own Operations
// on the project's interpreter. No DDL for this table is authored here.
const applyAuditMigrations = `
import importlib, sys
import sqlalchemy as sa
from alembic.migration import MigrationContext
from alembic.operations import Operations

engine = sa.create_engine(sys.argv[1])
with engine.begin() as connection:
    connection.execute(sa.text("CREATE TABLE IF NOT EXISTS internal_service_credentials (id uuid PRIMARY KEY)"))
    with Operations.context(MigrationContext.configure(connection)):
        for name in (
            "0047_add_worker_operator_audits",
            "0136_worker_operator_audits_operator_principal",
            "0137_worker_operator_audits_action_check",
        ):
            importlib.import_module("dev_health_ops.alembic.versions." + name).upgrade()
print("AUDIT_MIGRATIONS_APPLIED")
`

// TestEveryAuditedActionPassesTheMigratedAuditConstraints is the proof of
// record for the audit action check: against the table the real migrations
// build, the production PostgresAuditor writes a row for every Action in
// AuditedActions as the operator principal, and still refuses an action
// the check does not allow.
func TestEveryAuditedActionPassesTheMigratedAuditConstraints(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	instance, err := containers.StartPostgres(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer instance.Close(context.Background())

	_, currentFile, _, _ := runtime.Caller(0)
	root := filepath.Dir(filepath.Dir(filepath.Dir(currentFile)))
	python := pyoracle.Resolve(t, root)
	dsn := strings.Replace(instance.URI, "postgres://", "postgresql+psycopg2://", 1)
	dsn = strings.Replace(dsn, "postgresql://", "postgresql+psycopg2://", 1)
	command := exec.CommandContext(ctx, python, "-c", applyAuditMigrations, dsn)
	command.Env = append(os.Environ(), "PYTHONPATH="+filepath.Join(root, "src"))
	output, err := command.CombinedOutput()
	if err != nil || !strings.Contains(string(output), "AUDIT_MIGRATIONS_APPLIED") {
		t.Fatal(pyoracle.RunError(python, err, output))
	}

	pool, err := pgxpool.New(ctx, instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	auditor, err := NewPostgresAuditor(pool)
	if err != nil {
		t.Fatal(err)
	}
	event := func(action Action) AuditEvent {
		return AuditEvent{
			Principal: OperatorPrincipal, Action: action, ResourceType: "sync_route",
			ResourceID: "dispatch_sync_run", ReasonCode: "operator_test",
			CorrelationID: "corr-" + strings.ReplaceAll(string(action), ".", "-"),
			CreatedAt:     time.Date(2026, 9, 23, 0, 0, 0, 0, time.UTC),
		}
	}
	for _, action := range AuditedActions {
		handle, err := auditor.Begin(ctx, event(action))
		if err != nil {
			t.Errorf("%s: audit Begin refused by the migrated table: %v", action, err)
			continue
		}
		if err := handle.Complete(ctx, AuditSucceeded); err != nil {
			t.Errorf("%s: audit Complete: %v", action, err)
		}
	}
	var rows int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM public.worker_operator_audits
		WHERE principal_type = 'operator' AND principal_id = 'dho-workers' AND credential_id IS NULL
		  AND status = 'succeeded'`).Scan(&rows); err != nil || rows != len(AuditedActions) {
		t.Fatalf("succeeded operator audit rows = %d (err %v), want %d", rows, err, len(AuditedActions))
	}
	// An action outside the check stays refused: the constraint still binds.
	if _, err := auditor.Begin(ctx, event(ActionWorkgraphTrigger)); err == nil {
		t.Fatal("the migrated check allowed workgraph.manual_trigger, which it must not")
	}
}
