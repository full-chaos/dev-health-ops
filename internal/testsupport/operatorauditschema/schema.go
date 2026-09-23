// Package operatorauditschema builds the worker_operator_audits table for
// integration tests by running the REAL alembic migration code that shapes
// it -- 0047 (create), 0136 (principal check), 0137 and 0138 (action check;
// 0138 also widens the action column) -- through alembic's own Operations
// on the project's Python interpreter. No DDL for this table is authored in
// Go, so a test cannot pass against a schema the migrations do not build.
//
// internal/joboperator and internal/workersctl share this one copy.
package operatorauditschema

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/pyoracle"
)

// Migrations are the alembic revisions Apply runs, in order.
var Migrations = []string{
	"0047_add_worker_operator_audits",
	"0136_worker_operator_audits_operator_principal",
	"0137_worker_operator_audits_action_check",
	"0138_worker_operator_audits_direct_write_actions",
}

const applyScript = `
import importlib, sys
import sqlalchemy as sa
from alembic.migration import MigrationContext
from alembic.operations import Operations

engine = sa.create_engine(sys.argv[1])
with engine.begin() as connection:
    connection.execute(sa.text("CREATE TABLE IF NOT EXISTS internal_service_credentials (id uuid PRIMARY KEY)"))
    with Operations.context(MigrationContext.configure(connection)):
        for name in sys.argv[2:]:
            importlib.import_module("dev_health_ops.alembic.versions." + name).upgrade()
print("AUDIT_MIGRATIONS_APPLIED")
`

// Apply runs Migrations against the Postgres database at uri (a
// postgres:// or postgresql:// URI). internal_service_credentials, which
// 0047's foreign key names, is created empty first.
func Apply(t *testing.T, ctx context.Context, uri string) {
	t.Helper()
	_, currentFile, _, _ := runtime.Caller(0)
	root := filepath.Dir(filepath.Dir(filepath.Dir(filepath.Dir(currentFile))))
	python := pyoracle.Resolve(t, root)
	dsn := strings.Replace(uri, "postgres://", "postgresql+psycopg2://", 1)
	dsn = strings.Replace(dsn, "postgresql://", "postgresql+psycopg2://", 1)
	command := exec.CommandContext(ctx, python, append([]string{"-c", applyScript, dsn}, Migrations...)...)
	command.Env = append(os.Environ(), "PYTHONPATH="+filepath.Join(root, "src"))
	output, err := command.CombinedOutput()
	if err != nil || !strings.Contains(string(output), "AUDIT_MIGRATIONS_APPLIED") {
		t.Fatal(pyoracle.RunError(python, err, output))
	}
}
