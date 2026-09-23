// Package operatorauditschema lets integration tests build the
// worker_operator_audits table by running the REAL alembic migration code that shapes
// it -- 0047 (create), 0136 (principal check), 0137 and 0138 (action check;
// 0138 also widens the action column) -- through alembic's own Operations
// on the project's Python interpreter. No DDL for this table is authored in
// Go, so a test cannot pass against a schema the migrations do not build.
//
// internal/joboperator and internal/workersctl share this one copy.
package operatorauditschema

import (
	"os"
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

// The interpreter is started by each caller's _test.go file, not here: this
// package builds only the static parts of the command (Argv, Env) and checks
// its result (CheckApplied), so it never execs a program itself.

// Root is the repository root: the directory pyoracle.Resolve searches for
// the project's interpreter.
func Root() string {
	_, currentFile, _, _ := runtime.Caller(0)
	return filepath.Dir(filepath.Dir(filepath.Dir(filepath.Dir(currentFile))))
}

// Argv is the interpreter argument list that applies Migrations to the
// Postgres database at uri (a postgres:// or postgresql:// URI).
// internal_service_credentials, which 0047's foreign key names, is created
// empty first.
func Argv(uri string) []string {
	dsn := strings.Replace(uri, "postgres://", "postgresql+psycopg2://", 1)
	dsn = strings.Replace(dsn, "postgresql://", "postgresql+psycopg2://", 1)
	return append([]string{"-c", applyScript, dsn}, Migrations...)
}

// Env is the interpreter's environment: this process's, plus the project's
// src directory on PYTHONPATH.
func Env() []string {
	return append(os.Environ(), "PYTHONPATH="+filepath.Join(Root(), "src"))
}

// CheckApplied fails t unless the command ran and reported that every
// migration applied.
func CheckApplied(t *testing.T, python string, output []byte, err error) {
	t.Helper()
	if err != nil || !strings.Contains(string(output), "AUDIT_MIGRATIONS_APPLIED") {
		t.Fatal(pyoracle.RunError(python, err, output))
	}
}
