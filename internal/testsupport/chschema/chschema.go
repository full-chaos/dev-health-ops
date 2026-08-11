// Package chschema applies the REAL ClickHouse migration chain to a throwaway
// test container.
//
// It exists because hand-typed DDL in a Go test is not a schema: it is a
// SECOND, unversioned copy of one, and the test that reads it back can only
// ever confirm what the test itself declared. The derived work-item tables
// caught this concretely -- the hand-typed work_item_team_attributions carried
// the PRE-053 enums, missing the `issue_project` and `manual_fallback` source
// values and the `manual` and `none` confidence values that the production
// resolver actually emits (derivation_context.go:462,510,561). Every test over
// that table was green while an insert of a genuinely reachable row would have
// been rejected by the real column, and one fixture had been quietly written to
// `low` to stay inside the stale enum.
//
// So no DDL is authored here. The chain under
// src/dev_health_ops/migrations/clickhouse is applied through the project's own
// canonical migration entrypoint -- the same call the `migrate clickhouse
// upgrade` CLI makes -- which covers BOTH the .sql migrations and the .py ones
// (027 and 055 rebuild tables through a shadow-table swap that no static SQL
// extractor could reproduce).
//
// SHARED, and table-agnostic by construction: Apply takes no table list and
// migrates the database to the chain's head, so any package needing real
// tables gets all of them at once. It was written for three derived work-item
// destinations, but nothing about it is specific to them -- adopters need only
// call Apply instead of executing their own CREATE TABLE text.
package chschema

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

// applyScript runs the canonical migration entrypoint. force=true bypasses the
// AUTO_RUN_MIGRATIONS opt-out exactly as the CLI does, so a developer's ambient
// environment cannot turn this into a silent no-op that leaves a test asserting
// against an empty database.
const applyScript = `
import sys
from dev_health_ops.metrics.sinks.clickhouse import ClickHouseMetricsSink

sink = ClickHouseMetricsSink(dsn=sys.argv[1])
try:
    sink.ensure_schema(force=True)
finally:
    sink.close()
print("CHSCHEMA_APPLIED")
`

// Apply migrates the container to the current head of the real chain.
//
// It FAILS the test rather than skipping when Python is unavailable. A skip
// here would silently drop every schema-dependent assertion in the calling
// package while the package still reported ok, which is precisely the
// unmeasured-but-green shape this helper was written to remove.
func Apply(ctx context.Context, t *testing.T, instance *containers.Instance) {
	t.Helper()
	dsn, err := containers.ClickHouseHTTPDSN(ctx, instance)
	if err != nil {
		t.Fatalf("chschema: %v", err)
	}
	root, err := repoRoot()
	if err != nil {
		t.Fatalf("chschema: %v", err)
	}
	python, err := pythonBinary(root)
	if err != nil {
		t.Fatalf("chschema: %v", err)
	}
	var command *exec.Cmd
	switch filepath.Base(python) {
	case "python":
		command = exec.CommandContext(ctx, "python", "-c", applyScript, dsn)
	case "python3":
		command = exec.CommandContext(ctx, "python3", "-c", applyScript, dsn)
	default:
		t.Fatalf("chschema: unsupported Python executable %q", python)
	}
	command.Dir = root
	command.Env = append(os.Environ(),
		"PATH="+filepath.Dir(python)+string(os.PathListSeparator)+os.Getenv("PATH"),
		"PYTHONPATH="+filepath.Join(root, "src")+string(os.PathListSeparator)+os.Getenv("PYTHONPATH"),
	)
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("chschema: applying the real migration chain failed: %v\n%s", err, output)
	}
	// The runner can exit 0 having done nothing if the entrypoint ever stops
	// raising on failure, so require the positive marker rather than trusting
	// the exit code alone.
	if !strings.Contains(string(output), "CHSCHEMA_APPLIED") {
		t.Fatalf("chschema: migration runner produced no completion marker:\n%s", output)
	}
}

// repoRoot walks up from THIS file, not from the test's working directory, so
// the answer does not depend on which package invoked the helper.
func repoRoot() (string, error) {
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		return "", fmt.Errorf("cannot locate the chschema source file")
	}
	directory := filepath.Dir(file)
	for {
		migrations := filepath.Join(directory, "src", "dev_health_ops", "migrations", "clickhouse")
		if info, err := os.Stat(migrations); err == nil && info.IsDir() {
			return directory, nil
		}
		parent := filepath.Dir(directory)
		if parent == directory {
			return "", fmt.Errorf("no src/dev_health_ops/migrations/clickhouse above %s", filepath.Dir(file))
		}
		directory = parent
	}
}

// pythonBinary prefers the checked-out virtualenv, which is what the live
// Python oracle gate already relies on, then an explicit override, then PATH.
func pythonBinary(root string) (string, error) {
	if override := os.Getenv("DEV_HEALTH_PYTHON"); override != "" {
		return override, nil
	}
	venv := filepath.Join(root, ".venv", "bin", "python")
	if _, err := os.Stat(venv); err == nil {
		return venv, nil
	}
	found, err := exec.LookPath("python3")
	if err != nil {
		return "", fmt.Errorf(
			"no Python to run the migration chain: %s does not exist, "+
				"DEV_HEALTH_PYTHON is unset, and python3 is not on PATH", venv,
		)
	}
	return found, nil
}
