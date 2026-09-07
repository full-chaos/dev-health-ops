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
	command := pythonCommand(ctx, python, dsn, root)
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

// pythonCommand builds the migration-runner command from an ALREADY-RESOLVED
// interpreter path.
//
// It passes `python` itself rather than a bare "python"/"python3" literal, and
// that distinction is the whole point. exec.CommandContext resolves a bare name
// with LookPath against the PARENT process's PATH, at construction time.
// Setting command.Env afterwards changes the CHILD's environment and cannot
// affect how the executable is found -- so the PATH entry below was never able
// to make a bare name resolvable, and the previous code worked only on hosts
// where `python` already happened to be on PATH.
//
// It is not on PATH on Ubuntu, which ships `python3` only. Every chschema-based
// integration test there died with
//
//	chschema: applying the real migration chain failed:
//	exec: "python": executable file not found in $PATH
//
// while pythonBinary had already resolved a perfectly good absolute path one
// line earlier and the switch discarded it.
//
// The basename switch is gone with it. It only ever validated the NAME, and it
// rejected every interpreter not called exactly `python` or `python3` -- so
// DEV_HEALTH_PYTHON, which pythonBinary honours, was accepted there and then
// refused here for anything like `python3.12`, a uv-managed interpreter, or a
// wrapper script. An override the code accepts and then rejects is worse than
// one it does not offer.
//
// The PATH entry is KEPT because it is still correct for the child process
// itself (anything the migration runner shells out to), and is now guarded so a
// bare-name override cannot prepend "." to the child's PATH.
func pythonCommand(ctx context.Context, python, dsn, root string) *exec.Cmd {
	// The interpreter path is non-static BY DESIGN, and making it static is
	// exactly the bug this function exists to fix: a hard-coded "python" is
	// unresolvable on any host that ships only python3.
	//
	// Why it is not a code-injection path here. `python` comes from
	// pythonBinary(): the DEV_HEALTH_PYTHON environment variable, else
	// <root>/.venv/bin/python, else exec.LookPath("python3"). The env var is a
	// deliberate developer-facing knob for choosing an interpreter, set by
	// whoever is already running the test binary -- it is not request data, not
	// file content, and not attacker-reachable. Anyone able to set it can
	// already run arbitrary code as that user by running `go test` at all.
	//
	// There is no shell: exec.CommandContext execs directly, so word splitting
	// and metacharacter interpretation do not apply. The remaining argv is
	// fully static apart from the DSN.
	//
	// Reachability: this package is test support. Its only non-test importer is
	// internal/testsupport/oraclecompare, which is also test support; no
	// production binary links it. (Note this was a WEAKER claim than
	// internal/testsupport/computeparity made for the same rule -- that one
	// (now retired, CHAOS-5336: its two callers, dora/capacity's Python-producer
	// parity tests, were deleted along with the Python they compared against)
	// was importable only from _test.go files, and its argv came from checked-in
	// test code rather than an environment variable. Stating the difference
	// rather than reusing its wording.)
	//
	// The suppression must sit on the line DIRECTLY above the finding --
	// Semgrep does not scan back through an intervening comment block, which is
	// why this rationale is above and the pragma is flush against the call.
	// nosemgrep: go.lang.security.audit.dangerous-exec-command.dangerous-exec-command
	command := exec.CommandContext(ctx, python, "-c", applyScript, dsn)
	command.Dir = root
	environment := os.Environ()
	if filepath.IsAbs(python) {
		environment = append(environment,
			"PATH="+filepath.Dir(python)+string(os.PathListSeparator)+os.Getenv("PATH"))
	}
	environment = append(environment,
		"PYTHONPATH="+filepath.Join(root, "src")+string(os.PathListSeparator)+os.Getenv("PYTHONPATH"))
	command.Env = environment
	return command
}

// pythonBinary prefers the checked-out virtualenv, which is what the live
// Python oracle gate already relies on, then an explicit override, then PATH.
// Interpreter resolves the Python this package would use, for callers that
// must run the SAME interpreter chschema does.
//
// It exists because a test in internal/jobs/metrics/remaining hard-coded
// <root>/.venv/bin/python while chschema, three lines earlier in the same test,
// resolved python3 from PATH -- so the schema setup succeeded in CI and the
// caller's own Python invocation died with "no such file or directory". Two
// lookups in one test disagreeing about where Python lives.
//
// Exported rather than duplicated on purpose: a copied resolver is the same
// defect again one refactor later. It shares this package's repoRoot() as well
// as its lookup order, so caller and schema setup cannot diverge on either.
func Interpreter() (string, error) {
	root, err := repoRoot()
	if err != nil {
		return "", err
	}
	return pythonBinary(root)
}

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
