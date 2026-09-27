//go:build integration

package pgmigrate_test

import (
	"bytes"
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/cli"
	"github.com/full-chaos/dev-health-ops/internal/pgmigrate"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/pyoracle"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/venueoracle"
)

// pythonDowngrade runs the real `dev-hops migrate postgres downgrade <target>` (Alembic's real
// downgrade, `command.downgrade`) against uri.
func pythonDowngrade(t *testing.T, uri, target string) (code int, output string) {
	t.Helper()
	root, err := filepath.Abs(filepath.Join("..", ".."))
	if err != nil {
		t.Fatal(err)
	}
	python := pyoracle.Resolve(t, root)
	program := "import sys\nfrom dev_health_ops import cli\nraise SystemExit(cli.main(sys.argv[1:]))\n"
	command := exec.Command(python, "-c", program, "migrate", "postgres", "downgrade", target)
	pyURI := strings.Replace(uri, "postgres://", "postgresql://", 1)
	command.Env = append(os.Environ(), "PYTHONPATH="+filepath.Join(root, "src"), "POSTGRES_URI="+pyURI, "DATABASE_URI="+pyURI, "OTEL_ENABLED=false")
	command.Env = removeEnv(command.Env, "MIGRATION_DATABASE_URI")
	var stdout, stderr bytes.Buffer
	command.Stdout, command.Stderr = &stdout, &stderr
	err = command.Run()
	exitCode := 0
	if exitErr, ok := err.(*exec.ExitError); ok {
		exitCode = exitErr.ExitCode()
	} else if err != nil {
		t.Fatalf("running the python producer: %v", pyoracle.RunError(python, err, []byte(stderr.String())))
	}
	return exitCode, stdout.String() + stderr.String()
}

// goDowngrade runs `dho migrate postgres downgrade <args...>` directly, with a caller-chosen
// environment (a garbage/absent DSN, to prove the verb never resolves one).
func goDowngrade(t *testing.T, args []string, lookup func(string) (string, bool)) (code int, output string) {
	t.Helper()
	var run func(context.Context, cli.Env) int
	for _, child := range pgmigrate.Command(nil).Children {
		if child.Name == "downgrade" {
			run = child.Run
		}
	}
	var stdout, stderr bytes.Buffer
	got := run(context.Background(), cli.Env{Args: args, Lookup: lookup, Stdout: &stdout, Stderr: &stderr})
	return got, stdout.String() + stderr.String()
}

// alembicVersions reads the recorded revision(s), so the test can prove Python's downgrade actually
// moved them and Go's refusal left them untouched.
func alembicVersions(t *testing.T, uri string) []string {
	t.Helper()
	conn := connect(t, uri)
	defer conn.Close(context.Background())
	rows, err := conn.Query(context.Background(), "SELECT version_num FROM alembic_version ORDER BY version_num")
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var version string
		if err := rows.Scan(&version); err != nil {
			t.Fatal(err)
		}
		out = append(out, version)
	}
	return out
}

// TestDowngradeVenueOracleIsRefusedWhilePythonReallyDowngrades is the executed differential oracle
// for `dho migrate postgres downgrade` / the flat `dho migrate downgrade` alias:
//
//  1. the real Python verb (`command.downgrade`, real Alembic) genuinely downgrades a database at the
//     head by one revision -- proving `downgrade` is a real, working verb in the release this is a port
//     of, not something already dead or stubbed there too.
//  2. dho's `downgrade` refuses (exit code, and the ForwardOnlyDetail message) on the identical
//     database, with the database left completely unchanged (the recorded revision is identical
//     before and after).
//  3. dho's `downgrade` refuses IDENTICALLY given a garbage/absent DSN and no database at all: it has
//     no ResolveDSN dependency (unlike every other verb in this package), so there is nothing for a bad
//     DSN to break -- proving the refusal happens before any connection is even attempted, not merely
//     before a successful one.
//  4. the flat `dho migrate downgrade` alias is the identical Command (Aliases reuses the same
//     cli.Command value), so it refuses the same way.
func TestDowngradeVenueOracleIsRefusedWhilePythonReallyDowngrades(t *testing.T) {
	if os.Getenv("DEV_HEALTH_LIVE_PYTHON_ORACLES") != "1" {
		t.Skip("the live Python producer runs only with DEV_HEALTH_LIVE_PYTHON_ORACLES=1 and the full project Python environment")
	}

	pythonURI, _ := revisionsDatabase(t)
	before := alembicVersions(t, pythonURI)
	if len(before) == 0 {
		t.Fatal("the fresh database records no revision at all")
	}
	pyCode, pyOutput := pythonDowngrade(t, pythonURI, "-1")
	if pyCode != 0 {
		t.Fatalf("the real Python downgrade -1 failed (exit %d): %s -- if `downgrade` is no longer a real, working Alembic verb in this release, dho's forward-only refusal needs a different justification, not this oracle", pyCode, pyOutput)
	}
	after := alembicVersions(t, pythonURI)
	if len(after) == 0 || equalSorted(before, after) {
		t.Fatalf("the real Python downgrade -1 reported success but the recorded revision did not move: before=%v after=%v", before, after)
	}

	goURI, _ := revisionsDatabase(t)
	goBefore := alembicVersions(t, goURI)
	garbageLookup := func(key string) (string, bool) {
		switch key {
		case "POSTGRES_URI", "DATABASE_URI", "MIGRATION_DATABASE_URI":
			return "postgres://nobody:nothing@169.254.0.1:1/nonexistent", true
		default:
			return "", false
		}
	}
	goCode, goOutput := goDowngrade(t, []string{"-1"}, garbageLookup)
	if goCode != cli.ExitRefused {
		t.Fatalf("dho migrate postgres downgrade -1: exit %d, want ExitRefused (%d): %s", goCode, cli.ExitRefused, goOutput)
	}
	if !strings.Contains(goOutput, "forward_only") || !strings.Contains(goOutput, "forward-only") {
		t.Fatalf("dho's refusal does not carry its forward-only reason: %s", goOutput)
	}
	if strings.Contains(goOutput, "169.254.0.1") || strings.Contains(goOutput, "nonexistent") {
		t.Fatalf("dho's refusal leaked the garbage DSN it never should have touched: %s", goOutput)
	}
	goAfter := alembicVersions(t, goURI)
	if !equalSorted(goBefore, goAfter) {
		t.Fatalf("dho's refusal changed the database: before=%v after=%v", goBefore, goAfter)
	}

	// The flat alias is the identical Command value (Aliases reuses group.Children), so this exercises
	// nothing new mechanically, but pins that fact so a future refactor that split them apart would be
	// caught here rather than silently drifting.
	aliasCode, aliasOutput := 0, ""
	for _, child := range pgmigrate.Aliases(nil) {
		if child.Name == "downgrade" {
			var stdout, stderr bytes.Buffer
			aliasCode = child.Run(context.Background(), cli.Env{Args: []string{"-1"}, Lookup: garbageLookup, Stdout: &stdout, Stderr: &stderr})
			aliasOutput = stdout.String() + stderr.String()
		}
	}
	if aliasCode != goCode || aliasOutput != goOutput {
		t.Fatalf("the flat `migrate downgrade` alias disagrees with `migrate postgres downgrade`: exit %d %q vs exit %d %q", aliasCode, aliasOutput, goCode, goOutput)
	}

	venueoracle.WriteProof(t)
}

func equalSorted(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
