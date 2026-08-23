package remaining

import (
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

const (
	livePythonOraclesEnv     = "DEV_HEALTH_LIVE_PYTHON_ORACLES"
	livePythonOracleProofDir = "DEV_HEALTH_LIVE_PYTHON_ORACLE_PROOF_DIR"
	incidentSQLProofFile     = "remaining-dora-incident-sql"
)

// pythonIncidentQuery asks the REAL builder for the query, rather than a
// fixture recording what it once returned. A recorded string is a third copy
// that rots in step with neither side.
const pythonIncidentQuery = `
import os, sys
from dev_health_ops.metrics.active_incidents import IncidentWindow, active_incidents_query
sys.stdout.write(active_incidents_query(
    window=IncidentWindow.RESOLVED,
    org_id=sys.argv[1],
    repo_filter=sys.argv[2],
))
`

// TestGoIncidentProjectionMatchesLivePythonBuilder is the mandated live-oracle
// guard for CHAOS-3092 R1 (Option C).
//
// The native DORA executor reproduces active_incidents_query in Go because
// there is no way to call a Python SQL builder from a production Go worker.
// That makes it a COPY, and a copied query goes stale silently the moment the
// original changes -- exactly the divergence class internal/providersync's
// readback pairs exist to catch rather than commit
// (oracle_readback_integration_test.go). Nothing about a Go-side unit test
// would notice: it would keep asserting the Go string against itself.
//
// So the comparison is against the live builder, under BOTH ordering
// contracts, following internal/jobs/metrics/daily/clickhouse_test.go:54's
// precedent of executing the production Python selector rather than a copied
// fixture.
func TestGoIncidentProjectionMatchesLivePythonBuilder(t *testing.T) {
	if os.Getenv(livePythonOraclesEnv) != "1" {
		t.Skip("live Python oracles run only through ci/check_go.sh live-python-oracles")
	}
	proofDirectory := os.Getenv(livePythonOracleProofDir)
	if proofDirectory == "" {
		t.Fatalf("%s is required", livePythonOracleProofDir)
	}
	python := livePythonInterpreter(t)

	repoID := "00000000-0000-4000-8000-00000000000a"
	repoName := "acme/widgets"
	filters := []struct {
		name  string
		scope doraScope
	}{
		{name: "no repo scope", scope: doraScope{}},
		{name: "repo id", scope: doraScope{RepoID: &repoID}},
		{name: "repo name", scope: doraScope{RepoName: &repoName}},
	}
	contracts := []struct {
		name     string
		env      string
		contract OperationalOrderingContract
	}{
		{name: "legacy FINAL", env: "1", contract: OperationalOrderingLegacy},
		{name: "revision ordering", env: "2", contract: OperationalOrderingRevision},
	}

	for _, contract := range contracts {
		for _, filter := range filters {
			contract, filter := contract, filter
			t.Run(contract.name+"/"+filter.name, func(t *testing.T) {
				repoFilter := repoFilterClause(filter.scope, map[string]any{})
				want := runPythonIncidentQuery(t, python, contract.env, repoFilter)
				got := resolvedIncidentsQuery(repoFilter, contract.contract)
				if normalizeSQL(got) != normalizeSQL(want) {
					t.Errorf(
						"Go incident projection has drifted from the Python builder.\n"+
							"This is the copied-query-rots failure, not a formatting nit:\n"+
							"  go     = %s\n  python = %s",
						normalizeSQL(got), normalizeSQL(want),
					)
				}
			})
		}
	}

	// Only on a PASS. t.Errorf does not stop execution, so an unguarded write
	// here records a proof the run did not earn -- the lane would then hold a
	// marker saying this comparison was satisfied while it had in fact
	// diverged. The lane checks the exit code too, so nothing would have
	// shipped, but a marker that can mean either thing is not evidence.
	if t.Failed() {
		return
	}
	if err := os.WriteFile(
		filepath.Join(proofDirectory, incidentSQLProofFile), []byte("executed\n"), 0o600,
	); err != nil {
		t.Fatalf("write live Python oracle proof: %v", err)
	}
}

func runPythonIncidentQuery(t *testing.T, python, contract, repoFilter string) string {
	t.Helper()
	command := exec.Command(python, "-c", pythonIncidentQuery, "{org_id:String}", repoFilter)
	command.Env = append(os.Environ(), "OPERATIONAL_ORDERING_CONTRACT="+contract)
	output, err := command.Output()
	if err != nil {
		var stderr []byte
		if exitErr, ok := err.(*exec.ExitError); ok {
			stderr = exitErr.Stderr
		}
		t.Fatalf("run the live Python incident builder: %v: %s", err, stderr)
	}
	return string(output)
}

// normalizeSQL collapses whitespace so the comparison is about SQL STRUCTURE,
// not about how each language happens to indent a heredoc. It deliberately
// does NOT lowercase or reorder anything: a changed predicate, a changed join,
// a changed ORDER BY or a dropped LIMIT must all still fail.
var sqlWhitespace = regexp.MustCompile(`\s+`)

func normalizeSQL(query string) string {
	return strings.TrimSpace(sqlWhitespace.ReplaceAllString(query, " "))
}

// livePythonInterpreter resolves the interpreter AND proves it resolves
// dev_health_ops inside THIS checkout.
//
// On a machine with several worktrees an ambient interpreter silently supplies
// another checkout's dev_health_ops, and the comparison then runs half against
// one tree and half against another while reporting a clean pass -- the worst
// possible fault for a parity guard, because it is indistinguishable from
// success.
func livePythonInterpreter(t *testing.T) string {
	t.Helper()
	resolved := os.Getenv("PYTHON")
	if resolved == "" {
		path, err := exec.LookPath("python3")
		if err != nil {
			t.Fatalf("python3 is required for the incident-SQL oracle: %v", err)
		}
		resolved = path
	}
	root := repositoryRootForOracle(t)
	located, err := exec.Command(
		resolved, "-c",
		"import dev_health_ops, sys; sys.stdout.write(dev_health_ops.__file__)",
	).Output()
	if err != nil {
		t.Fatalf("resolve dev_health_ops with %s: %v", resolved, err)
	}
	if !strings.HasPrefix(string(located), root+string(os.PathSeparator)) {
		t.Fatalf(
			"%s resolves dev_health_ops to %s, OUTSIDE this checkout (%s) -- the "+
				"guard would be comparing another worktree's builder against this "+
				"worktree's port; set PYTHONPATH to this checkout's src",
			resolved, string(located), root,
		)
	}
	return resolved
}

func repositoryRootForOracle(t *testing.T) string {
	t.Helper()
	working, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	for directory := working; ; {
		if _, err := os.Stat(filepath.Join(directory, "go.mod")); err == nil {
			return directory
		}
		parent := filepath.Dir(directory)
		if parent == directory {
			t.Fatalf("no go.mod above %s", working)
		}
		directory = parent
	}
}
