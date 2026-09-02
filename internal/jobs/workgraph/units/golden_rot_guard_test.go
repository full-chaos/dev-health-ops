package units

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// TestWorkgraphComponentsGoldenMatchesLivePython is the rot guard for
// tests/fixtures/workgraph_components_split_python_golden.json (CHAOS-4441),
// following the pattern the repo_user_commit and numerical goldens established.
//
// TestBuildComponentsMatchesFrozenPythonGoldenExhaustively asserts that GO
// matches the frozen file. Nothing there asserts that PYTHON still does. That
// gap matters more here than for an ordinary metric: while the Go materializer
// is live and the Python membership projection is not yet ported, the two planes
// address rows in different tables by the same work_unit_id, and a change to
// components.py that Go does not follow re-addresses one plane and not the
// other -- silent divergence, no crash (backfill.py:113-127). Until CHAOS-4282
// lands and both jobs call one Go implementation, THIS TEST is the continuous
// guard on that window.
//
// A plain byte diff is sufficient: both sides are the same Python-rendered JSON
// text, and the generator is hermetic (it reads the frozen edge input rather
// than querying ClickHouse), so this runs anywhere the interpreter does.
func TestWorkgraphComponentsGoldenMatchesLivePython(t *testing.T) {
	if os.Getenv("DEV_HEALTH_LIVE_PYTHON_ORACLES") != "1" {
		t.Skip("live Python oracles run only through ci/check_go.sh live-python-oracles")
	}
	proofDirectory := os.Getenv("DEV_HEALTH_LIVE_PYTHON_ORACLE_PROOF_DIR")
	if proofDirectory == "" {
		t.Fatal("DEV_HEALTH_LIVE_PYTHON_ORACLE_PROOF_DIR is required")
	}

	repoRoot := repositoryRootPath(t)
	python := workgraphComponentsLivePython(t, repoRoot)
	generator := filepath.Join(
		repoRoot, "tests", "fixtures", "generate_workgraph_components_python_golden.py",
	)
	if info, err := os.Stat(generator); err != nil || !info.Mode().IsRegular() {
		t.Fatalf("golden generator is missing at %s: %v", generator, err)
	}

	command := exec.Command(python, generator, "--stdout")
	command.Env = append(os.Environ(), "PYTHONPATH="+filepath.Join(repoRoot, "src"))
	rendered, err := command.Output()
	if err != nil {
		var stderr []byte
		if exitErr, ok := err.(*exec.ExitError); ok {
			stderr = exitErr.Stderr
		}
		t.Fatalf("run the golden generator against live Python: %v: %s", err, stderr)
	}

	frozen, err := os.ReadFile(filepath.Join(repoRoot, "tests", "fixtures", goldenFixture))
	if err != nil {
		t.Fatal(err)
	}

	if string(frozen) == string(rendered) {
		if writeErr := os.WriteFile(
			filepath.Join(proofDirectory, "workgraph-components-golden"), []byte("executed"), 0o644,
		); writeErr != nil {
			t.Fatalf("write live-python-oracle proof: %v", writeErr)
		}
		return
	}
	t.Error(
		"live Python no longer reproduces the frozen golden.\n" +
			"This is Python drift, not a Go bug: tests/fixtures/" +
			"workgraph_components_split_python_golden.json was generated from the deployed\n" +
			"work_graph/investment/components.py and frozen. Regenerate with\n" +
			"    PYTHONPATH=src python tests/fixtures/generate_workgraph_components_python_golden.py\n" +
			"and treat the diff as a REAL behaviour change with cross-plane consequences: a change\n" +
			"to component grouping or to work_unit_id re-addresses work_unit_investments and\n" +
			"work_unit_membership, which are written by two different jobs. Port the change into\n" +
			"internal/jobs/workgraph/units in the SAME change set, or the planes diverge silently.",
	)
}

// workgraphComponentsLivePython mirrors the repo_user_commit guard's helper:
// PYTHON wins, else python3 on PATH, and either way the resolved interpreter
// must resolve dev_health_ops to a module INSIDE this checkout -- otherwise the
// guard would silently compare another worktree's producer against this
// worktree's frozen golden.
func workgraphComponentsLivePython(t *testing.T, repoRoot string) string {
	t.Helper()
	resolved := os.Getenv("PYTHON")
	if resolved == "" {
		path, err := exec.LookPath("python3")
		if err != nil {
			t.Fatalf("python3 is required for the golden rot guard: %v", err)
		}
		resolved = path
	}
	command := exec.Command(
		resolved, "-c", "import dev_health_ops, sys; sys.stdout.write(dev_health_ops.__file__)",
	)
	command.Env = append(os.Environ(), "PYTHONPATH="+filepath.Join(repoRoot, "src"))
	located, err := command.Output()
	if err != nil {
		t.Fatalf("resolve dev_health_ops with %s: %v", resolved, err)
	}
	module := string(located)
	if !strings.HasPrefix(module, repoRoot+string(os.PathSeparator)) {
		t.Fatalf(
			"%s resolves dev_health_ops to %s, which is OUTSIDE this checkout (%s) -- "+
				"the guard would be comparing another worktree's producer against this "+
				"worktree's frozen golden; set PYTHONPATH to this checkout's src",
			resolved, module, repoRoot,
		)
	}
	return resolved
}

// TestConfidenceCoercionGoldenMatchesLivePython is the rot guard for
// tests/fixtures/confidence_coercion_python_golden.json.
//
// Its own guard, separate from the component golden above, because it has a
// different producer: Python's float() as invoked by _edge_confidence's string
// branch. That branch is where three separate parity divergences were found
// (whitespace stripping, C99 hex literals plus ErrRange saturation, and signed
// NaN words), so it is the last place to rely on a shared marker.
func TestConfidenceCoercionGoldenMatchesLivePython(t *testing.T) {
	if os.Getenv("DEV_HEALTH_LIVE_PYTHON_ORACLES") != "1" {
		t.Skip("live Python oracles run only through ci/check_go.sh live-python-oracles")
	}
	proofDirectory := os.Getenv("DEV_HEALTH_LIVE_PYTHON_ORACLE_PROOF_DIR")
	if proofDirectory == "" {
		t.Fatal("DEV_HEALTH_LIVE_PYTHON_ORACLE_PROOF_DIR is required")
	}

	repoRoot := repositoryRootPath(t)
	python := workgraphComponentsLivePython(t, repoRoot)
	generator := filepath.Join(
		repoRoot, "tests", "fixtures", "generate_confidence_coercion_python_golden.py",
	)
	if info, err := os.Stat(generator); err != nil || !info.Mode().IsRegular() {
		t.Fatalf("coercion golden generator is missing at %s: %v", generator, err)
	}

	command := exec.Command(python, generator, "--stdout")
	command.Env = append(os.Environ(), "PYTHONPATH="+filepath.Join(repoRoot, "src"))
	rendered, err := command.Output()
	if err != nil {
		var stderr []byte
		if exitErr, ok := err.(*exec.ExitError); ok {
			stderr = exitErr.Stderr
		}
		t.Fatalf("run the coercion golden generator against live Python: %v: %s", err, stderr)
	}

	frozen, err := os.ReadFile(filepath.Join(repoRoot, "tests", "fixtures", coercionGoldenFixture))
	if err != nil {
		t.Fatal(err)
	}

	if string(frozen) == string(rendered) {
		if writeErr := os.WriteFile(
			filepath.Join(proofDirectory, "confidence-coercion-golden"), []byte("executed"), 0o644,
		); writeErr != nil {
			t.Fatalf("write live-python-oracle proof: %v", writeErr)
		}
		return
	}
	t.Error(
		"live Python no longer coerces the corpus the way the frozen golden records.\n" +
			"Regenerate with\n" +
			"    python tests/fixtures/generate_confidence_coercion_python_golden.py\n" +
			"and treat the diff as a real behaviour change: the coerced confidence decides which\n" +
			"edges the oversized-component split protects, which decides work_unit_id.",
	)
}

// TestInvestmentQualityGoldenMatchesLivePython is the rot guard for
// tests/fixtures/investment_quality_python_golden.json.
//
// Its own marker again, and for a reason the other two do not share: this
// fixture spans FOUR producers in TWO modules -- utils/normalization.clamp and
// evidence_quality_band, plus evidence._graph_density, _float_value and
// compute_evidence_quality -- and it additionally records whether
// evidence._float_value still agrees with components._edge_confidence.
//
// That last one is the load-bearing part. The Go port reuses a SINGLE function
// (ConfidenceFromValue) for what Python keeps as two separate copies of the
// same coercion in two files. Nothing in Python enforces that they stay
// identical. If one is edited without the other, the recorded agreement breaks,
// this guard fails, and the Go side must be split into two functions rather
// than silently absorbing the divergence into whichever call site happens to be
// tested.
//
// A failure here is also the only signal that would catch a change to clamp(),
// which is shared with the rest of the codebase and lives outside
// work_graph/investment entirely -- so a reviewer editing normalization.py has
// no local cue that the investment port depends on its NaN behaviour.
func TestInvestmentQualityGoldenMatchesLivePython(t *testing.T) {
	if os.Getenv("DEV_HEALTH_LIVE_PYTHON_ORACLES") != "1" {
		t.Skip("live Python oracles run only through ci/check_go.sh live-python-oracles")
	}
	proofDirectory := os.Getenv("DEV_HEALTH_LIVE_PYTHON_ORACLE_PROOF_DIR")
	if proofDirectory == "" {
		t.Fatal("DEV_HEALTH_LIVE_PYTHON_ORACLE_PROOF_DIR is required")
	}

	repoRoot := repositoryRootPath(t)
	python := workgraphComponentsLivePython(t, repoRoot)
	generator := filepath.Join(
		repoRoot, "tests", "fixtures", "generate_investment_quality_golden.py",
	)
	if info, err := os.Stat(generator); err != nil || !info.Mode().IsRegular() {
		t.Fatalf("quality generator is missing at %s: %v", generator, err)
	}

	command := exec.Command(python, generator, "--stdout")
	command.Env = append(os.Environ(), "PYTHONPATH="+filepath.Join(repoRoot, "src"))
	rendered, err := command.Output()
	if err != nil {
		var stderr []byte
		if exitErr, ok := err.(*exec.ExitError); ok {
			stderr = exitErr.Stderr
		}
		t.Fatalf("run the quality generator against live Python: %v: %s", err, stderr)
	}

	frozen, err := os.ReadFile(filepath.Join(
		repoRoot, "tests", "fixtures", "investment_quality_python_golden.json",
	))
	if err != nil {
		t.Fatal(err)
	}

	if string(frozen) == string(rendered) {
		if writeErr := os.WriteFile(
			filepath.Join(proofDirectory, "investment-quality-golden"), []byte("executed"), 0o644,
		); writeErr != nil {
			t.Fatalf("write live-python-oracle proof: %v", writeErr)
		}
		return
	}
	t.Error(
		"live Python no longer reproduces the frozen evidence-quality golden.\n" +
			"Regenerate with\n" +
			"    PYTHONPATH=src python tests/fixtures/generate_investment_quality_golden.py\n" +
			"and read the diff before accepting it. Three things it could mean, with\n" +
			"different responses:\n" +
			"  1. clamp() changed in utils/normalization.py -- that function is shared\n" +
			"     far beyond this port, and units.Clamp reproduces its NaN behaviour\n" +
			"     (NaN lands on the HIGH bound because of the max(low, min(high, v))\n" +
			"     nesting). Port the change; do not just re-freeze.\n" +
			"  2. a weight or threshold moved in compute_evidence_quality or\n" +
			"     evidence_quality_band -- evidence_quality is a stored column and the\n" +
			"     bands are stored categories, so this is a data change.\n" +
			"  3. evidence._float_value and components._edge_confidence have DIVERGED.\n" +
			"     The Go port shares one function for both; it must be split.",
	)
}

// TestMaxComponentNodesGoldenMatchesLivePython is the rot guard for
// tests/fixtures/max_component_nodes_python_golden.json.
//
// Its own marker, and this one guards a value that is not in any source file
// on either side: sys.get_int_max_str_digits(). It is an INTERPRETER RUNTIME
// SETTING, changeable with sys.set_int_max_str_digits() and adjustable by
// command-line flag or environment variable, so it can move without a diff
// anywhere in this repository OR in CPython's version.
//
// units.DefaultIntMaxStrDigits is therefore a claim about the DEPLOYED
// interpreter, not a fact about Python. If the two part company, every value
// between the old and new limits is parsed by one plane and refused by the
// other -- and for INVESTMENT_MAX_COMPONENT_NODES that means one plane splits
// oversized components while the other does not, minting different
// work_unit_ids for the same graph.
//
// The corpus straddles the limit in both directions and pins the counting rule
// (digits only; sign, whitespace and underscores excluded; leading zeros
// included), so a regenerated fixture that disagrees is naming a real change
// rather than a formatting one.
func TestMaxComponentNodesGoldenMatchesLivePython(t *testing.T) {
	if os.Getenv("DEV_HEALTH_LIVE_PYTHON_ORACLES") != "1" {
		t.Skip("live Python oracles run only through ci/check_go.sh live-python-oracles")
	}
	proofDirectory := os.Getenv("DEV_HEALTH_LIVE_PYTHON_ORACLE_PROOF_DIR")
	if proofDirectory == "" {
		t.Fatal("DEV_HEALTH_LIVE_PYTHON_ORACLE_PROOF_DIR is required")
	}

	repoRoot := repositoryRootPath(t)
	python := workgraphComponentsLivePython(t, repoRoot)
	generator := filepath.Join(
		repoRoot, "tests", "fixtures", "generate_max_component_nodes_golden.py",
	)
	if info, err := os.Stat(generator); err != nil || !info.Mode().IsRegular() {
		t.Fatalf("magnitude generator is missing at %s: %v", generator, err)
	}

	command := exec.Command(python, generator, "--stdout")
	command.Env = append(os.Environ(), "PYTHONPATH="+filepath.Join(repoRoot, "src"))
	rendered, err := command.Output()
	if err != nil {
		var stderr []byte
		if exitErr, ok := err.(*exec.ExitError); ok {
			stderr = exitErr.Stderr
		}
		t.Fatalf("run the magnitude generator against live Python: %v: %s", err, stderr)
	}

	frozen, err := os.ReadFile(filepath.Join(
		repoRoot, "tests", "fixtures", "max_component_nodes_python_golden.json",
	))
	if err != nil {
		t.Fatal(err)
	}

	if string(frozen) == string(rendered) {
		if writeErr := os.WriteFile(
			filepath.Join(proofDirectory, "max-component-nodes-magnitude"),
			[]byte("executed"), 0o644,
		); writeErr != nil {
			t.Fatalf("write live-python-oracle proof: %v", writeErr)
		}
		return
	}
	t.Error(
		"live Python no longer reproduces the frozen magnitude golden.\n" +
			"Regenerate with\n" +
			"    PYTHONPATH=src python tests/fixtures/generate_max_component_nodes_golden.py\n" +
			"then read the diff rather than accepting it. The likeliest cause is that\n" +
			"sys.get_int_max_str_digits() has moved -- an interpreter RUNTIME setting that\n" +
			"appears in no source file on either side. units.DefaultIntMaxStrDigits must be\n" +
			"updated with it, or every value between the two limits is parsed by one plane\n" +
			"and refused by the other. For INVESTMENT_MAX_COMPONENT_NODES that means one\n" +
			"plane splits oversized components and the other does not, which mints\n" +
			"different work_unit_ids for the same graph.",
	)
}
