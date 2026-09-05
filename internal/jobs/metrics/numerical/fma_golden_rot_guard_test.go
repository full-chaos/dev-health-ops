package numerical

import (
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"testing"
)

const (
	fmaLivePythonOraclesEnv     = "DEV_HEALTH_LIVE_PYTHON_ORACLES"
	fmaLivePythonOracleProofDir = "DEV_HEALTH_LIVE_PYTHON_ORACLE_PROOF_DIR"
	fmaGoldenProofFile          = "fma-golden"
)

// fmaLiveComparedKeys are the fma_golden.json top-level keys this rot guard
// can still regenerate from live Python. "hotspot_score" is deliberately
// EXCLUDED: CHAOS-5234/CHAOS-3092 deleted compute_file_hotspots (file_hotspots
// is fully native now, no straddle), so generate_fma_golden.py no longer
// produces that key at all. The frozen "hotspot_score" section in
// fma_golden.json stays as-is (untouched by this change) -- it is still the
// contract for filehotspots/fma_golden_test.go's pure Go-vs-frozen-bits test,
// which never touches Python. This rot guard now only re-verifies the THREE
// keys that still have a live Python source.
var fmaLiveComparedKeys = []string{"schema_version", "release_confidence", "percentile_float", "percentile_int"}

// TestFMAGoldenMatchesLivePython is the CHAOS-4818 rot guard, in the shape
// established by TestRemainingMetricsGoldenMatchesLivePython
// (golden_rot_guard_test.go): tests/fixtures/fma_golden.json was generated
// from REAL production Python (release_impact._compute_confidence,
// compute._percentile, compute_capacity._percentile) and then frozen. The
// bit-exact tests in fma_golden_test.go (and its per-package copies) only
// prove Go matches the FILE -- nothing proves Python still matches the file,
// so the moment any of those functions' numerics change, the frozen
// `expected_bits` values silently encode the OLD Python behaviour and every
// bit-exact test stays green while the two implementations have actually
// diverged.
func TestFMAGoldenMatchesLivePython(t *testing.T) {
	if os.Getenv(fmaLivePythonOraclesEnv) != "1" {
		t.Skip("live Python oracles run only through ci/check_go.sh live-python-oracles")
	}
	proofDirectory := os.Getenv(fmaLivePythonOracleProofDir)
	if proofDirectory == "" {
		t.Fatalf("%s is required", fmaLivePythonOracleProofDir)
	}

	repoRoot := repositoryRoot(t)
	python := livePython(t, repoRoot)
	generator := filepath.Join(repoRoot, "tests", "fixtures", "generate_fma_golden.py")
	if info, err := os.Stat(generator); err != nil || !info.Mode().IsRegular() {
		t.Fatalf("golden generator is missing at %s: %v", generator, err)
	}

	rendered, err := exec.Command(python, generator, "--stdout").Output()
	if err != nil {
		var stderr []byte
		if exitErr, ok := err.(*exec.ExitError); ok {
			stderr = exitErr.Stderr
		}
		t.Fatalf("run the FMA golden generator against live Python: %v: %s", err, stderr)
	}

	goldenPath := filepath.Join(repoRoot, "tests", "fixtures", "fma_golden.json")
	frozen, err := os.ReadFile(goldenPath)
	if err != nil {
		t.Fatal(err)
	}

	frozenSubset, err := fmaSubsetForLiveKeys(frozen)
	if err != nil {
		t.Fatalf("parse frozen fma_golden.json: %v", err)
	}
	renderedSubset, err := fmaSubsetForLiveKeys(rendered)
	if err != nil {
		t.Fatalf("parse live-rendered fma_golden.json: %v", err)
	}

	if reflect.DeepEqual(frozenSubset, renderedSubset) {
		if err := os.WriteFile(
			filepath.Join(proofDirectory, fmaGoldenProofFile), []byte("executed\n"), 0o600,
		); err != nil {
			t.Fatalf("write live Python oracle proof: %v", err)
		}
		return
	}

	t.Errorf(
		"live Python no longer reproduces the frozen FMA golden (release_confidence/" +
			"percentile_float/percentile_int keys only -- hotspot_score is excluded, see " +
			"fmaLiveComparedKeys).\n" +
			"This is Python drift, not a Go bug: tests/fixtures/fma_golden.json was " +
			"generated from production Python and frozen (release_impact._compute_confidence, " +
			"compute._percentile, compute_capacity._percentile). Regenerate with\n" +
			"    python tests/fixtures/generate_fma_golden.py\n" +
			"and review the diff as a real behaviour change -- if Go should follow, change Go " +
			"too and re-verify every fma_golden_test.go bit-exact test; if it should not, the " +
			"Python change is the bug.",
	)
}

// fmaSubsetForLiveKeys parses raw fma_golden.json bytes and returns only the
// keys this rot guard can still regenerate from live Python (see
// fmaLiveComparedKeys) -- comparing the full document would spuriously fail
// on "hotspot_score", which the generator deliberately no longer produces.
func fmaSubsetForLiveKeys(raw []byte) (map[string]any, error) {
	var full map[string]any
	if err := json.Unmarshal(raw, &full); err != nil {
		return nil, err
	}
	subset := make(map[string]any, len(fmaLiveComparedKeys))
	for _, key := range fmaLiveComparedKeys {
		value, ok := full[key]
		if !ok {
			return nil, errMissingFMAKey(key)
		}
		subset[key] = value
	}
	return subset, nil
}

type errMissingFMAKey string

func (e errMissingFMAKey) Error() string {
	return "missing expected key " + string(e)
}
