package pythonparity

import (
	"os"
	"os/exec"
	"path/filepath"
	"testing"
)

// TestClickHouseStringDecodeGoldenMatchesLivePython is the rot guard for
// tests/fixtures/clickhouse_string_decode_python_golden.json.
//
// Its own marker, and this one guards the most fragile producer of the three
// in this package: a THIRD-PARTY DEPENDENCY. The policy being reproduced is
// two lines inside clickhouse-connect's read loop
// (driver/buffer.py:135-138) --
//
//	try:    app(x.decode(encoding))
//	except UnicodeDecodeError:  app(x.hex())
//
// -- which is not part of that library's documented API and can change in any
// release. A version bump in the lockfile would move it with no diff in src/
// and no diff in this repository's own code at all.
//
// What a failure means is therefore specific: the driver's decode policy has
// changed, and every String column read by the investment port now arrives
// differently in Python than the Go side assumes. Because those strings are
// hashed into input_hash AND into work_unit_id, the consequence is not a
// display bug -- it re-addresses rows across two tables written by two
// different jobs, and re-bills every LLM categorization.
//
// The measured policy was established against a real ClickHouse container with
// the stored bytes verified server-side, at clickhouse-connect 0.15.1. This
// guard re-derives the corpus from the same expression; the container
// measurement is what licenses using that expression as the oracle, and is
// recorded in the fixture's _policy field and in plan section 5d.
func TestClickHouseStringDecodeGoldenMatchesLivePython(t *testing.T) {
	if os.Getenv("DEV_HEALTH_LIVE_PYTHON_ORACLES") != "1" {
		t.Skip("live Python oracles run only through ci/check_go.sh live-python-oracles")
	}
	proofDirectory := os.Getenv("DEV_HEALTH_LIVE_PYTHON_ORACLE_PROOF_DIR")
	if proofDirectory == "" {
		t.Fatal("DEV_HEALTH_LIVE_PYTHON_ORACLE_PROOF_DIR is required")
	}

	repoRoot := parityRepositoryRoot(t)
	python := parityLivePython(t, repoRoot)
	generator := filepath.Join(
		repoRoot, "tests", "fixtures", "generate_clickhouse_string_decode_golden.py",
	)
	if info, err := os.Stat(generator); err != nil || !info.Mode().IsRegular() {
		t.Fatalf("decode generator is missing at %s: %v", generator, err)
	}

	command := exec.Command(python, generator, "--stdout")
	command.Env = append(os.Environ(), "PYTHONPATH="+filepath.Join(repoRoot, "src"))
	rendered, err := command.Output()
	if err != nil {
		var stderr []byte
		if exitErr, ok := err.(*exec.ExitError); ok {
			stderr = exitErr.Stderr
		}
		t.Fatalf("run the decode generator against live Python: %v: %s", err, stderr)
	}

	frozen, err := os.ReadFile(filepath.Join(
		repoRoot, "tests", "fixtures", "clickhouse_string_decode_python_golden.json",
	))
	if err != nil {
		t.Fatal(err)
	}

	if string(frozen) == string(rendered) {
		if writeErr := os.WriteFile(
			filepath.Join(proofDirectory, "clickhouse-string-decode-golden"),
			[]byte("executed"), 0o644,
		); writeErr != nil {
			t.Fatalf("write live-python-oracle proof: %v", writeErr)
		}
		return
	}
	t.Error(
		"live Python no longer reproduces the frozen ClickHouse String decode golden.\n" +
			"Regenerate with\n" +
			"    PYTHONPATH=src python tests/fixtures/generate_clickhouse_string_decode_golden.py\n" +
			"then RE-MEASURE against a real container before accepting: the fixture's\n" +
			"oracle is an expression copied from clickhouse-connect's read loop, and if the\n" +
			"CPython UTF-8 decoder or the driver's fallback has changed, the copy is stale\n" +
			"and regenerating it only re-freezes the wrong answer.\n" +
			"chquery applies this policy to EVERY String column, and those strings are\n" +
			"hashed into input_hash and into work_unit_id -- so a change here re-addresses\n" +
			"rows across work_unit_investments and work_unit_membership and re-bills every\n" +
			"LLM categorization.",
	)
}
