package filehotspots

import (
	"encoding/json"
	"math"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

// TestSampleZScoresMatchesLivePythonBitExact is the regression test for the
// CHAOS-4818 AST-lint follow-up's first live finding: sampleZScores'
// `sumSquares += diff * diff` was an unguarded compound-assignment FMA site
// (the lint's original, expression-only version never inspected it -- see
// fma_lint_test.go's package doc comment).
//
// This calls sampleZScores DIRECTLY rather than going through
// ComputeFileRiskHotspots: that entry point aggregates per-file churn via a
// Go map (`allPaths`/`churnByPath`) before building the `churns` slice
// sampleZScores consumes, and Go's map iteration order is intentionally
// randomized per process -- so the SAME input can feed sampleZScores
// churn values in a DIFFERENT order on different runs, and floating-point
// summation is not associative, so the resulting bit pattern can differ
// run to run. That is a real, pre-existing, order-of-summation
// non-determinism bug in ComputeFileRiskHotspots, found while building
// this golden (a case that passed with -N -l failed without it, and vice
// versa -- the tell that something other than FMA was moving) -- but it is
// a SEPARATE defect from CHAOS-4818 (the class this lint/PR targets) and
// is reported, not fixed, here. Calling sampleZScores directly, with the
// churns in the same order generate_fma_followup_golden.py feeds Python's
// compute_file_risk_hotspots (which iterates a plain dict in insertion
// order, deterministically), isolates exactly the FMA-fusion site this
// test exists to prove, without either bug masking the other.
func TestSampleZScoresMatchesLivePythonBitExact(t *testing.T) {
	data, err := os.ReadFile(filepath.Join("..", "..", "..", "..", "..", "tests", "fixtures", "fma_followup_golden.json"))
	if err != nil {
		t.Fatal(err)
	}
	var fixture struct {
		HotspotRiskScore []struct {
			Case         string `json:"case"`
			Churns       []int  `json:"churns"`
			FileIndex    int    `json:"file_index"`
			ExpectedBits string `json:"expected_bits"`
		} `json:"hotspot_risk_score"`
	}
	if err := json.Unmarshal(data, &fixture); err != nil {
		t.Fatal(err)
	}
	if len(fixture.HotspotRiskScore) == 0 {
		t.Fatal("fixture has no hotspot_risk_score cases")
	}

	mismatches := 0
	for _, testCase := range fixture.HotspotRiskScore {
		churns := make([]float64, len(testCase.Churns))
		for i, c := range testCase.Churns {
			churns[i] = float64(c)
		}
		z := sampleZScores(churns)
		if testCase.FileIndex >= len(z) {
			t.Fatalf("case %s: file_index %d out of range (len %d)", testCase.Case, testCase.FileIndex, len(z))
		}
		wantBits, err := strconv.ParseUint(strings.TrimPrefix(testCase.ExpectedBits, "0x"), 16, 64)
		if err != nil {
			t.Fatalf("case %s: parse expected_bits %q: %v", testCase.Case, testCase.ExpectedBits, err)
		}
		gotBits := math.Float64bits(z[testCase.FileIndex])
		if gotBits != wantBits {
			mismatches++
			if mismatches <= 10 {
				t.Errorf("case %s: sampleZScores(churns=%v)[%d] = %v (bits %#x), want bits %#x (%v)",
					testCase.Case, testCase.Churns, testCase.FileIndex,
					z[testCase.FileIndex], gotBits, wantBits, math.Float64frombits(wantBits))
			}
		}
	}
	if mismatches > 10 {
		t.Errorf("... and %d more mismatches (total %d of %d cases)", mismatches-10, mismatches, len(fixture.HotspotRiskScore))
	}
}
