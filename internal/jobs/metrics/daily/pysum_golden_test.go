package daily

import (
	"encoding/json"
	"math"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

// TestWeightedSuccessRate7dMatchesLivePythonBitExact and
// TestSuccessRateTrendMatchesLivePythonBitExact are the CHAOS-4824
// regression tests for computePipelineStability's naive-loop-vs-
// compensated-sum() fix. Unlike CodeOwnershipGini's golden (repouser
// package), whose real-world integer inputs make the defect currently
// unreachable, these ARE the reachable site: `success_rate` is a fractional
// ratio in [0,1], and the corpus (tests/fixtures/generate_pysum_golden.py)
// deliberately includes >= 60 pseudo-random 3-8 element trials, 11 of which
// are independently verified (by this test file's own generator sanity
// check, run once at generation time) to diverge between naive and
// CPython-compensated summation -- so this corpus WILL go red against a
// naive-accumulator baseline, not just theoretically could.
//
// weightedSuccessRate7d/successRateTrendFromRates return the UNROUNDED
// value (computePipelineStability itself rounds to 4 decimals before
// storage) -- see those functions' doc comments for why: 4-decimal
// rounding is far coarser than the few-ULP difference this defect class
// produces, so a golden against the rounded, stored value would almost
// never go red.
func TestWeightedSuccessRate7dMatchesLivePythonBitExact(t *testing.T) {
	fixture := loadPysumFixture(t)
	if len(fixture.PipelineStability) == 0 {
		t.Fatal("fixture has no pipeline_stability cases")
	}
	mismatches := 0
	for _, testCase := range fixture.PipelineStability {
		got := weightedSuccessRate7d(testCase.SuccessRates)
		wantBits := parseBitsHex(t, testCase.WeightedSuccessRate7dBits)
		gotBits := math.Float64bits(got)
		if gotBits != wantBits {
			mismatches++
			if mismatches <= 10 {
				t.Errorf("weightedSuccessRate7d(%v) = %v (bits %#x), want bits %#x (%v)",
					testCase.SuccessRates, got, gotBits, wantBits, math.Float64frombits(wantBits))
			}
		}
	}
	if mismatches > 10 {
		t.Errorf("... and %d more mismatches (total %d of %d cases)", mismatches-10, mismatches, len(fixture.PipelineStability))
	}
}

func TestSuccessRateTrendMatchesLivePythonBitExact(t *testing.T) {
	fixture := loadPysumFixture(t)
	checked := 0
	mismatches := 0
	for _, testCase := range fixture.PipelineStability {
		if len(testCase.SuccessRates) < 2 || testCase.SuccessRateTrendBits == "" {
			continue
		}
		checked++
		got := successRateTrendFromRates(testCase.SuccessRates)
		wantBits := parseBitsHex(t, testCase.SuccessRateTrendBits)
		gotBits := math.Float64bits(got)
		if gotBits != wantBits {
			mismatches++
			if mismatches <= 10 {
				t.Errorf("successRateTrendFromRates(%v) = %v (bits %#x), want bits %#x (%v)",
					testCase.SuccessRates, got, gotBits, wantBits, math.Float64frombits(wantBits))
			}
		}
	}
	if checked == 0 {
		t.Fatal("fixture has no pipeline_stability cases with >= 2 success rates")
	}
	if mismatches > 10 {
		t.Errorf("... and %d more mismatches (total %d of %d cases)", mismatches-10, mismatches, checked)
	}
}

type pysumFixture struct {
	PipelineStability []struct {
		SuccessRates              []float64 `json:"success_rates"`
		WeightedSuccessRate7dBits string    `json:"weighted_success_rate_7d_bits"`
		SuccessRateTrendBits      string    `json:"success_rate_trend_bits"`
	} `json:"pipeline_stability"`
}

func loadPysumFixture(t *testing.T) pysumFixture {
	t.Helper()
	data, err := os.ReadFile(filepath.Join("..", "..", "..", "..", "tests", "fixtures", "pysum_golden.json"))
	if err != nil {
		t.Fatal(err)
	}
	var fixture pysumFixture
	if err := json.Unmarshal(data, &fixture); err != nil {
		t.Fatal(err)
	}
	return fixture
}

func parseBitsHex(t *testing.T, hex string) uint64 {
	t.Helper()
	value, err := strconv.ParseUint(strings.TrimPrefix(hex, "0x"), 16, 64)
	if err != nil {
		t.Fatalf("parse bits %q: %v", hex, err)
	}
	return value
}
