package testops

import (
	"encoding/json"
	"math"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

// See internal/jobs/metrics/numerical/fma_golden_test.go for the full
// explanation of this fixture and CHAOS-4818. This is the testops.percentile
// copy of dev_health_ops.metrics.compute._percentile's `sorted[lo]*(1-frac) +
// sorted[hi]*frac` interpolation, tested against the SAME
// tests/fixtures/fma_golden.json rows as every other duplicate.
func TestPercentileMatchesLivePythonBitExact(t *testing.T) {
	data, err := os.ReadFile(filepath.Join("..", "..", "..", "..", "tests", "fixtures", "fma_golden.json"))
	if err != nil {
		t.Fatal(err)
	}
	var fixture struct {
		PercentileFloat []struct {
			Values       []float64 `json:"values"`
			Percentile   float64   `json:"percentile"`
			ExpectedBits string    `json:"expected_bits"`
		} `json:"percentile_float"`
	}
	if err := json.Unmarshal(data, &fixture); err != nil {
		t.Fatal(err)
	}
	if len(fixture.PercentileFloat) == 0 {
		t.Fatal("fixture has no percentile_float cases")
	}
	mismatches := 0
	for _, testCase := range fixture.PercentileFloat {
		got := percentile(testCase.Values, testCase.Percentile)
		wantBits, err := strconv.ParseUint(strings.TrimPrefix(testCase.ExpectedBits, "0x"), 16, 64)
		if err != nil {
			t.Fatalf("parse expected_bits %q: %v", testCase.ExpectedBits, err)
		}
		gotBits := math.Float64bits(got)
		if gotBits != wantBits {
			mismatches++
			if mismatches <= 10 {
				t.Errorf("percentile(%v, %v) = %v (bits %#x), want bits %#x (%v)",
					testCase.Values, testCase.Percentile, got, gotBits, wantBits, math.Float64frombits(wantBits))
			}
		}
	}
	if mismatches > 10 {
		t.Errorf("... and %d more mismatches (total %d of %d cases)", mismatches-10, mismatches, len(fixture.PercentileFloat))
	}
}
