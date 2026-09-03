package numerical

import (
	"encoding/json"
	"math"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

// fmaGoldenFixture is the shape of tests/fixtures/fma_golden.json, generated
// by tests/fixtures/generate_fma_golden.py by IMPORTING and calling the real
// production Python functions. CHAOS-4818: Go's spec permits fusing an
// unguarded `x*y + z` into a single fused-multiply-add on arm64 (one
// rounding), where CPython always rounds the multiply and the add
// separately. Every case here asserts the exact IEEE-754 BIT PATTERN of the
// Go result against CPython's -- never "fused != unfused" (which is a no-op
// assertion on amd64, where this fusion typically does not happen).
type fmaGoldenFixture struct {
	ReleaseConfidence []struct {
		Coverage          float64 `json:"coverage"`
		TotalSessions     int     `json:"total_sessions"`
		ConcurrentDeploys int     `json:"concurrent_deploys"`
		MinimumSessions   int     `json:"minimum_sessions"`
		ExpectedBits      string  `json:"expected_bits"`
	} `json:"release_confidence"`
	PercentileFloat []struct {
		Values       []float64 `json:"values"`
		Percentile   float64   `json:"percentile"`
		ExpectedBits string    `json:"expected_bits"`
	} `json:"percentile_float"`
	PercentileInt []struct {
		SortedValues []int   `json:"sorted_values"`
		Percentile   float64 `json:"percentile"`
		Expected     int     `json:"expected"`
	} `json:"percentile_int"`
}

func loadFMAGolden(t *testing.T) fmaGoldenFixture {
	t.Helper()
	data, err := os.ReadFile(filepath.Join("..", "..", "..", "..", "tests", "fixtures", "fma_golden.json"))
	if err != nil {
		t.Fatal(err)
	}
	var fixture fmaGoldenFixture
	if err := json.Unmarshal(data, &fixture); err != nil {
		t.Fatal(err)
	}
	return fixture
}

// bitsFromHex parses "0x<16 hex digits>" (as generate_fma_golden.py's
// bits_hex writes it) into the uint64 math.Float64bits would return for the
// same value.
func bitsFromHex(t *testing.T, hex string) uint64 {
	t.Helper()
	value, err := strconv.ParseUint(strings.TrimPrefix(hex, "0x"), 16, 64)
	if err != nil {
		t.Fatalf("parse expected_bits %q: %v", hex, err)
	}
	return value
}

// TestReleaseImpactConfidenceMatchesLivePythonBitExact is the CHAOS-4818 red
// line: this is the exact ticket target (parity.go:321). The fixture's
// coverage x sessions x concurrent_deploys grid matches the shape of the
// 28,987-input sweep the ticket measured (12.5% fused != unfused), so it
// necessarily contains representatives of that divergent set.
func TestReleaseImpactConfidenceMatchesLivePythonBitExact(t *testing.T) {
	fixture := loadFMAGolden(t)
	if len(fixture.ReleaseConfidence) == 0 {
		t.Fatal("fixture has no release_confidence cases")
	}
	mismatches := 0
	for _, testCase := range fixture.ReleaseConfidence {
		got := ReleaseImpactConfidence(testCase.Coverage, testCase.TotalSessions, testCase.ConcurrentDeploys, testCase.MinimumSessions)
		wantBits := bitsFromHex(t, testCase.ExpectedBits)
		gotBits := math.Float64bits(got)
		if gotBits != wantBits {
			mismatches++
			if mismatches <= 10 {
				t.Errorf(
					"ReleaseImpactConfidence(coverage=%v, sessions=%d, concurrent=%d, minimum=%d) = %v (bits %#x), want bits %#x (%v)",
					testCase.Coverage, testCase.TotalSessions, testCase.ConcurrentDeploys, testCase.MinimumSessions,
					got, gotBits, wantBits, math.Float64frombits(wantBits),
				)
			}
		}
	}
	if mismatches > 10 {
		t.Errorf("... and %d more mismatches (total %d of %d cases)", mismatches-10, mismatches, len(fixture.ReleaseConfidence))
	}
}

// TestPercentileFloatFunctionsMatchLivePythonBitExact covers every duplicate
// of dev_health_ops.metrics.compute._percentile's `sorted[lo]*(1-frac) +
// sorted[hi]*frac` interpolation that lives in package numerical:
// deployPercentile (deploy.go:165). The four sibling copies in other
// packages (cicd, daily, repouser, testops) have their own copies of this
// test against the same fixture rows.
func TestPercentileFloatFunctionsMatchLivePythonBitExact(t *testing.T) {
	fixture := loadFMAGolden(t)
	if len(fixture.PercentileFloat) == 0 {
		t.Fatal("fixture has no percentile_float cases")
	}
	mismatches := 0
	for _, testCase := range fixture.PercentileFloat {
		got := deployPercentile(testCase.Values, testCase.Percentile)
		wantBits := bitsFromHex(t, testCase.ExpectedBits)
		gotBits := math.Float64bits(got)
		if gotBits != wantBits {
			mismatches++
			if mismatches <= 10 {
				t.Errorf("deployPercentile(%v, %v) = %v (bits %#x), want bits %#x (%v)",
					testCase.Values, testCase.Percentile, got, gotBits, wantBits, math.Float64frombits(wantBits))
			}
		}
	}
	if mismatches > 10 {
		t.Errorf("... and %d more mismatches (total %d of %d cases)", mismatches-10, mismatches, len(fixture.PercentileFloat))
	}
}

// TestIntegerPercentilesMatchesLivePython covers compute_capacity._percentile
// (int-truncating -- CHAOS-4818's FMA can move the interpolated float enough
// to flip which integer `int(...)` truncates to, even though the final type
// is int, not float64). Assertion is exact int equality, generated from live
// Python, not a bit pattern (there is no float64 result to inspect).
func TestIntegerPercentilesMatchesLivePython(t *testing.T) {
	fixture := loadFMAGolden(t)
	if len(fixture.PercentileInt) == 0 {
		t.Fatal("fixture has no percentile_int cases")
	}
	mismatches := 0
	for _, testCase := range fixture.PercentileInt {
		got := IntegerPercentiles(testCase.SortedValues, []float64{testCase.Percentile})
		if len(got) != 1 || got[0] != testCase.Expected {
			mismatches++
			if mismatches <= 10 {
				t.Errorf("IntegerPercentiles(%v, [%v]) = %v, want [%d]", testCase.SortedValues, testCase.Percentile, got, testCase.Expected)
			}
		}
	}
	if mismatches > 10 {
		t.Errorf("... and %d more mismatches (total %d of %d cases)", mismatches-10, mismatches, len(fixture.PercentileInt))
	}
}
