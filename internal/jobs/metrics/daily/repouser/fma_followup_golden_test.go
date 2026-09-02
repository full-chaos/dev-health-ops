package repouser

import (
	"encoding/json"
	"math"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/google/uuid"
)

// TestCodeOwnershipGiniFMAFollowupMatchesLivePythonBitExact is the
// regression test for the CHAOS-4818 AST-lint follow-up's second live
// finding: CodeOwnershipGini's `numerator += float64(index+1) * value` only
// converted the int operand -- the PRODUCT itself was still an unguarded
// compound-assignment FMA site (see fma_lint_test.go's package doc
// comment). Every churn value in fma_followup_golden.json's ownership_gini
// family is a small exact integer well under 2**53 -- this golden is
// scoped to the FMA-fusion class alone (CHAOS-4818), not the separate
// CHAOS-4824 compensated-sum/big.Int class for this same function (see
// generate_fma_followup_golden.py's module doc comment); a corpus
// exercising that class lives in CHAOS-4824's own PR, not here.
func TestCodeOwnershipGiniFMAFollowupMatchesLivePythonBitExact(t *testing.T) {
	data, err := os.ReadFile(filepath.Join("..", "..", "..", "..", "..", "tests", "fixtures", "fma_followup_golden.json"))
	if err != nil {
		t.Fatal(err)
	}
	var fixture struct {
		OwnershipGini []struct {
			Case         string `json:"case"`
			Churns       []int  `json:"churns"`
			ExpectedBits string `json:"expected_bits"`
		} `json:"ownership_gini"`
	}
	if err := json.Unmarshal(data, &fixture); err != nil {
		t.Fatal(err)
	}
	if len(fixture.OwnershipGini) == 0 {
		t.Fatal("fixture has no ownership_gini cases")
	}

	repoID := uuid.MustParse("00000000-0000-4000-8000-00000000000b")

	mismatches := 0
	for _, testCase := range fixture.OwnershipGini {
		windowStats := make([]CommitStatRow, len(testCase.Churns))
		for i, churn := range testCase.Churns {
			windowStats[i] = CommitStatRow{
				RepoID:      repoID,
				CommitHash:  "c" + strconv.Itoa(i),
				AuthorEmail: "a" + strconv.Itoa(i) + "@example.com",
				FilePath:    "src/shared.go",
				Additions:   churn,
			}
		}
		got := CodeOwnershipGini(repoID, windowStats)
		wantBits, err := strconv.ParseUint(strings.TrimPrefix(testCase.ExpectedBits, "0x"), 16, 64)
		if err != nil {
			t.Fatalf("case %s: parse expected_bits %q: %v", testCase.Case, testCase.ExpectedBits, err)
		}
		gotBits := math.Float64bits(got)
		if gotBits != wantBits {
			mismatches++
			if mismatches <= 10 {
				t.Errorf("case %s: CodeOwnershipGini(churns=%v) = %v (bits %#x), want bits %#x (%v)",
					testCase.Case, testCase.Churns, got, gotBits, wantBits, math.Float64frombits(wantBits))
			}
		}
	}
	if mismatches > 10 {
		t.Errorf("... and %d more mismatches (total %d of %d cases)", mismatches-10, mismatches, len(fixture.OwnershipGini))
	}
}
