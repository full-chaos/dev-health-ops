//go:build !race

package goapiproof

import (
	"fmt"
	"os"
	"testing"
)

// CHAOS-8135: the three exhaustive enumerations below are CPU-bound and run
// 100-140 s each under the race detector (the package took 486 s on a shared
// runner and then crossed the 600 s leg timeout). They test pure comparison
// logic, so the race detector adds cost and no signal. They are compiled out of
// the -race leg ONLY. The non-race unit leg runs them, and ci/check_go.sh
// (check_race_excluded_ran) fails unless it runs each one by name, uncached and
// without -race, and sees its own PASS line.

// TestCandidateAccounting_EnumeratedAdmissionImpliesTheInvariant runs every
// generated baseline against every candidate of up to three rows, with and
// without a page cut, through each duplicate shape of each route's bound
// Options. The integration build runs the same enumeration up to four rows.
func TestCandidateAccounting_EnumeratedAdmissionImpliesTheInvariant(t *testing.T) {
	t.Parallel()
	runAccountingEnumeration(t, 3)
}

// TestPersonDrilldownPRs_EnumeratedCursorAdmissionImpliesTheInvariant runs
// every generated baseline against every candidate of up to two rows,
// with and without a page cut, under every pair of cursor choices,
// through the person route's own bound Options. A comparison admitted with
// a next_cursor finding naming a different instant on each leg must
// satisfy cursorOracle. The integration build runs candidates up to three
// rows.
func TestPersonDrilldownPRs_EnumeratedCursorAdmissionImpliesTheInvariant(t *testing.T) {
	t.Parallel()
	runCursorEnumeration(t, 2)
}

// TestWriteSkewInvariant_RealSunburstDeclarations runs the generator on the
// captured team-scoped sunburst body under that case's real declarations,
// varying the value of one and two keyed elements.
func TestWriteSkewInvariant_RealSunburstDeclarations(t *testing.T) {
	t.Parallel()
	opts := investmentSunburstTeamScopedParityWithLimit(investmentSunburstDefaultLimit)
	raw, err := os.ReadFile("testdata/investmentsunburst_teamscoped_skew_baseline_3cf72260.json")
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	decl, _ := findOrderInsensitiveList(opts, "data")
	var rows []any
	decodeNumbers(t, raw, &rows)
	// A small slice of the real body keeps the enumeration fast; every
	// element keeps its real shape and keys.
	rows = rows[:8]
	keyOf := func(element any) string {
		k, _ := orderInsensitiveKey(element, decl.KeyFields)
		return k
	}
	leafFor := func(index int) skewLeaf {
		key := keyOf(rows[index])
		return skewLeaf{
			name: key,
			path: "data.value",
			set: func(body any, symbol string) {
				for _, element := range body.([]any) {
					if keyOf(element) == key {
						object := element.(map[string]any)
						if symbol == "missing" {
							delete(object, "value")
							return
						}
						object["value"] = skewValue(symbol)
					}
				}
			},
			matches: func(f Finding) bool {
				keys := findingKeys(f.Detail)
				return len(keys) == 1 && keys[0] == key
			},
		}
	}
	base := func() any {
		var copyRows []any
		decodeNumbers(t, mustMarshal(t, rows), &copyRows)
		return copyRows
	}
	for _, leaves := range [][]skewLeaf{{leafFor(0)}, {leafFor(0), leafFor(1)}} {
		t.Run(fmt.Sprintf("%d leaves", len(leaves)), func(t *testing.T) {
			counts := runSkewGenerator(t, base, leaves, []Options{opts})
			t.Logf("real sunburst team-scoped declarations, %d leaves: %s", len(leaves), formatSkewCounts(counts))
		})
	}
}
