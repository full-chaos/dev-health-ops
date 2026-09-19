//go:build integration

package goapiproof

import "testing"

// TestCandidateAccounting_EnumeratedAdmissionImpliesTheInvariantToFourRows
// extends the unit enumeration to candidates of four rows, which adds the
// longer-than-baseline relation for every three-id baseline.
func TestCandidateAccounting_EnumeratedAdmissionImpliesTheInvariantToFourRows(t *testing.T) {
	runAccountingEnumeration(t, 4)
}

// TestPersonDrilldownPRs_EnumeratedCursorAdmissionImpliesTheInvariantToThreeRows
// extends the cursor enumeration to candidates of three rows.
func TestPersonDrilldownPRs_EnumeratedCursorAdmissionImpliesTheInvariantToThreeRows(t *testing.T) {
	runCursorEnumeration(t, 3)
}
