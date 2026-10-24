package goapiproof

import "testing"

func TestInvestmentParity_DeclaresTheUncategorizedCountAsGoOnly(t *testing.T) {
	baseline := `{"theme_distribution":[],"subcategory_distribution":[],"evidence_quality_distribution":{},"evidence_quality_stats":{"mean":0.4,"stddev":0.1,"total":10,"band_counts":{},"quality_drivers":[]}}`
	for _, count := range []string{"0", "4"} {
		candidate := `{"theme_distribution":[],"subcategory_distribution":[],"evidence_quality_distribution":{},"evidence_quality_stats":{"mean":0.4,"stddev":0.1,"total":10,"band_counts":{},"quality_drivers":[],"uncategorized_count":` + count + `}}`
		result := Compare(restSnapshotFromJSON(t, baseline), restSnapshotFromJSON(t, candidate), investmentParity)
		if result.TerminalState != TerminalStateMatch || result.DifferencesOutsideBaselineDefect != 0 {
			t.Errorf("uncategorized_count=%s: terminal %q outside %d, want a match with the key declared Go-only: %+v", count, result.TerminalState, result.DifferencesOutsideBaselineDefect, result.Findings)
		}
	}
}
