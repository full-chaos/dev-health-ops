package analytics

import (
	"os"
	"testing"
)

// TestLatestWorkUnitInvestmentsSourceExportIsAPureRename proves CHAOS-4977's
// export (latestWorkUnitInvestmentsSource -> LatestWorkUnitInvestmentsSource)
// changed nothing but the identifier's case. testdata/latest_work_unit_
// investments_source_pre_export.sql is the exact byte-for-byte output the
// UNEXPORTED function returned, captured immediately before the rename
// (org_id={org_id:String} placeholder unresolved, exactly as the function
// always returns it -- no live query was run to produce this). If this
// test ever needs updating because the SQL genuinely changed, that change
// must go through the same review as any edit to this dedup-critical CTE
// (CHAOS-2374 cross-org leak, CHAOS-4547 null-transition history) --
// regenerate the golden deliberately, never silently.
func TestLatestWorkUnitInvestmentsSourceExportIsAPureRename(t *testing.T) {
	want, err := os.ReadFile("testdata/latest_work_unit_investments_source_pre_export.sql")
	if err != nil {
		t.Fatalf("read golden: %v", err)
	}

	got := LatestWorkUnitInvestmentsSource()
	if got != string(want) {
		t.Fatalf("LatestWorkUnitInvestmentsSource() output changed by the export rename.\n--- want (pre-export) ---\n%s\n--- got (post-export) ---\n%s", want, got)
	}
}
