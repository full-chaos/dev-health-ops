package analytics

import (
	"os"
	"testing"
)

// TestLatestWorkUnitRepoEffortSourceExportIsAPureRename and
// TestBuildUnitTeamSubqueryExportIsAPureRename prove that exporting
// (latestWorkUnitRepoEffortSource -> LatestWorkUnitRepoEffortSource,
// buildUnitTeamSubquery/unitTeamSubqueryOptions -> BuildUnitTeamSubquery/
// UnitTeamSubqueryOptions) changed nothing but the identifiers' case, the
// same discipline TestLatestWorkUnitInvestmentsSourceExportIsAPureRename
// (investment_export_rename_test.go) already established for the sibling
// export. The two testdata/*_pre_export.sql goldens are the exact
// byte-for-byte output the UNEXPORTED functions returned immediately
// before this rename -- regenerate deliberately, never silently, the same
// rule that file's own doc comment states.
func TestLatestWorkUnitRepoEffortSourceExportIsAPureRename(t *testing.T) {
	want, err := os.ReadFile("testdata/latest_work_unit_repo_effort_source_pre_export.sql")
	if err != nil {
		t.Fatalf("read golden: %v", err)
	}

	got := LatestWorkUnitRepoEffortSource()
	if got != string(want) {
		t.Fatalf("LatestWorkUnitRepoEffortSource() output changed by the export rename.\n--- want (pre-export) ---\n%s\n--- got (post-export) ---\n%s", want, got)
	}
}

func TestBuildUnitTeamSubqueryExportIsAPureRename(t *testing.T) {
	want, err := os.ReadFile("testdata/build_unit_team_subquery_pre_export.sql")
	if err != nil {
		t.Fatalf("read golden: %v", err)
	}

	got := BuildUnitTeamSubquery(UnitTeamSubqueryOptions{
		Source:         "latest_work_unit_investments AS work_unit_investments",
		Where:          "                WHERE work_unit_investments.from_ts < {end_ts:DateTime64(3, 'UTC')}\n                  AND work_unit_investments.to_ts >= {start_ts:DateTime64(3, 'UTC')}\n                  AND work_unit_investments.org_id = {org_id:String}",
		InnerTeamAlias: "team",
		IncludeTeamID:  false,
	})
	if got != string(want) {
		t.Fatalf("BuildUnitTeamSubquery() output changed by the export rename.\n--- want (pre-export) ---\n%s\n--- got (post-export) ---\n%s", want, got)
	}
}
