package testopswindow

import (
	"context"
	"strings"
	"testing"
)

// The window answer serves the summed report completeness counts of the measured repository days and the number of days not
// measured, as read; a window with no row serves 0 runs and 0 unmeasured days.
func TestResolve_ServesReportCompleteness(t *testing.T) {
	for name, tc := range map[string]struct {
		sums                                    [4]uint64
		runs, partial, unmarked, unmeasuredDays int
	}{
		"partial, unmarked and an unmeasured day": {[4]uint64{7, 2, 3, 1}, 7, 2, 3, 1},
		"all complete":         {[4]uint64{5, 0, 0, 0}, 5, 0, 0, 0},
		"every day unmeasured": {[4]uint64{0, 0, 0, 4}, 0, 0, 0, 4},
		"no row":               {[4]uint64{0, 0, 0, 0}, 0, 0, 0, 0},
	} {
		row := casesRow(8, 2, 0, 0, 10, uint64(1), uint64(5), 0)
		row = append(row[:8], tc.sums[0], tc.sums[1], tc.sums[2], tc.sums[3])
		client := &fakeClient{coverage: []any{uint64(4), uint64(1)}, cases: row}
		got, err := Resolve(context.Background(), client, "org-1", date(t, "2026-10-31"), date(t, "2026-11-13"), Scope{})
		if err != nil {
			t.Fatal(err)
		}
		rc := got.ReportCompleteness
		if rc == nil || rc.ReportRuns != tc.runs || rc.PartialRuns != tc.partial || rc.UnmarkedRuns != tc.unmarked || rc.UnmeasuredDays != tc.unmeasuredDays {
			t.Errorf("%s: reportCompleteness = %+v, want %d/%d/%d/%d", name, rc, tc.runs, tc.partial, tc.unmarked, tc.unmeasuredDays)
		}
	}
}

// The counts are summed over the same newest live rows as the cases; a NULL count is never read as 0 of a measured day.
func TestStatements_ReportCompletenessReadsTheNewestLiveRows(t *testing.T) {
	client := &fakeClient{coverage: []any{uint64(1), uint64(1)}, cases: casesRow(1, 0, 0, 0, 1, uint64(0), uint64(1), 0)}
	if _, _, _, _, _, _ = resolve(t, client, Scope{}); t.Failed() {
		return
	}
	for _, want := range []string{
		"ifNull(sum(report_runs), 0)",
		"ifNull(sum(partial_report_runs), 0)",
		"ifNull(sum(unmarked_report_runs), 0)",
		"countIf(report_runs IS NULL OR partial_report_runs IS NULL OR unmarked_report_runs IS NULL)",
		"LIMIT 1 BY org_id, repo_id, day",
	} {
		if !strings.Contains(client.casesText, want) {
			t.Errorf("cases statement lacks %q", want)
		}
	}
}
