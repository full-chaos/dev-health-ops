package providersync

import "testing"

// CHAOS-14723: a run of 142,951 report rows (the largest run seen on prod, Nov 2026; synthetic rows here) is under the run bound,
// so every row commits, with no per_run_cap observation, marker or Warn line.
func TestGitHubTestsRunOfTheLargestSeenRowCountCommitsEveryRow(t *testing.T) {
	const want = 142_951
	records := captureMembershipLogs(t)
	a, b, c := part("big-a", 1, 47_650), part("big-b", 1, 47_650), part("big-c", 1, 47_648)
	if got := rowsOfSuites(a) + rowsOfSuites(b) + rowsOfSuites(c); got != want {
		t.Fatalf("fixture holds %d rows, want %d", got, want)
	}
	walk, doer, _ := rowsWalk(t, 1, rowsArtifact{id: "71", size: 10, body: fxJUnit(a)},
		rowsArtifact{id: "72", size: 11, body: fxJUnit(b)}, rowsArtifact{id: "73", size: 12, body: fxJUnit(c)})
	if got := committedReportRows(walk); got != want {
		t.Errorf("committed %d report rows, want all %d", got, want)
	}
	if got := observed(walk, githubTestsPerRunCapCause); got != 0 || len(walk.cursor.SkippedArtifacts) != 0 {
		t.Errorf("per_run_cap %d, markers %+v; want none", got, walk.cursor.SkippedArtifacts)
	}
	if len(doer.downloaded) != 3 {
		t.Errorf("downloaded %v, want all 3", doer.downloaded)
	}
	for _, record := range *records {
		if record.Message == "provider per-run item cap reached; run committed with partial items" {
			t.Errorf("per-run cap Warn line: %v", membershipLogAttrs(record))
		}
	}
	if walk.cursor.Phase != "done" || !sameOptionalTime(walk.final.Watermark, nativeTestClaim("github", "cicd").BeforeAt) {
		t.Errorf("phase %q, watermark %v: a complete run must finalize and advance to the window end", walk.cursor.Phase, walk.final.Watermark)
	}
	if complete, ok := walk.final.Result["reports_complete"].(bool); !ok || !complete {
		t.Errorf("reports_complete=%v, want true", walk.final.Result["reports_complete"])
	}
}
