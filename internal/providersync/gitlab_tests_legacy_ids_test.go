package providersync

import (
	"encoding/json"
	"slices"
	"testing"
	"time"
)

// CHAOS-13925: a GitLab retry (attempt >= 2) writes new keys, so its rows list the keys the writer before CHAOS-13875 gave the
// same report; attempt 1 keeps those keys and lists nothing. The want values are the suite_id and snapshot_id that writer (ops
// 4444a99f) stored for pipeline 9001 through its real chunked route and sink (two-sha cell CELL-GITLAB-MIXED-1). They are recorded,
// not computed here, so a change of the key formula on either side turns these cells red.
const (
	gitLabLegacySuiteUnit       = "cc727586b1a20e62db45cd89b67647deeb1ab151f4387f4a12b471c37959d6ef"
	gitLabLegacySuiteUnitSecond = "7dc517477145ab6013c37155514667fe2a4a88b0f68b5d1e1b4ca683ddaadd3b"
	gitLabLegacyCoverageJob10   = "1d79fc90643036e29f1ad24a47bfee0ef47e3f7fa8eea18f820f02b621ee04e0"
	gitLabLegacyCoverageJob11   = "262582eff181b66151e1e70e3252a4376406ddbdc8d8a449d22e259d904f7033"
	gitLabLegacyCoverageJob12   = "8e79f62517be7bfc558056d31cef3c22115a7573018effb960aa6be37bf3c2b8"
)

// The report holds "unit" twice; the second suite's key folds in its ordinal. A retry lists each suite's own pre-slot key.
func TestGitLabTestsRetrySuiteRowsListThePreSlotKey(t *testing.T) {
	claim := nativeTestClaim("gitlab", "tests")
	at := time.Date(2026, 8, 3, 12, 0, 0, 0, time.UTC)
	report := gitLabTestsReportPayload{Suites: []gitLabTestsSuitePayload{
		{Name: "unit", Cases: []gitLabTestsCasePayload{{Name: "TestX", Status: "success"}}},
		{Name: "unit", Cases: []gitLabTestsCasePayload{{Name: "TestY", Status: "failed"}}},
	}}
	attempt := func(n int64) func(string) int64 { return func(string) int64 { return n } }
	one, _, _, err := normalizeGitLabNativeTestReportAttempts(claim, "repo", "9001", report, nil, nil, at, attempt(1))
	if err != nil {
		t.Fatal(err)
	}
	two, _, _, err := normalizeGitLabNativeTestReportAttempts(claim, "repo", "9001", report, nil, nil, at, attempt(2))
	if err != nil {
		t.Fatal(err)
	}
	if len(one) != 2 || len(two) != 2 {
		t.Fatalf("suites: attempt 1 %d, attempt 2 %d; want 2 each", len(one), len(two))
	}
	if one[0].SuiteID != gitLabLegacySuiteUnit || one[1].SuiteID != gitLabLegacySuiteUnitSecond {
		t.Fatalf("attempt 1 keys %s %s; want the pre-slot keys, which FINAL replaces in place", one[0].SuiteID, one[1].SuiteID)
	}
	if len(one[0].LegacySuiteIDs) != 0 || len(one[1].LegacySuiteIDs) != 0 {
		t.Fatalf("attempt 1 lists %v %v; want nothing", one[0].LegacySuiteIDs, one[1].LegacySuiteIDs)
	}
	got := [][]string{two[0].LegacySuiteIDs, two[1].LegacySuiteIDs}
	want := [][]string{{gitLabLegacySuiteUnit}, {gitLabLegacySuiteUnitSecond}}
	if !slices.EqualFunc(got, want, func(a, b []string) bool { return slices.Equal(a, b) }) {
		t.Fatalf("attempt 2 legacy ids\n  got  %v\n  want %v", got, want)
	}
}

// A retried job's coverage lists the pre-slot key under its own job id, then under each job of the name it retried; a job at
// attempt 1 (also the other job of a parallel pair) lists nothing.
func TestGitLabTestsRetryCoverageRowsListThePreSlotKeys(t *testing.T) {
	at := time.Date(2026, 8, 3, 12, 0, 0, 0, time.UTC)
	archive := githubTestsZip(t, map[string]string{"coverage.info": "SF:a.go\nDA:1,1\nDA:2,0\nend_of_record\n"})
	for _, tc := range []struct {
		name  string
		jobs  []string
		parse map[string][]string
	}{
		{"one job retried once", []string{`{"id":11,"name":"unit","retried":true}`, `{"id":12,"name":"unit"}`},
			map[string][]string{"11": nil, "12": {gitLabLegacyCoverageJob12, gitLabLegacyCoverageJob11}}},
		{"parallel pair, 2/2 retried", []string{`{"id":10,"name":"rspec 1/2"}`, `{"id":11,"name":"rspec 2/2","retried":true}`, `{"id":12,"name":"rspec 2/2"}`},
			map[string][]string{"10": nil, "12": {gitLabLegacyCoverageJob12, gitLabLegacyCoverageJob11}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			jobs := gitLabAttemptJobs(t, tc.jobs...)
			for id, want := range tc.parse {
				var job gitLabTestsJobPayload
				for _, raw := range tc.jobs {
					var candidate gitLabTestsJobPayload
					if err := json.Unmarshal([]byte(raw), &candidate); err != nil {
						t.Fatal(err)
					}
					if stringValue(candidate.ID) == id {
						job = candidate
					}
				}
				rows, err := parseGitLabTestsJobArtifact(archive, job, jobs, "repo", "9001", "org-a", nil, nil, at)
				if err != nil || len(rows.Coverage) != 1 {
					t.Fatalf("job %s: %+v err=%v", id, rows, err)
				}
				row := rows.Coverage[0]
				if !slices.Equal(row.LegacySnapshotIDs, want) {
					t.Fatalf("job %s (attempt %d) lists %v, want %v", id, row.RunAttempt, row.LegacySnapshotIDs, want)
				}
				if row.RunAttempt == 1 && id == "10" && row.SnapshotID != gitLabLegacyCoverageJob10 {
					t.Fatalf("job 10 at attempt 1 key %s; want the pre-slot key %s", row.SnapshotID, gitLabLegacyCoverageJob10)
				}
				if row.RunAttempt == 1 && id == "11" && row.SnapshotID != gitLabLegacyCoverageJob11 {
					t.Fatalf("job 11 at attempt 1 key %s; want the pre-slot key %s", row.SnapshotID, gitLabLegacyCoverageJob11)
				}
			}
		})
	}
}
