package providersync

import (
	"encoding/json"
	"testing"
	"time"
)

// CHAOS-13875: the attempt of a GitLab report slot is the retry rank of its job(s). Attempt 1 keeps the keys rows always had.

func gitLabAttemptJobs(t *testing.T, items ...string) gitLabTestsRunJobs {
	t.Helper()
	raw := make([]json.RawMessage, 0, len(items))
	for _, item := range items {
		raw = append(raw, json.RawMessage(item))
	}
	jobs, err := newGitLabTestsRunJobs(raw)
	if err != nil {
		t.Fatal(err)
	}
	return jobs
}

func TestGitLabTestsRunJobsRankByIDWithinAName(t *testing.T) {
	jobs := gitLabAttemptJobs(t,
		`{"id":12,"name":"rspec 2/2","retried":false}`, `{"id":10,"name":"rspec 1/2","retried":false}`,
		`{"id":11,"name":"rspec 2/2","retried":true}`, `{"id":9,"name":"rspec 2/2","retried":true}`)
	if !jobs.anyRetried {
		t.Fatal("a retried job is listed")
	}
	for id, want := range map[string]int64{"9": 1, "11": 2, "12": 3, "10": 1, "999": 1} {
		if got := jobs.jobAttempt(id); got != want {
			t.Errorf("jobAttempt(%s)=%d want %d", id, got, want)
		}
	}
	summary := map[string][]string{"rspec": {"10", "12"}, "other": {"404"}}
	if got := jobs.suiteAttempt("rspec", summary); got != 3 {
		t.Errorf("parallel suite attempt=%d want the max rank 3 over its build ids", got)
	}
	if got := jobs.suiteAttempt("rspec 2/2", nil); got != 3 {
		t.Errorf("name fallback attempt=%d want 3", got)
	}
	if got := jobs.suiteAttempt("other", summary); got != 1 {
		t.Errorf("a suite whose build ids are not listed (child pipeline) and whose name is no job: attempt=%d want 1", got)
	}
	if got := jobs.suiteAttempt("rspec", nil); got != 1 {
		t.Errorf("no summary and no job named rspec: attempt=%d want 1 (parallel jobs stay unfixed)", got)
	}
}

func TestGitLabTestsRunJobsWithoutARetryRankEverythingOne(t *testing.T) {
	jobs := gitLabAttemptJobs(t, `{"id":10,"name":"unit"}`, `{"id":11,"name":"unit"}`)
	if jobs.anyRetried || jobs.jobAttempt("11") != 1 || jobs.suiteAttempt("unit", nil) != 1 {
		t.Fatalf("no retried job: every attempt is 1, got %+v", jobs)
	}
}

func TestGitLabTestsRunJobsAListCutShortOnlyLowersARank(t *testing.T) {
	full := gitLabAttemptJobs(t, `{"id":11,"name":"unit","retried":true}`, `{"id":12,"name":"unit"}`)
	cut := gitLabAttemptJobs(t, `{"id":12,"name":"unit"}`)
	if full.jobAttempt("12") != 2 || cut.jobAttempt("12") > full.jobAttempt("12") {
		t.Fatalf("full=%d cut=%d: a truncated list must never raise a rank", full.jobAttempt("12"), cut.jobAttempt("12"))
	}
}

func TestGitLabTestsArtifactSelectionDropsRetriedJobsBeforeTheCap(t *testing.T) {
	raw := []json.RawMessage{
		json.RawMessage(`{"id":1,"name":"a","retried":true,"artifacts_file":{"filename":"r.zip"}}`),
		json.RawMessage(`{"id":2,"name":"a","retried":true,"artifacts_file":{"filename":"r.zip"}}`),
		json.RawMessage(`{"id":3,"name":"a","artifacts_file":{"filename":"r.zip"}}`),
	}
	selected, err := selectGitLabTestsArtifactJobs(raw, 1)
	if err != nil || len(selected) != 1 || stringValue(selected[0].ID) != "3" {
		t.Fatalf("selected=%+v err=%v; want only the live job 3 (retried jobs use no cap slot)", selected, err)
	}
}

func TestGitLabNativeTestReportAttemptOneKeepsTheLegacyKeys(t *testing.T) {
	claim := nativeTestClaim("gitlab", "tests")
	at := time.Date(2026, 8, 3, 12, 0, 0, 0, time.UTC)
	report := gitLabTestsReportPayload{Suites: []gitLabTestsSuitePayload{{Name: "rspec", Cases: []gitLabTestsCasePayload{
		{Name: "A", Status: "success"}, {Name: "A", Status: "failed"},
	}}}}
	legacySuites, legacyCases, _, err := normalizeGitLabNativeTestReport(claim, "repo", "9001", report, nil, nil, at)
	if err != nil {
		t.Fatal(err)
	}
	attempt := func(n int64) func(string) int64 { return func(string) int64 { return n } }
	one, oneCases, _, err := normalizeGitLabNativeTestReportAttempts(claim, "repo", "9001", report, nil, nil, at, attempt(1))
	if err != nil {
		t.Fatal(err)
	}
	two, twoCases, _, err := normalizeGitLabNativeTestReportAttempts(claim, "repo", "9001", report, nil, nil, at, attempt(2))
	if err != nil {
		t.Fatal(err)
	}
	again, againCases, _, err := normalizeGitLabNativeTestReportAttempts(claim, "repo", "9001", report, nil, nil, at, attempt(2))
	if err != nil {
		t.Fatal(err)
	}
	if one[0].SuiteID != legacySuites[0].SuiteID || oneCases[0].CaseID != legacyCases[0].CaseID || oneCases[1].CaseID != legacyCases[1].CaseID {
		t.Fatal("attempt 1 must keep the suite and case ids rows always had")
	}
	if two[0].SuiteID == one[0].SuiteID || twoCases[0].CaseID == oneCases[0].CaseID || twoCases[1].CaseID == twoCases[0].CaseID {
		t.Fatal("attempt 2 must have its own suite and case ids (and the ordinal split must survive)")
	}
	if again[0].SuiteID != two[0].SuiteID || againCases[0].CaseID != twoCases[0].CaseID {
		t.Fatal("the same attempt read again must keep its keys")
	}
	if two[0].ReportSlot != "rspec" || two[0].RunAttempt != 2 || one[0].ReportSlot != "rspec" || one[0].RunAttempt != 1 {
		t.Fatalf("slot/attempt labels: %+v %+v", one[0], two[0])
	}
}

func TestGitLabTestsJobArtifactCoverageKeysFollowTheAttempt(t *testing.T) {
	at := time.Date(2026, 8, 3, 12, 0, 0, 0, time.UTC)
	archive := githubTestsZip(t, map[string]string{"coverage.info": "SF:a.go\nDA:1,1\nDA:2,0\nend_of_record\n"})
	jobs := gitLabAttemptJobs(t, `{"id":11,"name":"unit","retried":true}`, `{"id":12,"name":"unit"}`)
	parse := func(id string) githubTestsReportRows {
		var job gitLabTestsJobPayload
		if err := json.Unmarshal([]byte(`{"id":`+id+`,"name":"unit"}`), &job); err != nil {
			t.Fatal(err)
		}
		rows, err := parseGitLabTestsJobArtifact(archive, job, jobs, "repo", "9001", "org-a", nil, nil, at)
		if err != nil || len(rows.Coverage) != 1 {
			t.Fatalf("job %s: %+v err=%v", id, rows, err)
		}
		return rows
	}
	first, retry := parse("11"), parse("12")
	legacy, err := parseGitHubTestsArtifact(archive, "11", "repo", "9001", "org-a", nil, nil, at)
	if err != nil {
		t.Fatal(err)
	}
	if first.Coverage[0].SnapshotID != legacy.Coverage[0].SnapshotID {
		t.Fatal("attempt 1 coverage keeps the job id key")
	}
	if retry.Coverage[0].SnapshotID == first.Coverage[0].SnapshotID {
		t.Fatal("the retried job writes its own snapshot")
	}
	if retry.Coverage[0].ReportSlot != "unit" || retry.Coverage[0].RunAttempt != 2 || first.Coverage[0].RunAttempt != 1 {
		t.Fatalf("labels: %+v %+v", first.Coverage[0], retry.Coverage[0])
	}
}
