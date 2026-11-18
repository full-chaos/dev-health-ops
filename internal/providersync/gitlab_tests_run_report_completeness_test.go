package providersync

import (
	"encoding/json"
	"io"
	"net/http"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
)

// CHAOS-13981: the GitLab tests route writes one report-completeness row per finished pipeline with report sources.

// gitLabJobsOverrideDoer serves the include_retried job listing of the report phase (the second listing of a pipeline) from
// reportJobs, and an empty test_report_summary; everything else goes to the wrapped doer.
type gitLabJobsOverrideDoer struct {
	*gitLabAllRefDoer
	reportJobsStatus int
	reportJobs       string
	jobListings      int
}

func (doer *gitLabJobsOverrideDoer) Do(request *http.Request) (*http.Response, error) {
	path := request.URL.Path
	respond := func(status int, body string) (*http.Response, error) {
		return &http.Response{StatusCode: status, Header: http.Header{"Content-Type": {"application/json"}}, ContentLength: -1,
			Body: io.NopCloser(strings.NewReader(body)), Request: request}, nil
	}
	if strings.HasSuffix(path, "/test_report_summary") {
		return respond(http.StatusOK, `{"test_suites":[]}`)
	}
	if strings.HasSuffix(path, "/jobs") && request.URL.Query().Get("include_retried") == "true" {
		doer.jobListings++
		if doer.jobListings == 2 && doer.reportJobsStatus != 0 {
			return respond(doer.reportJobsStatus, doer.reportJobs)
		}
	}
	return doer.gitLabAllRefDoer.Do(request)
}

func gitLabCompletenessRows(t *testing.T, batch CompleteRouteBatch) []testOpsRunReportCompletenessRow {
	t.Helper()
	var rows []testOpsRunReportCompletenessRow
	for _, effect := range batch.Effects {
		if effect.Destination != testOpsRunReportCompletenessDestination {
			continue
		}
		for _, raw := range effect.Rows {
			var row testOpsRunReportCompletenessRow
			if err := json.Unmarshal(raw, &row); err != nil {
				t.Fatal(err)
			}
			if !validTestOpsRunReportCompletenessRow(row) {
				t.Fatalf("row %+v is not the shape the sink accepts", row)
			}
			rows = append(rows, row)
		}
	}
	return rows
}

func TestGitLabTestsRunAttemptCountsEveryJobRetry(t *testing.T) {
	for _, cell := range []struct {
		name string
		jobs []string
		want int64
	}{
		{"no retry", []string{`{"id":10,"name":"unit"}`, `{"id":11,"name":"lint"}`}, 1},
		{"unit retried once", []string{`{"id":10,"name":"unit","retried":true}`, `{"id":12,"name":"unit"}`, `{"id":11,"name":"lint"}`}, 2},
		// The highest job rank is 2 here, as for one retry: a rule on it would give the second retry the first one's key.
		{"unit and lint retried once each", []string{`{"id":10,"name":"unit","retried":true}`, `{"id":12,"name":"unit"}`,
			`{"id":11,"name":"lint","retried":true}`, `{"id":13,"name":"lint"}`}, 3},
		{"unit retried twice", []string{`{"id":10,"name":"unit","retried":true}`, `{"id":12,"name":"unit","retried":true}`, `{"id":14,"name":"unit"}`}, 3},
	} {
		t.Run(cell.name, func(t *testing.T) {
			first, second := gitLabAttemptJobs(t, cell.jobs...), gitLabAttemptJobs(t, cell.jobs...)
			if got := first.runAttempt(); got != cell.want {
				t.Fatalf("runAttempt=%d want %d", got, cell.want)
			}
			if first.runAttempt() != second.runAttempt() {
				t.Fatal("the same job set must give the same key")
			}
		})
	}
}

func TestGitLabTestsRunReportRowOnlyForAFinishedPipelineWithReports(t *testing.T) {
	claim := nativeTestClaim("gitlab", "tests")
	at := time.Date(2026, 8, 14, 12, 0, 0, 123456789, time.UTC)
	repo := "c7198fbc-1945-3717-05d8-eb78866b4e79"
	report := func(expected, admitted int, reasons ...string) gitLabTestsRunReport {
		out := newGitLabTestsRunReport()
		out.expected, out.admitted = expected, admitted
		for _, reason := range reasons {
			out.lose(reason)
		}
		return out
	}
	pipeline := func(status string) gitLabTestsPipelinePayload {
		return gitLabTestsPipelinePayload{ID: "9001", Status: status}
	}
	for _, status := range []string{"running", "pending", "created", "manual", "scheduled", "waiting_for_resource", "preparing"} {
		if rows := gitLabTestsRunReportCompleteness(claim, repo, pipeline(status), 1, report(1, 1), at); rows != nil {
			t.Errorf("status %s: rows %+v, want none (the report set is not fixed)", status, rows)
		}
	}
	if rows := gitLabTestsRunReportCompleteness(claim, repo, pipeline("success"), 1, report(0, 0), at); rows != nil {
		t.Errorf("no report source and no loss: rows %+v, want none", rows)
	}
	for _, status := range []string{"success", "failed", "canceled", "skipped"} {
		rows := gitLabTestsRunReportCompleteness(claim, repo, pipeline(status), 2, report(2, 1, "per_run_cap", "artifact_oversized", "per_run_cap"), at)
		if len(rows) != 1 || !validTestOpsRunReportCompletenessRow(rows[0]) || rows[0].Complete || rows[0].RunAttempt != 2 ||
			rows[0].Provider != "gitlab_ci" || !slices.Equal(rows[0].Reasons, []string{"artifact_oversized", "per_run_cap"}) ||
			rows[0].ReportsExpected != 2 || rows[0].ReportsAdmitted != 1 {
			t.Errorf("status %s: rows %+v", status, rows)
		}
	}
	// A loss with no source read (the job list answered 404 and the pipeline had no native report) still names the run.
	rows := gitLabTestsRunReportCompleteness(claim, repo, pipeline("failed"), 1, report(0, 0, githubTestsArtifactUnavailableCause), at)
	if len(rows) != 1 || rows[0].Complete {
		t.Fatalf("rows %+v, want one partial row", rows)
	}
}

// Every reason the GitLab writer names is a value the closed vocabulary already has, classified as a loss; no GitLab-only value.
func TestGitLabTestsRunReportReasonsAreExistingLossReasons(t *testing.T) {
	named := []string{githubTestsArtifactUnavailableCause, githubTestsArtifactNotFoundReason, githubTestsPerRunCapCause,
		githubTestsCompressionRatioCause, githubTestsArchiveEntryCapCause}
	for _, reason := range gitLabTestsRunReportComponentReasons {
		named = append(named, reason)
	}
	for _, reason := range named {
		_, loss := githubTestsRunReportLossCauses[reason]
		_, verdictOnly := githubTestsRunReportVerdictOnlyReasons[reason]
		if !loss && !verdictOnly {
			t.Errorf("reason %q is not an existing loss reason", reason)
		}
	}
	// Every truncation component is either a per-pipeline skip with a reason, or not about one pipeline's reports.
	perPipeline := map[string]bool{gitLabReportMemberDroppedComponent: true}
	for component := range gitLabTestsRunReportComponentReasons {
		perPipeline[component] = true
	}
	notPerPipeline := map[string]bool{gitLabPipelineInventoryComponent: true, gitLabReportInventoryComponent: true,
		gitLabRunJobsComponent: true, gitLabRunJobsPageBudgetComponent: true}
	for component := range gitLabTestsTruncationComponents {
		if perPipeline[component] == notPerPipeline[component] {
			t.Errorf("component %q: per-pipeline=%v not-per-pipeline=%v, want exactly one", component, perPipeline[component], notPerPipeline[component])
		}
	}
}

func TestGitLabTestsRouteNamesSilentArtifactLossesOnTheRow(t *testing.T) {
	coverage := githubTestsZip(t, map[string]string{"coverage.info": githubTestsLCOVFixture})
	many := make([]gitLabAllRefArtifactJob, 0, gitLabTestsMaxArtifacts+1)
	for id := 100; id < 100+gitLabTestsMaxArtifacts+1; id++ {
		many = append(many, gitLabAllRefArtifactJob{ID: strconv.Itoa(id)})
	}
	for _, cell := range []struct {
		name          string
		jobs          []gitLabAllRefArtifactJob
		wantReasons   []string
		wantExpected  int64
		wantAdmitted  int64
		wantTruncated []string
	}{
		// More artifact jobs than the selection reads: the cut is named on the row; the unit result is unchanged (ticket 14144).
		{"over_the_artifact_cap", many, []string{"per_run_cap"}, 1 + gitLabTestsMaxArtifacts, 1 + gitLabTestsMaxArtifacts, nil},
		// A listed artifact gone at download (404/410) was a silent skip.
		{"artifact_gone", []gitLabAllRefArtifactJob{{ID: "21"}, {ID: "22", Gone: true}}, []string{"artifact_not_found"}, 3, 2, nil},
		{"every_report_read", []gitLabAllRefArtifactJob{{ID: "21"}}, []string{}, 2, 2, nil},
	} {
		t.Run(cell.name, func(t *testing.T) {
			pipelines := gitLabAllRefPipelines()[:1]
			pipelines[0].ArtifactJobs = cell.jobs
			walk, _ := gitLabAllRefCollectChunks(t, &gitLabAllRefDoer{t: t, archive: coverage, pipelines: pipelines}, nativeTestClaim("gitlab", "tests"))
			if walk.err != nil {
				t.Fatal(walk.err)
			}
			if truncated := gitLabAllRefTruncated(walk.final); !slices.Equal(truncated, cell.wantTruncated) {
				t.Fatalf("inventory_truncated=%v want %v", truncated, cell.wantTruncated)
			}
			rows := gitLabCompletenessRows(t, walk.batch)
			if len(rows) != 1 || rows[0].RunID != "9001" || rows[0].RunAttempt != 1 || rows[0].Complete != (len(cell.wantReasons) == 0) ||
				!slices.Equal(rows[0].Reasons, cell.wantReasons) || rows[0].ReportsExpected != cell.wantExpected || rows[0].ReportsAdmitted != cell.wantAdmitted {
				t.Fatalf("rows %+v, want reasons %v expected %d admitted %d", rows, cell.wantReasons, cell.wantExpected, cell.wantAdmitted)
			}
		})
	}
}

// An artifact with a report read and a member dropped at a size guard was not read whole: it is a source, not an admitted one.
func TestGitLabTestsGuardDroppedMemberKeepsTheArtifactOutOfAdmitted(t *testing.T) {
	for cause, archive := range silentSkipArchives(t) {
		t.Run(cause, func(t *testing.T) {
			pipelines := gitLabAllRefPipelines()[:1]
			pipelines[0].ArtifactJobs = []gitLabAllRefArtifactJob{{ID: "21"}}
			walk, _ := gitLabAllRefCollectChunks(t, &gitLabAllRefDoer{t: t, archive: archive, pipelines: pipelines}, nativeTestClaim("gitlab", "tests"))
			if walk.err != nil {
				t.Fatal(walk.err)
			}
			rows := gitLabCompletenessRows(t, walk.batch)
			if len(rows) != 1 || rows[0].Complete || !slices.Equal(rows[0].Reasons, []string{cause}) ||
				rows[0].ReportsExpected != 2 || rows[0].ReportsAdmitted != 1 {
				t.Fatalf("rows %+v, want partial [%s] expected 2 admitted 1 (the native report only)", rows, cause)
			}
		})
	}
}

// A retry whose retried marker is past the per-run job cap is still a new key: the attempt counts the whole listing.
func TestGitLabTestsRetryPastTheJobCapIsANewAttempt(t *testing.T) {
	jobs := make([]string, 0, githubTestsMaxJobsPerRun+2)
	for id := 1; id <= githubTestsMaxJobsPerRun; id++ {
		jobs = append(jobs, `{"id":`+strconv.Itoa(id)+`,"name":"job-`+strconv.Itoa(id)+`","status":"success"}`)
	}
	jobs = append(jobs, `{"id":1001,"name":"job-1","status":"success","retried":true}`, `{"id":1002,"name":"job-1","status":"success"}`)
	doer := &gitLabJobsOverrideDoer{gitLabAllRefDoer: &gitLabAllRefDoer{t: t, pipelines: gitLabAllRefPipelines()[:1]},
		reportJobsStatus: http.StatusOK, reportJobs: "[" + strings.Join(jobs, ",") + "]"}
	client := gitLabRepositoryClient(t, doer, "https://gitlab.example")
	var batch CompleteRouteBatch
	err := (GitLabTestsRouteHandler{}).CollectChunks(t.Context(), nativeTestClaim("gitlab", "tests"), providerfoundation.Credential{}, client,
		time.Date(2026, 8, 14, 12, 0, 0, 0, time.UTC), "",
		func(emission ChunkRouteEmission) error {
			batch.Effects = append(batch.Effects, emission.Batch.Effects...)
			return nil
		})
	if err != nil {
		t.Fatal(err)
	}
	rows := gitLabCompletenessRows(t, batch)
	if len(rows) != 1 || rows[0].RunAttempt != 2 || !slices.Equal(rows[0].Reasons, []string{"per_run_cap"}) {
		t.Fatalf("rows %+v, want attempt 2 partial [per_run_cap]", rows)
	}
}
