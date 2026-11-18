//go:build integration

package providersync

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
	clickhousestore "github.com/full-chaos/dev-health-ops/internal/storage/clickhouse"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/chschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

// CHAOS-13981: GitLab pipelines are synced through the production chunked route into ClickHouse (the real migration chain) and
// read with the reader rule of migration 153: a run with a kept suite and no completeness row is unmarked; a run whose
// newest-attempt row has complete = 0 is partial.

type gitLabCompletenessStack struct {
	ctx  context.Context
	conn driver.Conn
	sink TestOpsClickHouseEffects
}

func newGitLabCompletenessStack(t *testing.T) gitLabCompletenessStack {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	t.Cleanup(cancel)
	instance, err := containers.StartClickHouse(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = instance.Close(context.Background()) })
	conn, err := clickhousestore.Open(ctx, clickhousestore.DefaultConfig(instance.URI))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	chschema.Apply(ctx, t, instance)
	return gitLabCompletenessStack{ctx: ctx, conn: conn,
		sink: TestOpsClickHouseEffects{Conn: conn, Lease: providerfoundation.LeaseGuardFunc(func(context.Context) error { return nil })}}
}

// walk runs the chunked route once and writes every emitted effect through the production sink.
func (stack gitLabCompletenessStack) walk(t *testing.T, claim Claim, doer providerfoundation.HTTPDoer, at time.Time) CompleteRouteBatch {
	t.Helper()
	client := gitLabRepositoryClient(t, doer, "https://gitlab.example")
	client.Metrics = providerfoundation.NewMetrics()
	var final CompleteRouteBatch
	err := (GitLabTestsRouteHandler{}).CollectChunks(stack.ctx, claim, providerfoundation.Credential{}, client, at, "",
		func(emission ChunkRouteEmission) error {
			for _, effect := range emission.Batch.Effects {
				if len(effect.Rows) == 0 {
					continue
				}
				if err := stack.sink.WriteEffect(stack.ctx, claim, effect); err != nil {
					return fmt.Errorf("write %s: %w", effect.Destination, err)
				}
			}
			if emission.Final {
				final = emission.Batch
			}
			return nil
		})
	if err != nil {
		t.Fatalf("CollectChunks: %v", err)
	}
	return final
}

// reader is the migration 153 rule over runs with a kept suite: partial = newest attempt row complete = 0, unmarked = no row.
func (stack gitLabCompletenessStack) reader(t *testing.T, org string) (reportRuns, partialRuns, unmarkedRuns uint64) {
	t.Helper()
	if err := stack.conn.QueryRow(stack.ctx, `
SELECT count(), countIf(has_row AND newest_complete = 0), countIf(NOT has_row)
FROM (
  SELECT s.run_id AS run_id, c.run_id != '' AS has_row, c.complete AS newest_complete
  FROM (SELECT DISTINCT repo_id, run_id FROM test_suite_results FINAL WHERE org_id = ?) AS s
  LEFT JOIN (
    SELECT repo_id, run_id, argMax(complete, run_attempt) AS complete
    FROM testops_run_report_completeness FINAL WHERE org_id = ? GROUP BY repo_id, run_id
  ) AS c ON c.repo_id = s.repo_id AND c.run_id = s.run_id
) SETTINGS join_use_nulls = 0`, org, org).Scan(&reportRuns, &partialRuns, &unmarkedRuns); err != nil {
		t.Fatal(err)
	}
	return reportRuns, partialRuns, unmarkedRuns
}

type gitLabCompletenessVerdict struct {
	Attempt  uint32
	Complete uint8
	Reasons  []string
	Expected uint32
	Admitted uint32
}

func (stack gitLabCompletenessStack) verdicts(t *testing.T, org, runID string) []gitLabCompletenessVerdict {
	t.Helper()
	rows, err := stack.conn.Query(stack.ctx, `SELECT run_attempt, complete, CAST(reasons, 'Array(String)'), reports_expected, reports_admitted
FROM testops_run_report_completeness FINAL WHERE org_id = ? AND run_id = ? ORDER BY run_attempt`, org, runID)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var out []gitLabCompletenessVerdict
	for rows.Next() {
		var verdict gitLabCompletenessVerdict
		if err := rows.Scan(&verdict.Attempt, &verdict.Complete, &verdict.Reasons, &verdict.Expected, &verdict.Admitted); err != nil {
			t.Fatal(err)
		}
		out = append(out, verdict)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return out
}

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

func TestGitLabTestsPipelineWithASkippedReportIsNamedPartial(t *testing.T) {
	stack := newGitLabCompletenessStack(t)
	coverage := githubTestsZip(t, map[string]string{"coverage.info": githubTestsLCOVFixture})
	cells := []struct {
		name       string
		archive    []byte
		jobs       []gitLabAllRefArtifactJob
		pad        int
		jobsStatus int
		want       gitLabCompletenessVerdict
		wantRuns   [3]uint64
	}{
		// One artifact over the download cap (Content-Length above it, nothing read) beside a read one.
		{name: "R1_artifact_oversized", archive: coverage, jobs: []gitLabAllRefArtifactJob{{ID: "21"}, {ID: "22", Oversized: true}},
			want: gitLabCompletenessVerdict{1, 0, []string{"artifact_oversized"}, 3, 2}, wantRuns: [3]uint64{1, 1, 0}},
		// One report member over the compression-ratio guard inside a read artifact.
		{name: "R2_report_member_dropped", archive: silentSkipArchives(t)[githubTestsCompressionRatioCause], jobs: []gitLabAllRefArtifactJob{{ID: "21"}},
			want: gitLabCompletenessVerdict{1, 0, []string{"compression_ratio"}, 2, 2}, wantRuns: [3]uint64{1, 1, 0}},
		// The native test_report over the object cap: no suite is kept, the read artifact's coverage is. The run has no kept
		// suite, so the suite readers do not count it; its row still names it partial.
		{name: "R3_report_too_large", archive: coverage, jobs: []gitLabAllRefArtifactJob{{ID: "21"}}, pad: nativeMaxObjectBytes + 1,
			want: gitLabCompletenessVerdict{1, 0, []string{"artifact_oversized"}, 2, 1}, wantRuns: [3]uint64{0, 0, 0}},
		// The report phase's job listing answers 404: the artifacts are unknown, a known loss.
		{name: "R4_job_listing_not_found", archive: coverage, jobs: []gitLabAllRefArtifactJob{{ID: "21"}}, jobsStatus: http.StatusNotFound,
			want: gitLabCompletenessVerdict{1, 0, []string{"artifact_unavailable"}, 1, 1}, wantRuns: [3]uint64{1, 1, 0}},
		// Every report read: complete.
		{name: "R5_every_report_read", archive: coverage, jobs: []gitLabAllRefArtifactJob{{ID: "21"}},
			want: gitLabCompletenessVerdict{1, 1, []string{}, 2, 2}, wantRuns: [3]uint64{1, 0, 0}},
	}
	for index, cell := range cells {
		t.Run(cell.name, func(t *testing.T) {
			claim := nativeTestClaim("gitlab", "tests")
			claim.OrgID = fmt.Sprintf("org-13981-%d", index)
			pipelines := gitLabAllRefPipelines()[:1]
			pipelines[0].ArtifactJobs, pipelines[0].ReportPad = cell.jobs, cell.pad
			doer := &gitLabJobsOverrideDoer{gitLabAllRefDoer: &gitLabAllRefDoer{t: t, archive: cell.archive, pipelines: pipelines},
				reportJobsStatus: cell.jobsStatus, reportJobs: `{"message":"404 Not found"}`}
			final := stack.walk(t, claim, doer, time.Date(2026, 8, 14, 12, 0, 0, 0, time.UTC))
			got := stack.verdicts(t, claim.OrgID, "9001")
			reportRuns, partialRuns, unmarkedRuns := stack.reader(t, claim.OrgID)
			t.Logf("unit result inventory_truncated=%v; rows=%+v; reader report_runs=%d partial=%d unmarked=%d",
				final.Result["inventory_truncated"], got, reportRuns, partialRuns, unmarkedRuns)
			if len(got) != 1 || got[0].Attempt != cell.want.Attempt || got[0].Complete != cell.want.Complete ||
				!slices.Equal(got[0].Reasons, cell.want.Reasons) || got[0].Expected != cell.want.Expected || got[0].Admitted != cell.want.Admitted {
				t.Fatalf("completeness rows %+v, want [%+v]", got, cell.want)
			}
			if [3]uint64{reportRuns, partialRuns, unmarkedRuns} != cell.wantRuns {
				t.Fatalf("reader report_runs=%d partial=%d unmarked=%d, want %v", reportRuns, partialRuns, unmarkedRuns, cell.wantRuns)
			}
		})
	}
}

// The GitLab run_attempt rule, pinned the way the GitHub re-run rule is: a job retry with new reports is a new key, the
// earlier attempt's verdict stays, and the reader names the pipeline by its newest attempt.
func TestGitLabTestsJobRetryIsANewCompletenessKey(t *testing.T) {
	stack := newGitLabCompletenessStack(t)
	claim := nativeTestClaim("gitlab", "tests")
	claim.OrgID = "org-13981-retry"
	coverage := githubTestsZip(t, map[string]string{"coverage.info": githubTestsLCOVFixture})
	job := func(id, extra string) string {
		return `{"id":` + id + `,"name":"unit",` + extra + `"artifacts_file":{"filename":"reports.zip"},"status":"success",` +
			`"started_at":"2026-07-22T10:01:00Z","finished_at":"2026-07-22T10:05:00Z","runner":{"runner_type":"instance_type"}}`
	}

	// Walk 1: job 21 ran once, every report read.
	first := gitLabAllRefPipelines()[:1]
	first[0].ArtifactJobs = []gitLabAllRefArtifactJob{{ID: "21"}}
	stack.walk(t, claim, &gitLabJobsOverrideDoer{gitLabAllRefDoer: &gitLabAllRefDoer{t: t, archive: coverage, pipelines: first}},
		time.Date(2026, 8, 14, 12, 0, 0, 0, time.UTC))

	// Walk 2: job 21 was retried as job 23, whose artifact is over the download cap.
	second := gitLabAllRefPipelines()[:1]
	second[0].ArtifactJobs = []gitLabAllRefArtifactJob{{ID: "21"}, {ID: "23", Oversized: true}}
	retried := "[" + job("21", `"retried":true,`) + "," + job("23", "") + "]"
	stack.walk(t, claim, &gitLabJobsOverrideDoer{gitLabAllRefDoer: &gitLabAllRefDoer{t: t, archive: coverage, pipelines: second},
		reportJobsStatus: http.StatusOK, reportJobs: retried},
		time.Date(2026, 8, 14, 13, 0, 0, 0, time.UTC))

	got := stack.verdicts(t, claim.OrgID, "9001")
	reportRuns, partialRuns, unmarkedRuns := stack.reader(t, claim.OrgID)
	t.Logf("rows=%+v; reader report_runs=%d partial=%d unmarked=%d", got, reportRuns, partialRuns, unmarkedRuns)
	want := []gitLabCompletenessVerdict{
		{Attempt: 1, Complete: 1, Reasons: []string{}, Expected: 2, Admitted: 2},
		{Attempt: 2, Complete: 0, Reasons: []string{"artifact_oversized"}, Expected: 2, Admitted: 1},
	}
	if len(got) != len(want) {
		t.Fatalf("rows %+v, want %+v", got, want)
	}
	for index := range want {
		if got[index].Attempt != want[index].Attempt || got[index].Complete != want[index].Complete ||
			!slices.Equal(got[index].Reasons, want[index].Reasons) || got[index].Expected != want[index].Expected ||
			got[index].Admitted != want[index].Admitted {
			t.Fatalf("rows %+v, want %+v", got, want)
		}
	}
	if reportRuns != 1 || partialRuns != 1 || unmarkedRuns != 0 {
		t.Fatalf("reader report_runs=%d partial=%d unmarked=%d, want 1 1 0: the newest attempt names the pipeline", reportRuns, partialRuns, unmarkedRuns)
	}
}
