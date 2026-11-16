//go:build integration

package daily

import (
	"archive/zip"
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
	"github.com/full-chaos/dev-health-ops/internal/providersync"
	clickhousestore "github.com/full-chaos/dev-health-ops/internal/storage/clickhouse"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/chschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/fakehttp"
)

// CHAOS-13875: a GitLab job retried inside one pipeline, synced in two passes through the production chunked route into
// ClickHouse (the real migration chain) and read through the real daily readers. GitLab's pipeline test_report shows the latest
// job of each name only (app/models/ci/pipeline.rb builds_in_self_and_project_descendants: Ci::Build.latest).
//
// Field shapes of the fake come from GitLab's public source, not from a live payload (RISK-NOTES: "build_ids mapping is
// GitLab-source-argued, no live payload"):
//   - GET /pipelines/:id/test_report carries test_suites[].name, total_time, test_cases[] and NO build_ids
//     (app/serializers/test_suite_entity.rb, test_report_entity.rb).
//   - GET /pipelines/:id/test_report_summary carries test_suites[].name and build_ids[]
//     (app/serializers/test_suite_summary_entity.rb; doc/api/pipelines.md sample: "build_ids": [ 66004 ]).
//   - the suite name is the job's group name for parallel jobs ("rspec 1/2" -> "rspec") and the full name for matrix jobs
//     (app/models/ci/build.rb test_suite_name).
//     Source: https://gitlab.com/gitlab-org/gitlab/-/raw/master/<path above>, read 2026-11-15.

type glJob struct {
	id          int
	name        string
	retried     bool
	allowFailed bool
	artifact    []byte
}

type glState struct {
	updated string
	report  string
	summary string
	// summaryStatus 0 = 200; any other value is returned as the summary status.
	summaryStatus int
	jobs          []glJob
}

type glDoer struct {
	t            *testing.T
	state        glState
	downloaded   []int
	summaryCalls int
}

func (doer *glDoer) Do(request *http.Request) (*http.Response, error) {
	path := request.URL.Path
	status, header, body := http.StatusOK, http.Header{"Content-Type": {"application/json"}}, "{}"
	const root = "/api/v4/projects/123"
	switch {
	case path == root:
		body = `{"id":123,"path_with_namespace":"acme/api","default_branch":"main"}`
	case path == root+"/pipelines":
		body = `[{"id":9001,"name":"CI","ref":"main","status":"success","created_at":"2026-07-22T10:00:00Z","started_at":"2026-07-22T10:01:00Z","finished_at":"` +
			doer.state.updated + `","updated_at":"` + doer.state.updated + `","source":"push","sha":"abc","web_url":"https://gitlab.example/acme/api/-/pipelines/9001"}]`
	case path == root+"/pipelines/9001/jobs":
		withRetried := request.URL.Query().Get("include_retried") == "true"
		items := []string{}
		for _, job := range doer.state.jobs {
			if job.retried && !withRetried {
				continue
			}
			jobStatus := "success"
			if job.allowFailed {
				jobStatus = "failed"
			}
			items = append(items, fmt.Sprintf(`{"id":%d,"name":%q,"stage":"test","status":%q,"allow_failure":%t,"started_at":"2026-07-22T10:01:00Z","finished_at":%q,"retried":%t,"artifacts_file":{"filename":"reports.zip"}}`,
				job.id, job.name, jobStatus, job.allowFailed, doer.state.updated, job.retried))
		}
		body = "[" + strings.Join(items, ",") + "]"
	case path == root+"/pipelines/9001/test_report":
		body = doer.state.report
	case path == root+"/pipelines/9001/test_report_summary":
		doer.summaryCalls++
		if doer.state.summaryStatus != 0 {
			status, body = doer.state.summaryStatus, `{"message":"unavailable"}`
		} else {
			body = doer.state.summary
		}
	case strings.HasPrefix(path, root+"/jobs/") && strings.HasSuffix(path, "/artifacts"):
		var id int
		fmt.Sscanf(strings.TrimSuffix(strings.TrimPrefix(path, root+"/jobs/"), "/artifacts"), "%d", &id)
		found := false
		for _, job := range doer.state.jobs {
			if job.id == id {
				body, found = string(job.artifact), true
				header.Set("Content-Type", "application/zip")
			}
		}
		if !found {
			doer.t.Fatalf("artifact of an unlisted job %d", id)
		}
		doer.downloaded = append(doer.downloaded, id)
	default:
		doer.t.Fatalf("unexpected request %s", request.URL.String())
	}
	return &http.Response{StatusCode: status, Header: header, Body: io.NopCloser(strings.NewReader(body)), Request: request}, nil
}

func glLcov(t *testing.T, covered, total int) []byte {
	t.Helper()
	var b strings.Builder
	b.WriteString("SF:a.go\n")
	for line := 1; line <= total; line++ {
		hit := 0
		if line <= covered {
			hit = 1
		}
		fmt.Fprintf(&b, "DA:%d,%d\n", line, hit)
	}
	b.WriteString("end_of_record\n")
	var buffer bytes.Buffer
	writer := zip.NewWriter(&buffer)
	member, err := writer.Create("coverage.info")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := member.Write([]byte(b.String())); err != nil {
		t.Fatal(err)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	return buffer.Bytes()
}

func glReport(suite string, cases map[string]string) string {
	names := make([]string, 0, len(cases))
	for name := range cases {
		names = append(names, name)
	}
	sort.Strings(names)
	items := []string{}
	for _, name := range names {
		items = append(items, fmt.Sprintf(`{"name":%q,"classname":"tests.T","status":%q,"execution_time":"0.5"}`, name, cases[name]))
	}
	return `{"test_suites":[{"name":` + fmt.Sprintf("%q", suite) + `,"total_time":"1.0","test_cases":[` + strings.Join(items, ",") + `]}]}`
}

func glSummary(suite string, buildIDs ...int) string {
	ids := make([]string, 0, len(buildIDs))
	for _, id := range buildIDs {
		ids = append(ids, fmt.Sprint(id))
	}
	return `{"total":{"time":1,"count":1,"success":1,"failed":0,"skipped":0,"error":0,"suite_error":null},"test_suites":[{"name":` +
		fmt.Sprintf("%q", suite) + `,"total_time":1,"total_count":1,"build_ids":[` + strings.Join(ids, ",") + `],"suite_error":null}]}`
}

func (stack attemptsStack) glPass(t *testing.T, org string, since time.Time, state glState) *glDoer {
	t.Helper()
	before := since.Add(time.Hour)
	capability, _ := providersync.Capability("gitlab", "tests")
	claim := providersync.Claim{
		Unit: providersync.Unit{
			ID: uuid.NewString(), SyncRunID: uuid.NewString(), OrgID: org,
			IntegrationID: uuid.NewString(), SourceID: uuid.NewString(),
			SourceExternalID: "123", SourceName: "acme/api",
			Provider: "gitlab", Dataset: "tests", CostClass: capability.CostClass,
			Mode: "incremental", SinceAt: &since, BeforeAt: &before,
			CredentialID: uuid.NewString(), AuthSource: "integration_credential",
		},
		Owner: uuid.NewString(), Attempt: 1, LeaseExpiresAt: before.Add(time.Hour),
	}
	doer := &glDoer{t: t, state: state}
	lease := providerfoundation.LeaseGuardFunc(func(context.Context) error { return nil })
	client, err := providerfoundation.NewHTTPClient("gitlab", "https://gitlab.example", fakehttp.Client(doer),
		func(request *http.Request) error { request.Header.Set("PRIVATE-TOKEN", "secret"); return nil },
		providerfoundation.RetryPolicy{MaxAttempts: 1, InitialWait: time.Nanosecond, MaxWait: time.Nanosecond}, lease)
	if err != nil {
		t.Fatal(err)
	}
	sink := providersync.TestOpsClickHouseEffects{Conn: stack.conn, Lease: lease}
	err = providersync.GitLabTestsRouteHandler{}.CollectChunks(stack.ctx, claim, providerfoundation.Credential{}, client, before.Add(5*time.Minute), "",
		func(emission providersync.ChunkRouteEmission) error {
			for _, effect := range emission.Batch.Effects {
				if len(effect.Rows) == 0 {
					continue
				}
				if err := sink.WriteEffect(stack.ctx, claim, effect); err != nil {
					return fmt.Errorf("write %s: %w", effect.Destination, err)
				}
			}
			return nil
		})
	if err != nil {
		t.Fatalf("CollectChunks: %v", err)
	}
	return doer
}

func (stack attemptsStack) glCount(t *testing.T, org, table string) uint64 {
	t.Helper()
	var n uint64
	if err := stack.conn.QueryRow(stack.ctx, "SELECT count() FROM "+table+" FINAL WHERE org_id = ?", org).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

// glLabels is the sorted slot@attempt labels of a table; an engine without the columns reports why instead.
func (stack attemptsStack) glLabels(t *testing.T, org, table string) string {
	t.Helper()
	var labels string
	if err := stack.conn.QueryRow(stack.ctx, "SELECT toString(arraySort(groupArray(concat(toString(report_slot), '@', toString(run_attempt))))) FROM "+table+" FINAL WHERE org_id = ?", org).Scan(&labels); err != nil {
		return "unreadable: " + err.Error()
	}
	return labels
}

func (stack attemptsStack) glCoverageLines(t *testing.T, org string, day time.Time) string {
	t.Helper()
	rows, err := loadNativeTestopsLatestCoverage(stack.ctx, stack.conn, org, stack.repoID(t, org), day, day.Add(24*time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 || rows[0].LinesCovered == nil {
		return fmt.Sprintf("rows=%d", len(rows))
	}
	return fmt.Sprint(*rows[0].LinesCovered)
}

func TestTestopsGitLabReportAttemptsCountTheNewestAttemptAndKeepTheFlake(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	instance, err := containers.StartClickHouse(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer instance.Close(context.Background())
	conn, err := clickhousestore.Open(ctx, clickhousestore.DefaultConfig(instance.URI))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	chschema.Apply(ctx, t, instance)
	stack := attemptsStack{ctx: ctx, conn: conn}
	day := time.Date(2026, 7, 22, 0, 0, 0, 0, time.UTC)
	w1, w2 := day.Add(10*time.Hour), day.Add(11*time.Hour)
	const u1, u2 = "2026-07-22T10:30:00Z", "2026-07-22T11:40:00Z"
	failing, passing := glLcov(t, 1, 2), glLcov(t, 2, 2)

	// retry runs the plain shape: job `name` fails in pass 1 and is retried (new id) in pass 2.
	retry := func(t *testing.T, key, name string, first, second map[string]string) (string, *glDoer, *glDoer) {
		org := "org-" + key + "-" + uuid.NewString()[:8]
		one := stack.glPass(t, org, w1, glState{updated: u1, report: glReport(name, first), jobs: []glJob{{id: 11, name: name, artifact: failing}}})
		two := stack.glPass(t, org, w2, glState{updated: u2, report: glReport(name, second), summary: glSummary(name, 12),
			jobs: []glJob{{id: 11, name: name, retried: true, artifact: failing}, {id: 12, name: name, artifact: passing}}})
		return org, one, two
	}
	want := func(t *testing.T, org string, w attemptsWant) {
		t.Helper()
		got := attemptsGot(stack.metric(t, org, stack.repoID(t, org), day))
		t.Logf("%+v", got)
		if got != w {
			t.Fatalf("got %+v want %+v", got, w)
		}
	}

	t.Run("RX1_fail_then_pass_counts_once_and_stays_flaky", func(t *testing.T) {
		org, _, _ := retry(t, "rx1", "unit", map[string]string{"TestX": "failed"}, map[string]string{"TestX": "success"})
		want(t, org, attemptsWant{total: 1, passed: 1, failed: 0, flake: 1, distinct: 1})
	})

	t.Run("RX9_coverage_reads_the_newest_attempt_and_the_retried_job_is_not_downloaded_again", func(t *testing.T) {
		org, one, two := retry(t, "rx9", "unit", map[string]string{"TestX": "failed"}, map[string]string{"TestX": "success"})
		lines, labels := stack.glCoverageLines(t, org, day), stack.glLabels(t, org, "coverage_snapshots")
		t.Logf("RX9 coverage reader lines=%s labels=%s suites=%s downloads pass2=%v summary calls pass1=%d pass2=%d",
			lines, labels, stack.glLabels(t, org, "test_suite_results"), two.downloaded, one.summaryCalls, two.summaryCalls)
		if lines != "2" || labels != "['unit@1','unit@2']" {
			t.Fatalf("RX9: newest attempt's coverage (2 lines) must be the one read, labels ['unit@1','unit@2']; got lines=%s labels=%s", lines, labels)
		}
		if got := stack.glLabels(t, org, "test_suite_results"); got != "['unit@1','unit@2']" {
			t.Fatalf("suite labels %s", got)
		}
		if len(two.downloaded) != 1 || two.downloaded[0] != 12 {
			t.Fatalf("RX4: the retried job must not be downloaded again, got %v", two.downloaded)
		}
		if one.summaryCalls != 0 || two.summaryCalls != 1 {
			t.Fatalf("B3: summary GETs without a retry=%d (want 0), with one=%d (want exactly 1)", one.summaryCalls, two.summaryCalls)
		}
	})

	t.Run("RX2_pass_then_pass_is_not_flaky", func(t *testing.T) {
		org, _, _ := retry(t, "rx2", "unit", map[string]string{"TestX": "success"}, map[string]string{"TestX": "success"})
		want(t, org, attemptsWant{total: 1, passed: 1, flake: 0, distinct: 1})
	})

	t.Run("RX3_a_retry_that_drops_a_case_keeps_the_orphan_out_of_the_totals", func(t *testing.T) {
		org, _, _ := retry(t, "rx3", "unit", map[string]string{"TestX": "failed", "TestY": "failed"}, map[string]string{"TestX": "success"})
		want(t, org, attemptsWant{total: 1, passed: 1, failed: 0, flake: 1, distinct: 2})
	})

	t.Run("RX8_matrix_name_is_the_suite_name", func(t *testing.T) {
		org, _, _ := retry(t, "rx8", "test: [ruby 3.2]", map[string]string{"TestX": "failed"}, map[string]string{"TestX": "success"})
		want(t, org, attemptsWant{total: 1, passed: 1, flake: 1, distinct: 1})
		if got := stack.glLabels(t, org, "test_suite_results"); got != "['test: [ruby 3.2]@1','test: [ruby 3.2]@2']" {
			t.Fatalf("suite labels %s", got)
		}
	})

	t.Run("RX7_parallel_jobs_one_suite_retry_of_index_2", func(t *testing.T) {
		org := "org-rx7-" + uuid.NewString()[:8]
		stack.glPass(t, org, w1, glState{updated: u1, report: glReport("rspec", map[string]string{"A": "success", "B": "failed"}),
			jobs: []glJob{{id: 10, name: "rspec 1/2", artifact: passing}, {id: 11, name: "rspec 2/2", artifact: failing}}})
		two := stack.glPass(t, org, w2, glState{updated: u2, report: glReport("rspec", map[string]string{"A": "success", "B": "success"}),
			summary: glSummary("rspec", 10, 12),
			jobs:    []glJob{{id: 10, name: "rspec 1/2", artifact: passing}, {id: 11, name: "rspec 2/2", retried: true, artifact: failing}, {id: 12, name: "rspec 2/2", artifact: passing}}})
		want(t, org, attemptsWant{total: 2, passed: 2, failed: 0, flake: 1, distinct: 2})
		labels := stack.glLabels(t, org, "test_suite_results")
		t.Logf("RX7 suites=%s coverage=%s summary calls=%d", labels, stack.glLabels(t, org, "coverage_snapshots"), two.summaryCalls)
		if labels != "['rspec@1','rspec@2']" {
			t.Fatalf("RX7 suite labels %s, want the parallel suite at attempts 1 and 2", labels)
		}
	})

	t.Run("RX10_summary_failure_falls_back_to_the_name_and_never_refuses_the_run", func(t *testing.T) {
		org := "org-rx10-" + uuid.NewString()[:8]
		stack.glPass(t, org, w1, glState{updated: u1, report: glReport("unit", map[string]string{"TestX": "failed"}), jobs: []glJob{{id: 11, name: "unit", artifact: failing}}})
		stack.glPass(t, org, w2, glState{updated: u2, report: glReport("unit", map[string]string{"TestX": "success"}), summaryStatus: http.StatusServiceUnavailable,
			jobs: []glJob{{id: 11, name: "unit", retried: true, artifact: failing}, {id: 12, name: "unit", artifact: passing}}})
		want(t, org, attemptsWant{total: 1, passed: 1, flake: 1, distinct: 1})
		par := "org-rx10p-" + uuid.NewString()[:8]
		stack.glPass(t, par, w1, glState{updated: u1, report: glReport("rspec", map[string]string{"B": "failed"}), jobs: []glJob{{id: 11, name: "rspec 2/2", artifact: failing}}})
		stack.glPass(t, par, w2, glState{updated: u2, report: glReport("rspec", map[string]string{"B": "success"}), summaryStatus: http.StatusNotFound,
			jobs: []glJob{{id: 11, name: "rspec 2/2", retried: true, artifact: failing}, {id: 12, name: "rspec 2/2", artifact: passing}}})
		if got := stack.glLabels(t, par, "test_suite_results"); got != "['rspec@1']" {
			t.Fatalf("a parallel suite without the summary stays attempt 1 (named NOT fixed), got %s", got)
		}
	})

	t.Run("C0_no_retry_costs_no_summary_request_and_a_re_read_keeps_one_row", func(t *testing.T) {
		org := "org-c0-" + uuid.NewString()[:8]
		state := glState{updated: u1, report: glReport("unit", map[string]string{"TestX": "success"}), jobs: []glJob{{id: 11, name: "unit", artifact: passing}}}
		a := stack.glPass(t, org, w1, state)
		b := stack.glPass(t, org, w2, state)
		want(t, org, attemptsWant{total: 1, passed: 1, distinct: 1})
		if a.summaryCalls+b.summaryCalls != 0 || stack.glCount(t, org, "test_suite_results") != 1 || stack.glCount(t, org, "coverage_snapshots") != 1 {
			t.Fatalf("summary calls %d, suite rows %d, coverage rows %d", a.summaryCalls+b.summaryCalls, stack.glCount(t, org, "test_suite_results"), stack.glCount(t, org, "coverage_snapshots"))
		}
	})

	t.Run("B2a_a_re_sync_of_a_run_with_legacy_rows_leaves_no_duplicate", func(t *testing.T) {
		org := "org-b2a-" + uuid.NewString()[:8]
		state := glState{updated: u1, report: glReport("unit", map[string]string{"TestX": "success"}), jobs: []glJob{{id: 11, name: "unit", artifact: passing}}}
		stack.glPass(t, org, w1, state)
		for _, table := range []string{"test_suite_results", "coverage_snapshots"} {
			if err := conn.Exec(ctx, "ALTER TABLE "+table+" UPDATE report_slot = '', run_attempt = 0 WHERE org_id = '"+org+"' SETTINGS mutations_sync = 2"); err != nil {
				t.Fatal(err)
			}
		}
		if got := stack.glLabels(t, org, "test_suite_results"); got != "['@0']" {
			t.Fatalf("the rows were not made legacy: %s", got)
		}
		stack.glPass(t, org, w2, state)
		suites, coverage := stack.glCount(t, org, "test_suite_results"), stack.glCount(t, org, "coverage_snapshots")
		labels := stack.glLabels(t, org, "test_suite_results") + stack.glLabels(t, org, "coverage_snapshots")
		if suites != 1 || coverage != 1 || labels != "['unit@1']['unit@1']" {
			t.Fatalf("suites=%d coverage=%d labels=%s; want 1, 1, both rewritten in place as unit@1", suites, coverage, labels)
		}
		want(t, org, attemptsWant{total: 1, passed: 1, distinct: 1})
	})

	t.Run("G_allowed_failure_job_is_counted_as_today", func(t *testing.T) {
		org := "org-gaf-" + uuid.NewString()[:8]
		stack.glPass(t, org, w1, glState{updated: u1, report: glReport("lint", map[string]string{"TestL": "failed"}),
			jobs: []glJob{{id: 11, name: "lint", allowFailed: true, artifact: failing}}})
		want(t, org, attemptsWant{total: 1, passed: 0, failed: 1, distinct: 1})
	})

}
