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
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/jobs/metrics/testops"
	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
	"github.com/full-chaos/dev-health-ops/internal/providersync"
	clickhousestore "github.com/full-chaos/dev-health-ops/internal/storage/clickhouse"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/chschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/fakehttp"
)

// CHAOS-13873: a GitHub run whose failed job is re-run uploads a new artifact under the same name. These cells sync such a run in
// two passes through the production chunked route into ClickHouse (the real migration chain) and read the day through the real
// daily readers: totals count the newest attempt of each report slot once; the flake measures see every attempt's statuses.

const attemptsRunID = "43600000001"

type attemptsArtifact struct {
	id      int
	name    string
	created string
	expired bool
	archive []byte
}

// attemptsRunState is what the GitHub API shows for the run at one pass.
type attemptsRunState struct {
	created   string
	attempt   int
	started   string
	updated   string
	artifacts []attemptsArtifact
}

type attemptsDoer struct {
	t     *testing.T
	runID string
	state attemptsRunState
	paths []string
}

func (doer *attemptsDoer) Do(request *http.Request) (*http.Response, error) {
	doer.t.Helper()
	path := request.URL.Path
	doer.paths = append(doer.paths, path)
	status, header, body := http.StatusOK, http.Header{"Content-Type": {"application/json"}}, "{}"
	switch {
	case path == "/repos/acme/api":
		body = `{"id":4567,"name":"api","full_name":"Acme/API","html_url":"https://github.com/Acme/API","default_branch":"main","archived":false}`
	case path == "/repos/acme/api/actions/runs":
		body = `{"workflow_runs":[{"id":` + doer.runID + `,"name":"Go","status":"completed","conclusion":"success",` +
			`"created_at":"` + doer.created() + `","run_started_at":"` + doer.state.started + `","updated_at":"` + doer.state.updated + `",` +
			`"run_attempt":` + strconv.Itoa(doer.state.attempt) + `,"event":"push","head_sha":"abc","head_branch":"main",` +
			`"html_url":"https://github.com/acme/api/actions/runs/` + doer.runID + `","pull_requests":[]}]}`
	case path == "/repos/acme/api/actions/runs/"+doer.runID+"/jobs":
		body = `{"jobs":[{"id":11,"name":"go","status":"completed","conclusion":"success","started_at":"` + doer.state.started +
			`","completed_at":"` + doer.state.updated + `","labels":["ubuntu-latest"]}]}`
	case path == "/repos/acme/api/actions/runs/"+doer.runID+"/artifacts":
		items := make([]string, 0, len(doer.state.artifacts))
		for _, artifact := range doer.state.artifacts {
			items = append(items, fmt.Sprintf(`{"id":%d,"name":%q,"expired":%t,"size_in_bytes":%d,"created_at":%q}`,
				artifact.id, artifact.name, artifact.expired, len(artifact.archive), artifact.created))
		}
		body = `{"total_count":` + strconv.Itoa(len(items)) + `,"artifacts":[` + strings.Join(items, ",") + `]}`
	case strings.HasPrefix(path, "/repos/acme/api/actions/artifacts/") && strings.HasSuffix(path, "/zip"):
		id := strings.TrimSuffix(strings.TrimPrefix(path, "/repos/acme/api/actions/artifacts/"), "/zip")
		status, body = http.StatusFound, ""
		header.Set("Location", "https://blob.example/a"+id+".zip")
	case strings.HasPrefix(path, "/a") && strings.HasSuffix(path, ".zip"):
		id, _ := strconv.Atoi(strings.TrimSuffix(strings.TrimPrefix(path, "/a"), ".zip"))
		found := false
		for _, artifact := range doer.state.artifacts {
			if artifact.id == id {
				header.Set("Content-Type", "application/zip")
				body, found = string(artifact.archive), true
			}
		}
		if !found {
			doer.t.Fatalf("download of an unlisted artifact %d", id)
		}
	default:
		doer.t.Fatalf("unexpected request %s", request.URL.String())
	}
	return &http.Response{StatusCode: status, Header: header, Body: io.NopCloser(strings.NewReader(body)), Request: request}, nil
}

func (doer *attemptsDoer) created() string {
	if doer.state.created != "" {
		return doer.state.created
	}
	return doer.state.started
}

func (doer *attemptsDoer) downloaded(id int) bool {
	for _, path := range doer.paths {
		if path == "/a"+strconv.Itoa(id)+".zip" {
			return true
		}
	}
	return false
}

// attemptsJUnit is one go-junit-report style report: one suite, the cases with their statuses ("passed" or "failed").
func attemptsJUnit(t *testing.T, suite, timestamp string, seed float64, cases map[string]string) []byte {
	t.Helper()
	names := make([]string, 0, len(cases))
	for name := range cases {
		names = append(names, name)
	}
	sort.Strings(names)
	var b strings.Builder
	b.WriteString(`<testsuites><testsuite name="` + suite + `" tests="` + strconv.Itoa(len(cases)) + `" time="2.5" timestamp="` + timestamp + `">`)
	for index, name := range names {
		duration := strconv.FormatFloat(seed+float64(index)/100, 'f', 3, 64)
		b.WriteString(`<testcase classname="` + suite + `" name="` + name + `" time="` + duration + `">`)
		if cases[name] == "failed" {
			b.WriteString(`<failure message="Failed" type="">boom</failure>`)
		}
		b.WriteString(`</testcase>`)
	}
	b.WriteString(`</testsuite></testsuites>`)
	var buffer bytes.Buffer
	writer := zip.NewWriter(&buffer)
	member, err := writer.Create("report.xml")
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

type attemptsStack struct {
	ctx  context.Context
	conn driver.Conn
}

// pass runs one tests unit over [since, since+1h) for the run state and writes every emitted effect through the production sink.
func (stack attemptsStack) pass(t *testing.T, org, runID string, since time.Time, state attemptsRunState) *attemptsDoer {
	t.Helper()
	before := since.Add(time.Hour)
	capability, _ := providersync.Capability("github", "tests")
	claim := providersync.Claim{
		Unit: providersync.Unit{
			ID: uuid.NewString(), SyncRunID: uuid.NewString(), OrgID: org,
			IntegrationID: uuid.NewString(), SourceID: uuid.NewString(),
			SourceExternalID: "acme/api", SourceName: "acme/api",
			Provider: "github", Dataset: "tests", CostClass: capability.CostClass,
			Mode: "incremental", SinceAt: &since, BeforeAt: &before,
			CredentialID: uuid.NewString(), AuthSource: "integration_credential",
		},
		Owner: uuid.NewString(), Attempt: 1, LeaseExpiresAt: before.Add(time.Hour),
	}
	doer := &attemptsDoer{t: t, runID: runID, state: state}
	lease := providerfoundation.LeaseGuardFunc(func(context.Context) error { return nil })
	client, err := providerfoundation.NewHTTPClient("github", "https://api.github.com", fakehttp.Client(doer),
		func(request *http.Request) error { request.Header.Set("Authorization", "Bearer secret"); return nil },
		providerfoundation.RetryPolicy{MaxAttempts: 1, InitialWait: time.Nanosecond, MaxWait: time.Nanosecond}, lease)
	if err != nil {
		t.Fatal(err)
	}
	sink := providersync.TestOpsClickHouseEffects{Conn: stack.conn, Lease: lease}
	err = providersync.GitHubTestsRouteHandler{}.CollectChunks(stack.ctx, claim, providerfoundation.Credential{}, client, before.Add(5*time.Minute), "",
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

func (stack attemptsStack) repoID(t *testing.T, org string) uuid.UUID {
	t.Helper()
	var repo string
	if err := stack.conn.QueryRow(stack.ctx, "SELECT toString(any(repo_id)) FROM test_suite_results WHERE org_id = ?", org).Scan(&repo); err != nil {
		t.Fatal(err)
	}
	return uuid.MustParse(repo)
}

func (stack attemptsStack) metric(t *testing.T, org string, repoID uuid.UUID, day time.Time) testops.TestMetric {
	t.Helper()
	metrics, err := computeTestopsTestMetricsWithoutTeams(stack.ctx, stack.conn, org, repoID, day.AddDate(0, 0, -29), day, day.Add(24*time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	if len(metrics) == 0 {
		return testops.TestMetric{}
	}
	return metrics[0]
}

type attemptsWant struct {
	total, passed, failed, flake, distinct int
	retryDependency, failureRecurrence     float64
}

func attemptsGot(m testops.TestMetric) attemptsWant {
	return attemptsWant{total: m.TotalCases, passed: m.PassedCount, failed: m.FailedCount, flake: m.FlakeCases, distinct: m.DistinctCases,
		retryDependency: m.RetryDependencyRate, failureRecurrence: m.FailureRecurrence}
}

func TestTestopsReportAttemptsCountTheNewestAttemptAndKeepTheFlake(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
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
	attempt1 := func(artifacts ...attemptsArtifact) attemptsRunState {
		return attemptsRunState{attempt: 1, started: "2026-07-22T10:01:00Z", updated: "2026-07-22T10:30:00Z", artifacts: artifacts}
	}
	attemptN := func(n int, artifacts ...attemptsArtifact) attemptsRunState {
		return attemptsRunState{created: "2026-07-22T10:01:00Z", attempt: n, started: "2026-07-22T11:10:00Z", updated: "2026-07-22T11:40:00Z", artifacts: artifacts}
	}
	const made1, madeN = "2026-07-22T10:20:00Z", "2026-07-22T11:30:00Z"
	ts1, tsN := "2026-07-22T10:02:00", "2026-07-22T11:12:00"
	flakyFail := attemptsJUnit(t, "pkg/b", ts1, 0.2, map[string]string{"TestX": "failed"})
	flakyPass := attemptsJUnit(t, "pkg/b", tsN, 0.25, map[string]string{"TestX": "passed"})

	t.Run("X1_fail_then_pass_counts_once_and_stays_flaky", func(t *testing.T) {
		org := "org-x1-" + uuid.NewString()[:8]
		stack.pass(t, org, attemptsRunID, w1, attempt1(attemptsArtifact{102, "go-test-b", made1, false, flakyFail}))
		stack.pass(t, org, attemptsRunID, w2, attemptN(2, attemptsArtifact{203, "go-test-b", madeN, false, flakyPass}))
		got := attemptsGot(stack.metric(t, org, stack.repoID(t, org), day))
		t.Logf("X1 %+v", got)
		if want := (attemptsWant{total: 1, passed: 1, failed: 0, flake: 1, distinct: 1}); got != want {
			t.Fatalf("X1 got %+v want %+v", got, want)
		}
	})

	t.Run("X1_cross_day_counts_once_over_both_days", func(t *testing.T) {
		org := "org-x1d-" + uuid.NewString()[:8]
		next := day.Add(24 * time.Hour)
		stack.pass(t, org, attemptsRunID, w1, attempt1(attemptsArtifact{102, "go-test-b", made1, false, flakyFail}))
		stack.pass(t, org, attemptsRunID, next, attemptsRunState{created: "2026-07-22T10:01:00Z", attempt: 2, started: "2026-07-23T00:10:00Z", updated: "2026-07-23T00:40:00Z",
			artifacts: []attemptsArtifact{{203, "go-test-b", "2026-07-23T00:30:00Z", false,
				attemptsJUnit(t, "pkg/b", "2026-07-23T00:12:00", 0.25, map[string]string{"TestX": "passed"})}}})
		repo := stack.repoID(t, org)
		first, second := attemptsGot(stack.metric(t, org, repo, day)), attemptsGot(stack.metric(t, org, repo, next))
		t.Logf("X1-cross day D %+v, day D+1 %+v", first, second)
		if first.total+second.total != 1 || second.passed != 1 || second.flake != 1 {
			t.Fatalf("X1-cross: D %+v, D+1 %+v; want one counted case over both days, passed and flaky on D+1", first, second)
		}
	})

	t.Run("X2_pass_then_pass_is_not_flaky", func(t *testing.T) {
		org := "org-x2-" + uuid.NewString()[:8]
		stack.pass(t, org, attemptsRunID, w1, attempt1(attemptsArtifact{102, "go-test-b", made1, false,
			attemptsJUnit(t, "pkg/b", ts1, 0.2, map[string]string{"TestX": "passed"})}))
		stack.pass(t, org, attemptsRunID, w2, attemptN(2, attemptsArtifact{203, "go-test-b", madeN, false, flakyPass}))
		got := attemptsGot(stack.metric(t, org, stack.repoID(t, org), day))
		t.Logf("X2 %+v", got)
		if want := (attemptsWant{total: 1, passed: 1, flake: 0, distinct: 1}); got != want {
			t.Fatalf("X2 got %+v want %+v", got, want)
		}
	})

	t.Run("X3_rerun_flake_leaves_the_other_cases_not_retry_dependent", func(t *testing.T) {
		org := "org-x3-" + uuid.NewString()[:8]
		cases1 := map[string]string{"TestA": "passed", "TestB": "passed", "TestC": "passed", "TestD": "passed", "TestX": "failed"}
		casesN := map[string]string{"TestA": "passed", "TestB": "passed", "TestC": "passed", "TestD": "passed", "TestX": "passed"}
		stack.pass(t, org, attemptsRunID, w1, attempt1(attemptsArtifact{102, "go-test-b", made1, false, attemptsJUnit(t, "pkg/b", ts1, 0.2, cases1)}))
		stack.pass(t, org, attemptsRunID, w2, attemptN(2, attemptsArtifact{203, "go-test-b", madeN, false, attemptsJUnit(t, "pkg/b", tsN, 0.25, casesN)}))
		got := attemptsGot(stack.metric(t, org, stack.repoID(t, org), day))
		t.Logf("X3 %+v", got)
		if want := (attemptsWant{total: 5, passed: 5, flake: 1, distinct: 5, retryDependency: 0}); got != want {
			t.Fatalf("X3 got %+v want %+v", got, want)
		}
	})

	t.Run("C1b_a_shard_listed_twice_counts_once", func(t *testing.T) {
		org := "org-c1b-" + uuid.NewString()[:8]
		stack.pass(t, org, attemptsRunID, w1, attempt1(attemptsArtifact{102, "go-test-b", made1, false, flakyFail}))
		doer := stack.pass(t, org, attemptsRunID, w2, attemptN(2,
			attemptsArtifact{102, "go-test-b", made1, false, flakyFail}, attemptsArtifact{203, "go-test-b", madeN, false, flakyPass}))
		got := attemptsGot(stack.metric(t, org, stack.repoID(t, org), day))
		t.Logf("C1b %+v downloaded102=%v downloaded203=%v", got, doer.downloaded(102), doer.downloaded(203))
		if want := (attemptsWant{total: 1, passed: 1, flake: 1, distinct: 1}); got != want || doer.downloaded(102) {
			t.Fatalf("C1b got %+v (downloaded the older copy: %v) want %+v", got, doer.downloaded(102), want)
		}
	})

	shardA := attemptsJUnit(t, "pkg/a", ts1, 0.1, map[string]string{"TestA1": "passed", "TestA2": "passed"})

	t.Run("C1_rerun_of_one_shard_counts_each_shard_once", func(t *testing.T) {
		org := "org-c1-" + uuid.NewString()[:8]
		stack.pass(t, org, attemptsRunID, w1, attempt1(attemptsArtifact{101, "go-test-a", made1, false, shardA}, attemptsArtifact{102, "go-test-b", made1, false, flakyFail}))
		stack.pass(t, org, attemptsRunID, w2, attemptN(2, attemptsArtifact{101, "go-test-a", made1, false, shardA}, attemptsArtifact{203, "go-test-b", madeN, false, flakyPass}))
		got := attemptsGot(stack.metric(t, org, stack.repoID(t, org), day))
		t.Logf("C1 %+v", got)
		if want := (attemptsWant{total: 3, passed: 3, flake: 1, distinct: 3}); got != want {
			t.Fatalf("C1 got %+v want %+v", got, want)
		}
	})

	t.Run("C3_attempt3_carry_over_counts_once", func(t *testing.T) {
		org := "org-c3-" + uuid.NewString()[:8]
		stack.pass(t, org, attemptsRunID, w1, attempt1(attemptsArtifact{101, "go-test-a", made1, false, shardA}, attemptsArtifact{102, "go-test-b", made1, false, flakyFail}))
		stack.pass(t, org, attemptsRunID, w2, attemptN(3, attemptsArtifact{101, "go-test-a", made1, false, shardA}, attemptsArtifact{205, "go-test-b", madeN, false, flakyPass}))
		got := attemptsGot(stack.metric(t, org, stack.repoID(t, org), day))
		var labels string
		if err := conn.QueryRow(ctx, "SELECT toString(arraySort(groupArray(concat(toString(report_slot), '@', toString(run_attempt))))) FROM test_suite_results FINAL WHERE org_id = ?", org).Scan(&labels); err != nil {
			t.Logf("C3 labels unreadable (a build without 151): %v", err)
		}
		t.Logf("C3 %+v suite labels %s", got, labels)
		if want := (attemptsWant{total: 3, passed: 3, flake: 1, distinct: 3}); got != want {
			t.Fatalf("C3 got %+v want %+v", got, want)
		}
		if labels != "['go-test-a@1','go-test-a@2','go-test-b@1','go-test-b@3']" {
			t.Fatalf("C3 suite labels %s, want the carried-over shard at 1 and 2 and the re-run shard at 1 and 3", labels)
		}
	})

	t.Run("C4_a_fail_then_pass_case_stays_a_recurrent_failure", func(t *testing.T) {
		org := "org-c4-" + uuid.NewString()[:8]
		stack.pass(t, org, "43600000009", day.Add(-72*time.Hour+10*time.Hour), attemptsRunState{attempt: 1, started: "2026-07-19T10:01:00Z", updated: "2026-07-19T10:30:00Z",
			artifacts: []attemptsArtifact{{90, "go-test-b", "2026-07-19T10:20:00Z", false, attemptsJUnit(t, "pkg/b", "2026-07-19T10:02:00", 0.2, map[string]string{"TestX": "failed"})}}})
		stack.pass(t, org, attemptsRunID, w1, attempt1(attemptsArtifact{102, "go-test-b", made1, false, flakyFail}))
		stack.pass(t, org, attemptsRunID, w2, attemptN(2, attemptsArtifact{203, "go-test-b", madeN, false, flakyPass}))
		got := attemptsGot(stack.metric(t, org, stack.repoID(t, org), day))
		t.Logf("C4 %+v", got)
		if got.failureRecurrence != 1 || got.flake != 1 {
			t.Fatalf("C4 got %+v, want failure_recurrence 1 (TestX failed today on attempt 1 and before) and flake 1", got)
		}
	})

	shardC := attemptsJUnit(t, "pkg/c", ts1, 0.3, map[string]string{"TestC1": "passed", "TestC2": "passed", "TestC3": "passed"})

	t.Run("C2_guard_an_artifact_expired_in_the_newest_pass_still_counts", func(t *testing.T) {
		org := "org-c2-" + uuid.NewString()[:8]
		stack.pass(t, org, attemptsRunID, w1, attempt1(attemptsArtifact{101, "go-test-a", made1, false, shardA}, attemptsArtifact{104, "go-test-c", made1, false, shardC}))
		stack.pass(t, org, attemptsRunID, w2, attemptsRunState{attempt: 1, started: "2026-07-22T10:01:00Z", updated: "2026-07-22T11:40:00Z",
			artifacts: []attemptsArtifact{{101, "go-test-a", made1, false, shardA}, {104, "go-test-c", made1, true, shardC}}})
		got := attemptsGot(stack.metric(t, org, stack.repoID(t, org), day))
		t.Logf("C2 %+v", got)
		if want := (attemptsWant{total: 5, passed: 5, distinct: 5}); got != want {
			t.Fatalf("C2 got %+v want %+v", got, want)
		}
	})

	t.Run("C0_guard_the_same_artifacts_twice_count_once", func(t *testing.T) {
		org := "org-c0-" + uuid.NewString()[:8]
		state := attempt1(attemptsArtifact{101, "go-test-a", made1, false, shardA}, attemptsArtifact{104, "go-test-c", made1, false, shardC})
		stack.pass(t, org, attemptsRunID, w1, state)
		stack.pass(t, org, attemptsRunID, w2, state)
		got := attemptsGot(stack.metric(t, org, stack.repoID(t, org), day))
		t.Logf("C0 %+v", got)
		if want := (attemptsWant{total: 5, passed: 5, distinct: 5}); got != want {
			t.Fatalf("C0 got %+v want %+v", got, want)
		}
	})
}
