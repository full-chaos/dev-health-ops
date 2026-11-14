package providersync

import (
	"context"
	"io"
	"net/http"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/fakehttp"
)

func completedRunsWindow(t *testing.T) (Claim, time.Time) {
	t.Helper()
	return githubTestsWindowClaim(t,
		time.Date(2026, 7, 22, 12, 0, 0, 0, time.UTC),
		time.Date(2026, 7, 22, 13, 0, 0, 0, time.UTC)), time.Date(2026, 7, 22, 13, 5, 0, 0, time.UTC)
}

// A run that has not completed is not read: no artifact listing, no download, no report rows.
func TestGitHubTestsReportPhaseSkipsARunThatIsNotCompleted(t *testing.T) {
	t.Parallel()
	for _, status := range []string{"in_progress", "queued", "waiting", "requested", "pending"} {
		doer := githubTestsReportWindowDoerFor(t, "2026-07-22T12:30:00Z")
		doer.status = status
		claim, at := completedRunsWindow(t)
		suites, cases := collectGitHubTestsReportRows(t, doer, claim, at)
		if doer.fetchedArtifacts() || suites != 0 || cases != 0 {
			t.Fatalf("%s run: artifacts fetched=%v suites=%d cases=%d, want none; requests %v", status, doer.fetchedArtifacts(), suites, cases, doer.requests)
		}
		for _, path := range doer.requests {
			if strings.Contains(path, "/zip") {
				t.Fatalf("%s run: downloaded %s", status, path)
			}
		}
	}
}

// The same run seen in progress and then completed writes its report rows only on the completed pass.
func TestGitHubTestsReportPhaseReadsTheRunOnceItIsCompleted(t *testing.T) {
	t.Parallel()
	claim, at := completedRunsWindow(t)
	first := githubTestsReportWindowDoerFor(t, "2026-07-22T12:20:00Z")
	first.status = "in_progress"
	if _, cases := collectGitHubTestsReportRows(t, first, claim, at); cases != 0 {
		t.Fatalf("in-progress pass wrote %d cases", cases)
	}
	second := githubTestsReportWindowDoerFor(t, "2026-07-22T12:40:00Z")
	firstSuites, firstCases := collectGitHubTestsReportRows(t, second, claim, at)
	again := githubTestsReportWindowDoerFor(t, "2026-07-22T12:40:00Z")
	againSuites, againCases := collectGitHubTestsReportRows(t, again, claim, at.Add(time.Hour))
	if firstCases == 0 || firstSuites != againSuites || firstCases != againCases {
		t.Fatalf("completed pass rows %d/%d, repeat pass %d/%d, want equal and non-zero", firstSuites, firstCases, againSuites, againCases)
	}
}

// A run skipped as in progress is not excluded later by the claim window: its updated_at only moves forward.
func TestGitHubTestsReportPhaseDoesNotExcludeALaterCompletedRunByTheClaimWindow(t *testing.T) {
	t.Parallel()
	since := time.Date(2026, 7, 22, 12, 0, 0, 0, time.UTC)
	claim := githubTestsWindowClaim(t, since, since.Add(time.Hour))
	doer := githubTestsReportWindowDoerFor(t, "2026-07-22T12:59:00Z")
	doer.createdAt = "2026-07-22T09:00:00Z"
	if _, cases := collectGitHubTestsReportRows(t, doer, claim, since.Add(time.Hour+5*time.Minute)); cases == 0 {
		t.Fatalf("run created 3 h before the window and completed inside it was excluded: %v", doer.requests)
	}
}

// The run listing starts one day before the claim's since date, so a run created the day before and completed inside the window is listed.
func TestGitHubTestsRunListingStartsOneDayBeforeTheSinceDate(t *testing.T) {
	t.Parallel()
	doer := githubTestsReportWindowDoerFor(t, "2026-07-22T12:30:00Z")
	claim, at := completedRunsWindow(t)
	collectGitHubTestsReportRows(t, doer, claim, at)
	found := false
	for _, query := range doer.runQueries {
		if strings.Contains(query, "created=%3E%3D2026-07-21") || strings.Contains(query, "created=>=2026-07-21") {
			found = true
		}
		if strings.Contains(query, "branch=") && strings.Contains(query, "2026-07-22") {
			t.Fatalf("listing still starts on the since date: %q", query)
		}
	}
	if !found {
		t.Fatalf("no run listing created>=2026-07-21: %v", doer.runQueries)
	}
}

// githubTestsPassDoer serves one default-branch run with a chosen status and artifact set (id -> JUnit body).
type githubTestsPassDoer struct {
	t         *testing.T
	status    string
	artifacts map[string]string
	ids       []string
}

func (doer *githubTestsPassDoer) Do(request *http.Request) (*http.Response, error) {
	status, header, body := http.StatusOK, http.Header{"Content-Type": {"application/json"}}, "{}"
	path := request.URL.Path
	switch {
	case path == "/repos/acme/api":
		body = gitHubRepositoryFixture
	case path == "/repos/acme/api/actions/runs":
		body = `{"workflow_runs":[{"id":9001,"name":"CI","status":"` + doer.status + `","conclusion":"success",` +
			`"created_at":"2026-07-22T10:00:00Z","run_started_at":"2026-07-22T10:01:00Z","updated_at":"2026-07-22T12:30:00Z",` +
			`"run_attempt":1,"event":"push","head_sha":"abc","head_branch":"main","html_url":"https://github.com/acme/api/actions/runs/9001","pull_requests":[]}]}`
	case path == "/repos/acme/api/actions/runs/9001/jobs":
		body = `{"jobs":[]}`
	case path == "/repos/acme/api/actions/runs/9001/artifacts":
		var list []string
		for _, id := range doer.ids {
			list = append(list, `{"id":`+id+`,"expired":false}`)
		}
		body = `{"artifacts":[` + strings.Join(list, ",") + `]}`
	case strings.HasPrefix(path, "/repos/acme/api/actions/artifacts/") && strings.HasSuffix(path, "/zip"):
		status = http.StatusFound
		header.Set("Location", "https://blob.example/"+strings.Split(path, "/")[6]+".zip")
		body = ""
	case strings.HasPrefix(path, "/") && strings.HasSuffix(path, ".zip"):
		header.Set("Content-Type", "application/zip")
		body = string(githubTestsZip(doer.t, map[string]string{"junit.xml": doer.artifacts[strings.TrimSuffix(strings.TrimPrefix(path, "/"), ".zip")]}))
	default:
		doer.t.Fatalf("unexpected request %s", request.URL.String())
	}
	return &http.Response{StatusCode: status, Header: header, Body: io.NopCloser(strings.NewReader(body)), Request: request}, nil
}

func collectGitHubTestsCaseRows(t *testing.T, doer *githubTestsPassDoer) []testCaseResultRow {
	t.Helper()
	var out []testCaseResultRow
	claim, at := completedRunsWindow(t)
	err := GitHubTestsRouteHandler{}.CollectChunks(context.Background(), claim, providerfoundation.Credential{},
		githubTestsClient(t, fakehttp.Client(doer)), at, "", func(emission ChunkRouteEmission) error {
			for _, effect := range emission.Batch.Effects {
				if effect.Destination == "test_case_results" {
					rows, decodeErr := decodeEffectRows[testCaseResultRow](effect)
					if decodeErr != nil {
						return decodeErr
					}
					out = append(out, rows...)
				}
			}
			return nil
		})
	if err != nil {
		t.Fatalf("CollectChunks: %v", err)
	}
	return out
}

func junitOf(cases ...string) string {
	var body strings.Builder
	body.WriteString(`<testsuites><testsuite name="s" time="1.0">`)
	for _, c := range cases {
		body.WriteString(`<testcase classname="pkg" name="` + c + `" time="0.1"/>`)
	}
	body.WriteString(`</testsuite></testsuites>`)
	return body.String()
}

// Pass 1 sees the run in progress with one shard; pass 2 sees it completed with the aggregate and both shards.
// The partial pass writes nothing, so no key exists that the completed pass does not also write.
func TestGitHubTestsRunReadTwiceWritesNoRowsFromThePartialPass(t *testing.T) {
	t.Parallel()
	artifacts := map[string]string{"20": junitOf("a", "b", "c", "d"), "21": junitOf("a", "b"), "22": junitOf("c", "d")}
	partial := collectGitHubTestsCaseRows(t, &githubTestsPassDoer{t: t, status: "in_progress", artifacts: artifacts, ids: []string{"21"}})
	final := collectGitHubTestsCaseRows(t, &githubTestsPassDoer{t: t, status: "completed", artifacts: artifacts, ids: []string{"20", "21", "22"}})
	if len(partial) != 0 {
		t.Fatalf("the in-progress pass wrote %d case rows, want 0", len(partial))
	}
	if len(final) == 0 {
		t.Fatal("the completed pass wrote no case rows")
	}
}

// Web shape: a finished run read in one pass repeats a case identity inside one artifact (two suites) and across
// two artifacts that disagree on the outcome. The completed-only rule drops nothing by identity: every row stays.
func TestGitHubTestsCompletedRunKeepsEveryRepeatedCaseIdentity(t *testing.T) {
	t.Parallel()
	twoSuites := `<testsuites><testsuite name="project-a" time="1.0"><testcase classname="pkg" name="login" time="0.1"/></testsuite>` +
		`<testsuite name="project-b" time="1.0"><testcase classname="pkg" name="login" time="0.1"/></testsuite></testsuites>`
	failing := `<testsuites><testsuite name="leg-2" time="1.0"><testcase classname="pkg" name="login" time="0.1"><failure message="x"/></testcase></testsuite></testsuites>`
	rows := collectGitHubTestsCaseRows(t, &githubTestsPassDoer{t: t, status: "completed",
		artifacts: map[string]string{"31": twoSuites, "32": failing}, ids: []string{"31", "32"}})
	if len(rows) != 3 {
		t.Fatalf("got %d case rows, want all 3 (2 suites in one artifact + a disagreeing artifact)", len(rows))
	}
	keys := map[string]bool{}
	for _, row := range rows {
		keys[row.SuiteID+"/"+row.CaseID] = true
	}
	if len(keys) != 3 {
		t.Fatalf("got %d distinct keys, want 3", len(keys))
	}
}

func artifactListingStart(t *testing.T, claim Claim) string {
	t.Helper()
	doer := githubTestsReportWindowDoerFor(t, "2026-07-22T12:30:00Z")
	collectGitHubTestsReportRows(t, doer, claim, time.Date(2026, 7, 22, 13, 5, 0, 0, time.UTC))
	for _, query := range doer.runQueries {
		if strings.Contains(query, "branch=") {
			values, err := url.ParseQuery(query)
			if err != nil {
				t.Fatal(err)
			}
			return strings.TrimPrefix(values.Get("created"), ">=")
		}
	}
	t.Fatalf("no artifact run listing: %v", doer.runQueries)
	return ""
}

func TestGitHubTestsRunListingStartFollowsTheClaimWidth(t *testing.T) {
	t.Parallel()
	since := time.Date(2026, 7, 22, 12, 0, 0, 0, time.UTC)
	hour := githubTestsWindowClaim(t, since, since.Add(time.Hour))
	if got := artifactListingStart(t, hour); got != "2026-07-21" {
		t.Fatalf("1 h claim lists from %s, want the day before (2026-07-21)", got)
	}
	wide := githubTestsWindowClaim(t, since, since.Add(30*24*time.Hour-time.Second))
	if got := artifactListingStart(t, wide); got != "2026-06-22" {
		t.Fatalf("30-day claim lists from %s, want date(since - width) = 2026-06-22", got)
	}
	open := nativeTestClaim("github", "tests")
	open.SinceAt, open.BeforeAt = &since, nil
	if got := artifactListingStart(t, open); got != "2026-07-21" {
		t.Fatalf("claim with no BeforeAt lists from %s, want one day (2026-07-21)", got)
	}
}

// The listing is never stricter than the drop rule: a run that finished exactly at since - width is kept by the
// drop rule and is created no later than it finishes, so the listing's start date is on or before its creation date.
func TestGitHubTestsRunListingIsNotStricterThanTheDropRule(t *testing.T) {
	t.Parallel()
	since := time.Date(2026, 7, 22, 12, 0, 0, 0, time.UTC)
	for _, width := range []time.Duration{time.Hour, 24 * time.Hour, 72 * time.Hour, 30*24*time.Hour - time.Second} {
		claim := githubTestsWindowClaim(t, since, since.Add(width))
		oldest := since.Add(-width)
		pipeline := githubTestsPipelineRow{StartedAt: oldest, FinishedAt: &oldest}
		if githubTestsReportRunOutsideWindow(pipeline, claim) {
			t.Fatalf("width %s: drop rule excludes a run finishing at since - width", width)
		}
		start, err := time.Parse(time.DateOnly, artifactListingStart(t, claim))
		if err != nil {
			t.Fatal(err)
		}
		if start.After(oldest) {
			t.Fatalf("width %s: listing starts %s, after the oldest kept run's finish %s", width, start, oldest)
		}
	}
}
