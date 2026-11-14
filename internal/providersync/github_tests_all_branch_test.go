package providersync

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/fakehttp"
)

// githubTestsTwoRunDoer serves a pull-request run (9001, branch feature/x) and a
// later default-branch run (9002) on the SAME commit, each with one JUnit
// artifact of identical bytes.
type githubTestsTwoRunDoer struct {
	t           *testing.T
	archive     []byte
	runListings []string
}

func (doer *githubTestsTwoRunDoer) Do(request *http.Request) (*http.Response, error) {
	doer.t.Helper()
	header := http.Header{"Content-Type": {"application/json"}}
	path := request.URL.Path
	status, body := http.StatusOK, "{}"
	run := func(id, branch, event string) string {
		return `{"id":` + id + `,"name":"CI","status":"completed","conclusion":"success","created_at":"2026-07-22T10:00:00Z","run_started_at":"2026-07-22T10:01:00Z","updated_at":"2026-07-22T10:05:00Z","run_attempt":1,"event":"` + event + `","head_sha":"abc","head_branch":"` + branch + `","html_url":"https://github.com/acme/api/actions/runs/` + id + `","pull_requests":[]}`
	}
	switch {
	case path == "/repos/acme/api":
		body = gitHubRepositoryFixture
	case path == "/repos/acme/api/actions/runs":
		doer.runListings = append(doer.runListings, request.URL.RawQuery)
		body = `{"workflow_runs":[` + run("9001", "feature/x", "pull_request") + `,` + run("9002", "main", "push") + `]}`
		if request.URL.Query().Get("branch") == "main" {
			body = `{"workflow_runs":[` + run("9002", "main", "push") + `]}`
		}
	case strings.HasSuffix(path, "/jobs"):
		body = `{"jobs":[{"id":11,"name":"unit","status":"completed","conclusion":"success","started_at":"2026-07-22T10:01:00Z","completed_at":"2026-07-22T10:04:00Z","labels":["ubuntu-latest"]}]}`
	case path == "/repos/acme/api/actions/runs/9001/artifacts":
		body = `{"artifacts":[{"id":77,"name":"go-test-pr","expired":false}]}`
	case path == "/repos/acme/api/actions/runs/9002/artifacts":
		body = `{"artifacts":[{"id":78,"name":"go-test-main","expired":false}]}`
	case path == "/repos/acme/api/actions/artifacts/77/zip" || path == "/repos/acme/api/actions/artifacts/78/zip":
		status = http.StatusFound
		header.Set("Location", "https://blob.example/report.zip")
		body = ""
	case path == "/report.zip":
		header.Set("Content-Type", "application/zip")
		body = string(doer.archive)
	default:
		doer.t.Fatalf("unexpected request %s", request.URL.String())
	}
	return &http.Response{StatusCode: status, Header: header, Body: io.NopCloser(strings.NewReader(body)), Request: request}, nil
}

func githubTestsRowKeys(t *testing.T, batch CompleteRouteBatch, destination string, fields ...string) []string {
	t.Helper()
	var keys []string
	for _, effect := range batch.Effects {
		if effect.Destination != destination {
			continue
		}
		for _, raw := range effect.Rows {
			var row map[string]any
			if err := json.Unmarshal(raw, &row); err != nil {
				t.Fatal(err)
			}
			parts := make([]string, 0, len(fields))
			for _, field := range fields {
				parts = append(parts, field+"="+row[field].(string))
			}
			keys = append(keys, strings.Join(parts, ","))
		}
	}
	slices.Sort(keys)
	return keys
}

// A pull-request run and the main run on the same commit are two runs: two
// sets of rows under distinct (repo, run, suite, case) keys, and a second
// window over the same runs rewrites the same keys.
func TestGitHubTestsPullRequestAndMainRunOnOneCommitKeepDistinctRowKeys(t *testing.T) {
	doer := &githubTestsTwoRunDoer{t: t, archive: githubTestsZip(t, map[string]string{"junit.xml": githubTestsJUnitFixture})}
	collect := func() CompleteRouteBatch {
		batch, err := (GitHubTestsRouteHandler{}).Collect(context.Background(), nativeTestClaim("github", "tests"),
			providerfoundation.Credential{}, githubTestsClient(t, fakehttp.Client(doer)), time.Now())
		if err != nil {
			t.Fatal(err)
		}
		return batch
	}
	first := collect()
	suites := githubTestsRowKeys(t, first, "test_suite_results", "run_id", "suite_id")
	cases := githubTestsRowKeys(t, first, "test_case_results", "run_id", "suite_id", "case_id")
	if len(suites) != 2 || !strings.Contains(suites[0], "run_id=9001") || !strings.Contains(suites[1], "run_id=9002") {
		t.Fatalf("suite keys = %v, want one suite per run (9001 and 9002)", suites)
	}
	if len(cases) != 4 || len(slices.Compact(slices.Clone(cases))) != 4 {
		t.Fatalf("case keys = %v, want 4 distinct keys (2 cases x 2 runs)", cases)
	}
	second := collect()
	if !slices.Equal(suites, githubTestsRowKeys(t, second, "test_suite_results", "run_id", "suite_id")) ||
		!slices.Equal(cases, githubTestsRowKeys(t, second, "test_case_results", "run_id", "suite_id", "case_id")) {
		t.Fatal("a second window over the same runs wrote different row keys")
	}
}

// The same bounds on the wire: the report phase's run listing of a real claim.
func TestGitHubTestsReportPhaseListsRunsFromTheClaimsFirstDayUpToItsUpperBound(t *testing.T) {
	doer := &githubTestsTwoRunDoer{t: t, archive: githubTestsZip(t, map[string]string{"junit.xml": githubTestsJUnitFixture})}
	claim := nativeTestClaim("github", "tests")
	if _, err := (GitHubTestsRouteHandler{}).Collect(context.Background(), claim, providerfoundation.Credential{},
		githubTestsClient(t, fakehttp.Client(doer)), time.Now()); err != nil {
		t.Fatal(err)
	}
	// nativeTestClaim: since 2026-07-01T00:00:00Z, before 2026-07-31T23:59:59Z, so
	// one window width (30d 23:59:59) before since falls on 2026-05-31.
	want := "2026-05-31..2026-07-31T23:59:59Z"
	found := false
	for _, listing := range doer.runListings {
		found = found || strings.Contains(listing, "created="+strings.NewReplacer(":", "%3A").Replace(want))
	}
	if !found {
		t.Fatalf("no run listing carried created=%s: %v", want, doer.runListings)
	}
}

// githubTestsMidnightRunDoer serves one run created at 23:50 on day D and
// finished at 00:20 on day D+1, listed only when its created date falls inside
// the request's created filter, as GitHub does.
type githubTestsMidnightRunDoer struct {
	t       *testing.T
	archive []byte
}

func (doer *githubTestsMidnightRunDoer) Do(request *http.Request) (*http.Response, error) {
	doer.t.Helper()
	header := http.Header{"Content-Type": {"application/json"}}
	path := request.URL.Path
	status, body := http.StatusOK, "{}"
	switch {
	case path == "/repos/acme/api":
		body = gitHubRepositoryFixture
	case path == "/repos/acme/api/actions/runs":
		created := time.Date(2026, 7, 21, 23, 50, 0, 0, time.UTC)
		lower, upper, _ := strings.Cut(request.URL.Query().Get("created"), "..")
		inside := true
		if lower != "" && lower != "*" {
			layout := time.RFC3339
			if len(lower) == len(time.DateOnly) {
				layout = time.DateOnly
			}
			start, err := time.Parse(layout, lower)
			if err != nil {
				doer.t.Fatalf("created lower bound %q: %v", lower, err)
			}
			inside = inside && !created.Before(start)
		}
		if upper != "" && upper != "*" {
			end, err := time.Parse(time.RFC3339, upper)
			if err != nil {
				doer.t.Fatalf("created upper bound %q: %v", upper, err)
			}
			inside = inside && !created.After(end)
		}
		body = `{"workflow_runs":[]}`
		if inside {
			body = `{"workflow_runs":[{"id":9100,"name":"CI","status":"completed","conclusion":"success","created_at":"2026-07-21T23:50:00Z","run_started_at":"2026-07-21T23:51:00Z","updated_at":"2026-07-22T00:20:00Z","run_attempt":1,"event":"pull_request","head_sha":"abc","head_branch":"feature/x","html_url":"https://github.com/acme/api/actions/runs/9100","pull_requests":[]}]}`
		}
	case strings.HasSuffix(path, "/jobs"):
		body = `{"jobs":[]}`
	case path == "/repos/acme/api/actions/runs/9100/artifacts":
		body = `{"artifacts":[{"id":91,"name":"go-test","expired":false}]}`
	case path == "/repos/acme/api/actions/artifacts/91/zip":
		status = http.StatusFound
		header.Set("Location", "https://blob.example/report.zip")
		body = ""
	case path == "/report.zip":
		header.Set("Content-Type", "application/zip")
		body = string(doer.archive)
	default:
		doer.t.Fatalf("unexpected request %s", request.URL.String())
	}
	return &http.Response{StatusCode: status, Header: header, Body: io.NopCloser(strings.NewReader(body)), Request: request}, nil
}

// A run created at 23:50 that finishes at 00:20 is still changing during the
// window [00:00, 01:00) of the next day: the window rule keeps it in scope for
// one window width back, so the report listing must reach back that far too,
// across midnight. Otherwise its artifacts, which may only appear after its
// last window, are never fetched.
func TestGitHubTestsReportPhaseListsARunCreatedBeforeMidnightThatFinishesAfterIt(t *testing.T) {
	doer := &githubTestsMidnightRunDoer{t: t, archive: githubTestsZip(t, map[string]string{"junit.xml": githubTestsJUnitFixture})}
	claim := nativeTestClaim("github", "tests")
	since := time.Date(2026, 7, 22, 0, 0, 0, 0, time.UTC)
	before := time.Date(2026, 7, 22, 1, 0, 0, 0, time.UTC)
	claim.SinceAt, claim.BeforeAt = &since, &before
	batch, err := (GitHubTestsRouteHandler{}).Collect(context.Background(), claim, providerfoundation.Credential{},
		githubTestsClient(t, fakehttp.Client(doer)), time.Date(2026, 7, 22, 1, 5, 0, 0, time.UTC))
	if err != nil {
		t.Fatal(err)
	}
	counts := map[string]int{}
	for _, effect := range batch.Effects {
		counts[effect.Destination] = len(effect.Rows)
	}
	if counts["test_suite_results"] != 1 || counts["test_case_results"] != 2 {
		t.Fatalf("the run created before midnight lost its report: %v", counts)
	}
}
