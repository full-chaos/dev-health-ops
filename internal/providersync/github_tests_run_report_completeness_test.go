package providersync

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"slices"
	"strconv"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/fakehttp"
)

// CHAOS-13826: the chunked GitHub tests route writes one testops_run_report_completeness row per walked run that has report
// artifacts: complete, or partial with each loss cause. This file names the destination and its columns as strings, so it builds
// on a tree without the writer: there it is the red cell (no such effect row).

type completenessArtifact struct {
	name     string
	expired  bool
	body     []byte
	listSize int // 0 = len(body)
}

type completenessDoer struct {
	t         *testing.T
	runs      string
	artifacts []completenessArtifact
}

func (doer *completenessDoer) Do(request *http.Request) (*http.Response, error) {
	doer.t.Helper()
	header := http.Header{"Content-Type": {"application/json"}}
	path := request.URL.Path
	switch {
	case path == "/repos/acme/api":
		return githubTestsHTTPResponse(request, header, gitHubRepositoryFixture), nil
	case path == "/repos/acme/api/actions/runs":
		return githubTestsHTTPResponse(request, header, doer.runs), nil
	case strings.HasSuffix(path, "/jobs"):
		return githubTestsHTTPResponse(request, header, `{"jobs":[]}`), nil
	case strings.HasSuffix(path, "/artifacts"):
		items := make([]string, 0, len(doer.artifacts))
		for index, artifact := range doer.artifacts {
			size := artifact.listSize
			if size == 0 {
				size = len(artifact.body)
			}
			items = append(items, fmt.Sprintf(`{"id":%d,"name":%q,"expired":%t,"size_in_bytes":%d}`, index+1, artifact.name, artifact.expired, size))
		}
		return githubTestsHTTPResponse(request, header, `{"artifacts":[`+strings.Join(items, ",")+`]}`), nil
	case strings.HasPrefix(path, "/repos/acme/api/actions/artifacts/") && strings.HasSuffix(path, "/zip"):
		id, err := strconv.Atoi(strings.TrimSuffix(strings.TrimPrefix(path, "/repos/acme/api/actions/artifacts/"), "/zip"))
		if err != nil || id < 1 || id > len(doer.artifacts) || doer.artifacts[id-1].expired {
			doer.t.Fatalf("unexpected download %s", path)
		}
		return &http.Response{
			StatusCode: http.StatusOK, Header: http.Header{"Content-Type": {"application/zip"}},
			Body: io.NopCloser(bytes.NewReader(doer.artifacts[id-1].body)), Request: request,
		}, nil
	}
	doer.t.Fatalf("unexpected request %s", request.URL.String())
	return nil, nil
}

// completenessRuns lists the given run ids (one item each, so an id given twice is listed twice) at the given attempt.
func completenessRuns(attempt int, ids ...int) string {
	items := make([]string, 0, len(ids))
	for _, id := range ids {
		items = append(items, fmt.Sprintf(`{"id":%d,"name":"CI","status":"completed","conclusion":"success","created_at":"2026-07-22T10:00:00Z",`+
			`"run_started_at":"2026-07-22T10:01:00Z","updated_at":"2026-07-22T10:05:00Z","run_attempt":%d,"event":"push","head_sha":"abc",`+
			`"head_branch":"main","html_url":"https://github.com/acme/api/actions/runs/%d","pull_requests":[]}`, id, attempt, id))
	}
	return `{"workflow_runs":[` + strings.Join(items, ",") + `]}`
}

type completenessRow struct {
	RunID           string   `json:"run_id"`
	RunAttempt      int64    `json:"run_attempt"`
	Provider        string   `json:"provider"`
	Complete        bool     `json:"complete"`
	Reasons         []string `json:"reasons"`
	ReportsExpected int64    `json:"reports_expected"`
	ReportsAdmitted int64    `json:"reports_admitted"`
}

func completenessRows(t *testing.T, handler GitHubTestsRouteHandler, doer *completenessDoer) ([]completenessRow, CompleteRouteBatch) {
	t.Helper()
	final, effects := walkGitHubTestsChunksCapturingEffects(t, handler, nativeTestClaim("github", "cicd"), githubTestsClient(t, fakehttp.Client(doer)), 4)
	var rows []completenessRow
	for _, effect := range effects {
		if effect.Destination != "testops_run_report_completeness" {
			continue
		}
		for _, raw := range effect.Rows {
			var row completenessRow
			if err := json.Unmarshal(raw, &row); err != nil {
				t.Fatal(err)
			}
			rows = append(rows, row)
		}
	}
	return rows, final
}

func completenessJUnit(prefix string, cases int) string {
	var body strings.Builder
	body.WriteString(`<testsuites><testsuite name="` + prefix + `suite" time="1.5">`)
	for index := 1; index <= cases; index++ {
		body.WriteString(`<testcase name="` + prefix + `case-` + strconv.Itoa(index) + `" classname="pkg" time="0.` + strconv.Itoa(index) + `"/>`)
	}
	body.WriteString(`</testsuite></testsuites>`)
	return body.String()
}

func TestGitHubTestsRunReportCompletenessRows(t *testing.T) {
	readable := func(prefix string) []byte {
		return githubTestsZip(t, map[string]string{"junit.xml": completenessJUnit(prefix, 2)})
	}
	copyOf := readable("same-")
	cells := []struct {
		name      string
		handler   GitHubTestsRouteHandler
		attempt   int
		runs      []int
		artifacts []completenessArtifact
		want      []completenessRow
	}{
		{
			name:      "every report read is complete",
			artifacts: []completenessArtifact{{name: "test-results-a", body: readable("a-")}, {name: "test-results-b", body: readable("b-")}},
			want:      []completenessRow{{RunID: "1", RunAttempt: 1, Provider: "github_actions", Complete: true, Reasons: []string{}, ReportsExpected: 2, ReportsAdmitted: 2}},
		},
		{
			name:      "an archive that does not open is partial",
			artifacts: []completenessArtifact{{name: "test-results-a", body: readable("a-")}, {name: "test-results-b", body: []byte("not a zip archive")}},
			want:      []completenessRow{{RunID: "1", RunAttempt: 1, Provider: "github_actions", Reasons: []string{"unreadable_archive"}, ReportsExpected: 2, ReportsAdmitted: 1}},
		},
		{
			name:      "an expired report is partial",
			artifacts: []completenessArtifact{{name: "test-results-a", body: readable("a-")}, {name: "test-results-b", expired: true, body: readable("b-")}},
			want:      []completenessRow{{RunID: "1", RunAttempt: 1, Provider: "github_actions", Reasons: []string{"artifact_expired"}, ReportsExpected: 2, ReportsAdmitted: 1}},
		},
		{
			name:      "a report over the size cap is partial",
			handler:   GitHubTestsRouteHandler{MaxArtifactBytes: 4096},
			artifacts: []completenessArtifact{{name: "test-results-a", body: readable("a-")}, {name: "test-results-b", body: readable("b-"), listSize: 5000}},
			want:      []completenessRow{{RunID: "1", RunAttempt: 1, Provider: "github_actions", Reasons: []string{"artifact_oversized"}, ReportsExpected: 2, ReportsAdmitted: 1}},
		},
		{
			name:    "artifacts over the per-run cap are partial",
			handler: GitHubTestsRouteHandler{MaxArtifactsPerRun: 2},
			artifacts: []completenessArtifact{
				{name: "test-results-a", body: readable("a-")}, {name: "test-results-b", body: readable("b-")}, {name: "test-results-c", body: readable("c-")},
			},
			want: []completenessRow{{RunID: "1", RunAttempt: 1, Provider: "github_actions", Reasons: []string{"per_run_cap"}, ReportsExpected: 3, ReportsAdmitted: 2}},
		},
		{
			// acp ruling C7: the producer's copy drop is not yet shown lossless, so a dropped copy fails closed.
			name:      "a dropped copy is partial until the producer drop is lossless",
			artifacts: []completenessArtifact{{name: "test-results-a", body: copyOf}, {name: "test-results-b", body: copyOf}},
			want:      []completenessRow{{RunID: "1", RunAttempt: 1, Provider: "github_actions", Reasons: []string{"aggregate_of_parts"}, ReportsExpected: 2, ReportsAdmitted: 1}},
		},
		{
			name:    "a re-run attempt is its own key",
			attempt: 3,
			artifacts: []completenessArtifact{
				{name: "test-results-a", body: readable("a-")},
			},
			want: []completenessRow{{RunID: "1", RunAttempt: 3, Provider: "github_actions", Complete: true, Reasons: []string{}, ReportsExpected: 1, ReportsAdmitted: 1}},
		},
		{
			// The second walk of the run does not record the expired artifact again (marker dedup); its row still names it.
			name:      "a run walked twice in a unit names its expired report both times",
			runs:      []int{1, 1},
			artifacts: []completenessArtifact{{name: "test-results-a", body: readable("a-")}, {name: "test-results-b", expired: true, body: readable("b-")}},
			want: []completenessRow{
				{RunID: "1", RunAttempt: 1, Provider: "github_actions", Reasons: []string{"artifact_expired"}, ReportsExpected: 2, ReportsAdmitted: 1},
				{RunID: "1", RunAttempt: 1, Provider: "github_actions", Reasons: []string{"artifact_expired"}, ReportsExpected: 2, ReportsAdmitted: 1},
			},
		},
		{
			name:      "a run with no report artifact has no row",
			artifacts: nil,
			want:      nil,
		},
	}
	for _, cell := range cells {
		t.Run(cell.name, func(t *testing.T) {
			attempt := cell.attempt
			if attempt == 0 {
				attempt = 1
			}
			runs := cell.runs
			if runs == nil {
				runs = []int{1}
			}
			got, final := completenessRows(t, cell.handler, &completenessDoer{t: t, runs: completenessRuns(attempt, runs...), artifacts: cell.artifacts})
			if !slices.EqualFunc(got, cell.want, func(a, b completenessRow) bool {
				return a.RunID == b.RunID && a.RunAttempt == b.RunAttempt && a.Provider == b.Provider && a.Complete == b.Complete &&
					slices.Equal(a.Reasons, b.Reasons) && a.ReportsExpected == b.ReportsExpected && a.ReportsAdmitted == b.ReportsAdmitted
			}) {
				t.Fatalf("completeness rows %+v, want %+v (unit result reports_complete=%v incomplete=%v)",
					got, cell.want, final.Result["reports_complete"], final.Result["incomplete"])
			}
		})
	}
}
