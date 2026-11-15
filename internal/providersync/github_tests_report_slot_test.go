package providersync

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
)

type reportSlotArtifact struct {
	id      int
	name    string
	created string
	archive []byte
}

// reportSlotDoer serves one completed run at a given attempt with the listed artifacts and records every download.
type reportSlotDoer struct {
	t         *testing.T
	attempt   int
	started   string
	artifacts []reportSlotArtifact
	paths     []string
}

func (doer *reportSlotDoer) Do(request *http.Request) (*http.Response, error) {
	doer.t.Helper()
	path := request.URL.Path
	doer.paths = append(doer.paths, path)
	status, header, body := http.StatusOK, http.Header{"Content-Type": {"application/json"}}, "{}"
	switch {
	case path == "/repos/acme/api":
		body = gitHubRepositoryFixture
	case path == "/repos/acme/api/actions/runs":
		body = `{"workflow_runs":[{"id":43600000001,"name":"Go","status":"completed","conclusion":"success",` +
			`"created_at":"2026-07-22T10:00:00Z","run_started_at":"` + doer.started + `","updated_at":"2026-07-22T11:40:00Z",` +
			`"run_attempt":` + strconv.Itoa(doer.attempt) + `,"event":"push","head_sha":"abc","head_branch":"main",` +
			`"html_url":"https://github.com/acme/api/actions/runs/43600000001","pull_requests":[]}]}`
	case path == "/repos/acme/api/actions/runs/43600000001/jobs":
		body = `{"jobs":[]}`
	case path == "/repos/acme/api/actions/runs/43600000001/artifacts":
		items := make([]string, 0, len(doer.artifacts))
		for _, artifact := range doer.artifacts {
			items = append(items, fmt.Sprintf(`{"id":%d,"name":%q,"expired":false,"size_in_bytes":%d,"created_at":%q}`,
				artifact.id, artifact.name, len(artifact.archive), artifact.created))
		}
		body = `{"total_count":` + strconv.Itoa(len(items)) + `,"artifacts":[` + strings.Join(items, ",") + `]}`
	case strings.HasPrefix(path, "/repos/acme/api/actions/artifacts/") && strings.HasSuffix(path, "/zip"):
		id := strings.TrimSuffix(strings.TrimPrefix(path, "/repos/acme/api/actions/artifacts/"), "/zip")
		status, body = http.StatusFound, ""
		header.Set("Location", "https://blob.example/a"+id+".zip")
	case strings.HasPrefix(path, "/a") && strings.HasSuffix(path, ".zip"):
		id, _ := strconv.Atoi(strings.TrimSuffix(strings.TrimPrefix(path, "/a"), ".zip"))
		for _, artifact := range doer.artifacts {
			if artifact.id == id {
				header.Set("Content-Type", "application/zip")
				body = string(artifact.archive)
			}
		}
	default:
		doer.t.Fatalf("unexpected request %s", request.URL.String())
	}
	return &http.Response{StatusCode: status, Header: header, Body: io.NopCloser(strings.NewReader(body)), Request: request}, nil
}

func (doer *reportSlotDoer) downloads() []string {
	var out []string
	for _, path := range doer.paths {
		if strings.HasPrefix(path, "/a") && strings.HasSuffix(path, ".zip") {
			out = append(out, path)
		}
	}
	sort.Strings(out)
	return out
}

// reportSlotKeys runs one tests unit over the doer and returns the emitted suite and case natural keys.
func reportSlotKeys(t *testing.T, doer *reportSlotDoer) (suites, cases []string) {
	t.Helper()
	since := time.Date(2026, 7, 22, 11, 0, 0, 0, time.UTC)
	err := GitHubTestsRouteHandler{}.CollectChunks(context.Background(),
		githubTestsWindowClaim(t, since, since.Add(time.Hour)), providerfoundation.Credential{},
		githubTestsClient(t, doer), since.Add(65*time.Minute), "",
		func(emission ChunkRouteEmission) error {
			for _, effect := range emission.Batch.Effects {
				switch effect.Destination {
				case "test_suite_results":
					rows, err := decodeEffectRows[testSuiteResultRow](effect)
					if err != nil {
						return err
					}
					for _, row := range rows {
						suites = append(suites, row.SuiteID)
					}
				case "test_case_results":
					rows, err := decodeEffectRows[testCaseResultRow](effect)
					if err != nil {
						return err
					}
					for _, row := range rows {
						cases = append(cases, row.SuiteID+"/"+row.CaseID)
					}
				}
			}
			return nil
		})
	if err != nil {
		t.Fatalf("CollectChunks: %v", err)
	}
	sort.Strings(suites)
	sort.Strings(cases)
	return suites, cases
}

func reportSlotArchive(t *testing.T) []byte {
	t.Helper()
	return githubTestsZip(t, map[string]string{"report.xml": `<testsuites><testsuite name="pkg/b" time="1.5" timestamp="2026-07-22T10:02:00">` +
		`<testcase classname="pkg/b" name="TestX" time="0.5"></testcase><testcase classname="pkg/b" name="TestY" time="1.0"></testcase>` +
		`</testsuite></testsuites>`})
}

// CHAOS-13873 u1: the same report bytes under a new artifact id, the same name and the same attempt keep their natural keys, so a
// re-read replaces the rows instead of adding a second copy beside them. A different attempt of the same name gets its own keys.
func TestGitHubTestsReportKeysFollowTheSlotAndAttemptNotTheArtifactID(t *testing.T) {
	t.Parallel()
	archive := reportSlotArchive(t)
	first := &reportSlotDoer{t: t, attempt: 1, started: "2026-07-22T10:01:00Z",
		artifacts: []reportSlotArtifact{{101, "go-test-b", "2026-07-22T10:20:00Z", archive}}}
	again := &reportSlotDoer{t: t, attempt: 1, started: "2026-07-22T10:01:00Z",
		artifacts: []reportSlotArtifact{{205, "go-test-b", "2026-07-22T10:20:00Z", archive}}}
	suites1, cases1 := reportSlotKeys(t, first)
	suites2, cases2 := reportSlotKeys(t, again)
	if len(suites1) != 1 || len(cases1) != 2 {
		t.Fatalf("the report gave suites %v cases %v, want 1 and 2", suites1, cases1)
	}
	if strings.Join(suites1, ",") != strings.Join(suites2, ",") || strings.Join(cases1, ",") != strings.Join(cases2, ",") {
		t.Fatalf("the same slot and attempt under artifact 101 and 205 gave different keys: %v %v / %v %v", suites1, cases1, suites2, cases2)
	}
	rerun := &reportSlotDoer{t: t, attempt: 2, started: "2026-07-22T11:10:00Z",
		artifacts: []reportSlotArtifact{{206, "go-test-b", "2026-07-22T11:30:00Z", archive}}}
	suites3, _ := reportSlotKeys(t, rerun)
	if strings.Join(suites3, ",") == strings.Join(suites1, ",") {
		t.Fatalf("attempt 2 of the slot reused attempt 1's suite key %v", suites3)
	}
}

// CHAOS-13873 u2: a listing that holds an older attempt's artifact next to the re-run's, under one name, reads only the newest.
func TestGitHubTestsReportReadsOnlyTheNewestArtifactOfAName(t *testing.T) {
	t.Parallel()
	archive := reportSlotArchive(t)
	doer := &reportSlotDoer{t: t, attempt: 2, started: "2026-07-22T11:10:00Z", artifacts: []reportSlotArtifact{
		{102, "go-test-b", "2026-07-22T10:20:00Z", archive}, {203, "go-test-b", "2026-07-22T11:30:00Z", archive}, {104, "go-test-c", "2026-07-22T10:20:00Z", archive},
	}}
	_, cases := reportSlotKeys(t, doer)
	if got := strings.Join(doer.downloads(), ","); got != "/a104.zip,/a203.zip" {
		t.Fatalf("downloaded %s, want only the newest go-test-b (203) and go-test-c (104)", got)
	}
	if len(cases) != 4 {
		t.Fatalf("emitted %d case rows, want 4 (2 per kept artifact)", len(cases))
	}
}

// CHAOS-13873 u3: the attempt label of an artifact.
func TestGitHubTestsReportSlotLabel(t *testing.T) {
	t.Parallel()
	started := "2026-07-22T11:10:00Z"
	for _, cell := range []struct {
		name    string
		attempt any
		started *string
		created *string
		want    githubTestsReportSlot
	}{
		{"made in the latest attempt", "2", &started, ptrString("2026-07-22T11:30:00Z"), githubTestsReportSlot{"go-test-b", 2}},
		{"made at the latest attempt's start", "2", &started, &started, githubTestsReportSlot{"go-test-b", 2}},
		{"carried over from an earlier attempt", "3", &started, ptrString("2026-07-22T10:20:00Z"), githubTestsReportSlot{"go-test-b", 2}},
		{"first attempt made before its start", "1", &started, ptrString("2026-07-22T10:20:00Z"), githubTestsReportSlot{"go-test-b", 1}},
		{"no created_at", "2", &started, nil, githubTestsReportSlot{"go-test-b", 2}},
		{"no run_started_at", "2", nil, ptrString("2026-07-22T10:20:00Z"), githubTestsReportSlot{"go-test-b", 2}},
		{"no run_attempt", nil, &started, ptrString("2026-07-22T11:30:00Z"), githubTestsReportSlot{"go-test-b", 1}},
	} {
		run := gitHubWorkflowRunPayload{RunAttempt: cell.attempt, RunStartedAt: cell.started}
		artifact := githubTestsArtifactPayload{ID: "7", Name: "go-test-b", CreatedAt: cell.created}
		if got := githubTestsReportSlotFor(run, artifact); got != cell.want {
			t.Errorf("%s: %+v, want %+v", cell.name, got, cell.want)
		}
	}
	if got := githubTestsReportSlotFor(gitHubWorkflowRunPayload{}, githubTestsArtifactPayload{ID: "7"}); got.Name != "7" {
		t.Errorf("a nameless artifact takes its id as the slot, got %+v", got)
	}
}
