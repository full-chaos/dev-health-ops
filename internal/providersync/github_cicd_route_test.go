package providersync

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
)

const gitHubCICDWorkflowRunsFixture = `{
  "workflow_runs": [
    {
      "id": 101,
      "conclusion": "success",
      "status": "completed",
      "created_at": "2026-07-22T10:00:00Z",
      "run_started_at": "2026-07-22T10:01:00Z",
      "updated_at": "2026-07-22T10:05:00Z",
      "run_attempt": 3
    },
    {
      "id": 102,
      "status": "queued",
      "created_at": "2026-07-22T11:00:00Z",
      "run_started_at": null,
      "updated_at": null,
      "run_attempt": "not-a-number"
    },
    {
      "id": 103,
      "status": "completed",
      "created_at": "2026-06-20T10:00:00Z",
      "run_started_at": "2026-06-20T10:01:00Z",
      "updated_at": "2026-06-20T10:05:00Z",
      "run_attempt": 1
    },
    {
      "id": 104,
      "status": "completed",
      "created_at": "2026-08-01T10:00:00Z",
      "run_started_at": "2026-08-01T10:01:00Z",
      "updated_at": "2026-08-01T10:05:00Z",
      "run_attempt": 1
    }
  ]
}`

type gitHubCICDDoer struct {
	t        *testing.T
	requests []string
}

func (doer *gitHubCICDDoer) Do(request *http.Request) (*http.Response, error) {
	doer.t.Helper()
	doer.requests = append(doer.requests, request.URL.Path)
	body := gitHubCICDWorkflowRunsFixture
	if request.URL.Path == "/repos/acme/api" {
		body = gitHubRepositoryFixture
	}
	return &http.Response{
		StatusCode: http.StatusOK,
		Header:     http.Header{"Content-Type": []string{"application/json"}},
		Body:       io.NopCloser(strings.NewReader(body)),
		Request:    request,
	}, nil
}

func TestGitHubCICDRouteEmitsOnlyWindowedPipelineRuns(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 7, 23, 12, 30, 0, 0, time.UTC)
	doer := &gitHubCICDDoer{t: t}
	client := gitHubRepositoryClient(t, doer, "https://api.github.com")
	claim := nativeTestClaim("github", "cicd")

	batch, err := (GitHubCICDRouteHandler{}).Collect(
		context.Background(), claim, providerfoundation.Credential{}, client, now,
	)
	if err != nil {
		t.Fatal(err)
	}
	descriptor, ok := (CompleteRouteSwitches{}).Descriptor("github", "cicd")
	if !ok {
		t.Fatal("github/cicd has no canonical descriptor")
	}
	if err := batch.validate(descriptor); err != nil {
		t.Fatalf("batch does not satisfy the canonical destination manifest: %v", err)
	}
	if batch.Watermark == nil || !batch.Watermark.Equal(*claim.BeforeAt) {
		t.Fatalf("watermark=%v want claim.BeforeAt=%v", batch.Watermark, claim.BeforeAt)
	}
	if len(batch.Effects) != 1 || batch.Effects[0].Destination != "ci_pipeline_runs" ||
		batch.Effects[0].Recovery != EffectReadbackRequired || len(batch.Effects[0].Rows) != 2 {
		t.Fatalf("effects=%+v", batch.Effects)
	}
	if got, want := doer.requests, []string{"/repos/acme/api", "/repos/acme/api/actions/runs"}; len(got) != len(want) || got[0] != want[0] || got[1] != want[1] {
		t.Fatalf("requests=%v want=%v", got, want)
	}

	var first, second ciPipelineRunRow
	if err := json.Unmarshal(batch.Effects[0].Rows[0], &first); err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(batch.Effects[0].Rows[1], &second); err != nil {
		t.Fatal(err)
	}
	if first.OrgID != claim.OrgID || first.RunID != "101" || first.Status == nil || *first.Status != "success" ||
		first.RetryCount != 2 || first.QueuedAt == nil || first.StartedAt.IsZero() ||
		first.FinishedAt == nil {
		t.Fatalf("first row=%+v", first)
	}
	if second.OrgID != claim.OrgID || second.RunID != "102" || second.Status == nil || *second.Status != "queued" ||
		second.RetryCount != 0 || second.QueuedAt == nil ||
		!second.StartedAt.Equal(*second.QueuedAt) || second.FinishedAt != nil {
		t.Fatalf("second row=%+v", second)
	}
}

func TestPipelineRunReadbackRejectsEachPersistedFieldMismatch(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 7, 23, 12, 30, 0, 0, time.UTC)
	status := "success"
	queuedAt := now.Add(-2 * time.Minute)
	finishedAt := now.Add(-time.Minute)
	expected := ciPipelineRunRow{
		OrgID: "org-1", RepoID: "c7198fbc-1945-3717-05d8-eb78866b4e79", RunID: "101",
		Status: &status, QueuedAt: &queuedAt, StartedAt: now.Add(-90 * time.Second),
		FinishedAt: &finishedAt, RetryCount: 2, LastSynced: now,
	}
	matching := ciPipelineRunVersion{Row: expected, LastSynced: now, Found: true}
	if got := comparePipelineRunVersion(expected, matching); got != EffectExact {
		t.Fatalf("matching version=%s", got)
	}
	tests := []struct {
		name   string
		mutate func(*ciPipelineRunVersion)
	}{
		{"org ID", func(actual *ciPipelineRunVersion) { actual.Row.OrgID = "other-org" }},
		{"run ID", func(actual *ciPipelineRunVersion) { actual.Row.RunID = "other" }},
		{"status", func(actual *ciPipelineRunVersion) { value := "failure"; actual.Row.Status = &value }},
		{"queued at", func(actual *ciPipelineRunVersion) { value := queuedAt.Add(time.Second); actual.Row.QueuedAt = &value }},
		{"started at", func(actual *ciPipelineRunVersion) { actual.Row.StartedAt = expected.StartedAt.Add(time.Second) }},
		{"finished at", func(actual *ciPipelineRunVersion) {
			value := finishedAt.Add(time.Second)
			actual.Row.FinishedAt = &value
		}},
		{"retry count", func(actual *ciPipelineRunVersion) { actual.Row.RetryCount++ }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			actual := matching
			test.mutate(&actual)
			if got := comparePipelineRunVersion(expected, actual); got != EffectConflict {
				t.Fatalf("inspection=%s", got)
			}
		})
	}
}

func TestWorkflowRetryCountClampsPreFirstAttempts(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		input any
		want  uint32
	}{
		{json.Number("0"), 0},
		{json.Number("1"), 0},
		{json.Number("2"), 1},
		{json.Number("3"), 2},
		{"invalid", 0},
	} {
		if got := workflowRetryCount(test.input); got != test.want {
			t.Fatalf("workflowRetryCount(%v)=%d want=%d", test.input, got, test.want)
		}
	}
}
