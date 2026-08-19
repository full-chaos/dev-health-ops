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

type gitLabPullRequestTraversalTrace struct {
	ProducerRequests  []string                             `json:"producer_requests"`
	UsageRequestCount int                                  `json:"usage_request_count"`
	Rows              []gitLabPullRequestTraversalTraceRow `json:"rows"`
}

type gitLabPullRequestTraversalTraceRow struct {
	Number        int     `json:"number"`
	State         string  `json:"state"`
	ReviewsCount  int     `json:"reviews_count"`
	CommentsCount int     `json:"comments_count"`
	FirstReviewAt *string `json:"first_review_at"`
}

type gitLabPullRequestTraversalDoer struct {
	t        *testing.T
	input    map[string]any
	requests []*http.Request
}

func (doer *gitLabPullRequestTraversalDoer) Do(request *http.Request) (*http.Response, error) {
	doer.t.Helper()
	doer.requests = append(doer.requests, request)
	path := request.URL.Path
	query := request.URL.Query()
	var payload any
	headers := make(http.Header)
	switch {
	case path == "/api/v4/projects/123":
		payload = json.RawMessage(gitLabRepositoryFixture)
	case path == "/api/v4/projects/123/merge_requests":
		page := query.Get("page")
		pages, ok := doer.input["mr_pages"].(map[string]any)
		if !ok {
			doer.t.Fatalf("mr_pages=%T", doer.input["mr_pages"])
		}
		payload = pages[page]
		if nextPages, ok := doer.input["mr_next_pages"].(map[string]any); ok {
			if next := nextPages[page]; next != nil {
				headers.Set("X-Next-Page", stringValue(next))
			}
		}
	default:
		prefix := "/api/v4/projects/123/merge_requests/"
		if !strings.HasPrefix(path, prefix) {
			doer.t.Fatalf("unexpected traversal request %s", request.URL.RequestURI())
		}
		remaining := strings.TrimPrefix(path, prefix)
		parts := strings.Split(strings.Trim(remaining, "/"), "/")
		if len(parts) != 2 {
			doer.t.Fatalf("unexpected MR request %s", request.URL.RequestURI())
		}
		iid, endpoint := parts[0], parts[1]
		switch endpoint {
		case "approvals":
			approvalMap, _ := doer.input["approvals"].(map[string]any)
			payload = approvalMap[iid]
			if payload == nil {
				payload = map[string]any{}
			}
		case "notes":
			notesByIID, _ := doer.input["notes_pages"].(map[string]any)
			pages, _ := notesByIID[iid].(map[string]any)
			payload = pages[query.Get("page")]
			if payload == nil {
				payload = []any{}
			}
			nextByIID, _ := doer.input["notes_next_pages"].(map[string]any)
			nextPages, _ := nextByIID[iid].(map[string]any)
			if next := nextPages[query.Get("page")]; next != nil {
				headers.Set("X-Next-Page", stringValue(next))
			}
		default:
			doer.t.Fatalf("unexpected MR endpoint %s", endpoint)
		}
	}
	if payload == nil {
		payload = []any{}
	}
	encoded, err := json.Marshal(payload)
	if err != nil {
		doer.t.Fatal(err)
	}
	return &http.Response{
		StatusCode: http.StatusOK, Header: headers,
		Body: io.NopCloser(strings.NewReader(string(encoded))), Request: request,
	}, nil
}

func oracleGitLabPullRequestTraversalCases() []oracleCase {
	return []oracleCase{{
		ID: "pagewise_enrichment_notes_pagination_and_since_stop",
		Input: map[string]any{
			"repo_id":  "c7198fbc-1945-3717-05d8-eb78866b4e79",
			"per_page": 2,
			"since":    "2026-07-01T00:00:00Z", "until": "2026-07-31T23:59:59Z",
			"mr_pages": map[string]any{
				"1": []any{
					map[string]any{"iid": 99, "state": "opened", "updated_at": "2026-08-01T10:00:00Z"},
					map[string]any{"iid": 7, "title": "Add API", "state": "opened", "author": map[string]any{"username": "author"}, "created_at": "2026-07-15T10:00:00Z", "updated_at": "2026-07-20T10:00:00Z", "user_notes_count": 4},
				},
				"2": []any{map[string]any{"iid": 6, "state": "closed", "updated_at": "2026-06-30T10:00:00Z"}},
			},
			"mr_next_pages": map[string]any{"1": "2"},
			"approvals":     map[string]any{"7": map[string]any{"approved_by": []any{map[string]any{"user": map[string]any{"id": 88, "username": "reviewer"}}}}},
			"notes_pages": map[string]any{"7": map[string]any{
				"1": []any{map[string]any{"id": 1, "system": true, "body": "approved this merge request", "author": map[string]any{"username": "reviewer"}, "created_at": "2026-07-16T11:00:00Z"}, map[string]any{"id": 2, "type": "DiffNote", "author": map[string]any{"username": "reviewer2"}, "created_at": "2026-07-17T11:00:00Z"}},
				"2": []any{map[string]any{"id": 3, "type": "DiscussionNote", "author": map[string]any{"username": "reviewer3"}, "created_at": "2026-07-18T11:00:00Z"}},
			}},
			"notes_next_pages": map[string]any{"7": map[string]any{"1": "2"}},
		},
	}}
}

func buildGitLabPullRequestTraversalTrace(t *testing.T, input map[string]any) gitLabPullRequestTraversalTrace {
	t.Helper()
	doer := &gitLabPullRequestTraversalDoer{t: t, input: input}
	claim := nativeTestClaim("gitlab", "prs")
	claim.SinceAt = oracleGitLabPullRequestTraversalTime(t, input, "since")
	claim.BeforeAt = oracleGitLabPullRequestTraversalTime(t, input, "until")
	perPage := input["per_page"].(int)
	normalizedAt := time.Date(2026, 8, 9, 12, 0, 0, 987000000, time.UTC)
	batch, err := (GitLabPullRequestRouteHandler{PerPage: perPage}).Collect(
		context.Background(), claim, providerfoundation.Credential{},
		gitLabRepositoryClient(t, doer, "https://gitlab.test"), normalizedAt,
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(doer.requests) == 0 || batch.Evidence.Requests != len(doer.requests) {
		t.Fatalf("evidence=%+v requests=%d", batch.Evidence, len(doer.requests))
	}
	trace := gitLabPullRequestTraversalTrace{
		ProducerRequests:  make([]string, 0, len(doer.requests)-1),
		UsageRequestCount: batch.Evidence.Requests - 1,
		Rows:              make([]gitLabPullRequestTraversalTraceRow, 0),
	}
	for _, request := range doer.requests[1:] {
		trace.ProducerRequests = append(trace.ProducerRequests, request.URL.RequestURI())
	}
	for _, effect := range batch.Effects {
		if effect.Destination != "git_pull_requests" {
			continue
		}
		for _, raw := range effect.Rows {
			var row pullRequestRow
			if err := json.Unmarshal(raw, &row); err != nil {
				t.Fatal(err)
			}
			var firstReviewAt *string
			if row.FirstReviewAt != nil {
				value := row.FirstReviewAt.UTC().Format(time.RFC3339Nano)
				firstReviewAt = &value
			}
			trace.Rows = append(trace.Rows, gitLabPullRequestTraversalTraceRow{
				Number: row.Number, State: row.State, ReviewsCount: row.ReviewsCount,
				CommentsCount: row.CommentsCount, FirstReviewAt: firstReviewAt,
			})
		}
	}
	return trace
}

func oracleGitLabPullRequestTraversalTime(t *testing.T, input map[string]any, key string) *time.Time {
	t.Helper()
	value, ok := input[key].(string)
	if !ok || value == "" {
		return nil
	}
	parsed, err := time.Parse(time.RFC3339Nano, value)
	if err != nil {
		t.Fatal(err)
	}
	return &parsed
}

func TestGitLabPullRequestTraversalMatchesLivePythonProducer(t *testing.T) {
	compareRowsAgainstPythonOracle(
		t, "gitlab/prs/traversal", oracleGitLabPullRequestTraversalCases(),
		buildGitLabPullRequestTraversalTrace, nil,
	)
}
