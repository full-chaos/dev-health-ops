package flame

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	dhclickhouse "github.com/full-chaos/dev-health-go/clickhouse"
)

// fixtureRowScanner replays a fixed slice of pre-built rows -- same shape
// as drilldown/quadrant's own fixtureRowScanner, extended for this
// package's own destination types.
type fixtureRowScanner struct {
	rows  [][]any
	index int
}

func (s *fixtureRowScanner) Next() bool {
	if s.index >= len(s.rows) {
		return false
	}
	s.index++
	return true
}

func (s *fixtureRowScanner) Scan(dest ...any) error {
	row := s.rows[s.index-1]
	for i, d := range dest {
		switch typed := d.(type) {
		case *string:
			v, _ := row[i].(string)
			*typed = v
		case **string:
			if row[i] == nil {
				*typed = nil
				continue
			}
			v := row[i].(string)
			*typed = &v
		case *time.Time:
			*typed = row[i].(time.Time)
		case **time.Time:
			if row[i] == nil {
				*typed = nil
				continue
			}
			v := row[i].(time.Time)
			*typed = &v
		default:
			return fmt.Errorf("fixtureRowScanner: unsupported dest type %T", d)
		}
	}
	return nil
}

func (s *fixtureRowScanner) Err() error   { return nil }
func (s *fixtureRowScanner) Close() error { return nil }

// fakeQueryClient dispatches on query text, same convention as
// quadrant/drilldown's own fakeQueryClient.
type fakeQueryClient struct {
	t       *testing.T
	handler func(t *testing.T, query string, bindings []dhclickhouse.Binding) (dhclickhouse.RowScanner, error)
}

func (c fakeQueryClient) Query(_ context.Context, query string, bindings []dhclickhouse.Binding) (dhclickhouse.RowScanner, error) {
	return c.handler(c.t, query, bindings)
}

func day(y int, m time.Month, d, hh, mm, ss int) time.Time {
	return time.Date(y, m, d, hh, mm, ss, 0, time.UTC)
}

// loadGolden decodes a testdata JSON file -- captured from the REAL Python
// build_flame_response (via its three per-entity-type callees),
// monkeypatched ClickHouse readers, FlameResponse.model_dump(mode="json"))
// via a one-off `.venv/bin/python` invocation (see this PR's own
// TEST-EVIDENCE for the exact script), THEN hand-adjusted to replace each
// naive Python timestamp with its RFC 3339 form -- the declared,
// already-established divergence this package's own doc comment explains
// (DATETIME WIRE FORM). Every other field is byte-identical to the
// captured Python JSON. DisallowUnknownFields makes a field-name mismatch
// a hard test failure, not a silent drop.
func loadGolden(t *testing.T, name string) Response {
	t.Helper()
	data, err := os.ReadFile("testdata/" + name)
	if err != nil {
		t.Fatalf("read golden %s: %v", name, err)
	}
	var resp Response
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&resp); err != nil {
		t.Fatalf("decode golden %s: %v", name, err)
	}
	return resp
}

func mustMarshal(t *testing.T, v any) string {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	return string(b)
}

const repoID = "11111111-1111-1111-1111-111111111111"

// TestGoldenPRWithRework replays testdata/pr_with_rework.json: a merged PR
// with a first_review_at (Review waiting frame) and one changes_requested
// review producing a Rework loop frame.
func TestGoldenPRWithRework(t *testing.T) {
	client := fakeQueryClient{t: t, handler: func(t *testing.T, query string, bindings []dhclickhouse.Binding) (dhclickhouse.RowScanner, error) {
		switch {
		case strings.Contains(query, "FROM git_pull_requests FINAL"):
			return &fixtureRowScanner{rows: [][]any{{
				"Add retry logic", "merged",
				day(2024, 1, 10, 9, 0, 0), day(2024, 1, 10, 14, 0, 0), day(2024, 1, 11, 15, 30, 0), nil,
			}}}, nil
		case strings.Contains(query, "FROM git_pull_request_reviews FINAL"):
			return &fixtureRowScanner{rows: [][]any{
				{"changes_requested", day(2024, 1, 10, 16, 0, 0)},
				{"approved", day(2024, 1, 11, 10, 0, 0)},
			}}, nil
		}
		t.Fatalf("unexpected query for pr_with_rework fixture:\n%s", query)
		return nil, nil
	}}

	got, err := BuildResponse(context.Background(), client, "org-1", Params{EntityType: "pr", EntityID: repoID + ":42"})
	if err != nil {
		t.Fatalf("BuildResponse: %v", err)
	}
	want := loadGolden(t, "pr_with_rework.json")
	if gotJSON, wantJSON := mustMarshal(t, got), mustMarshal(t, want); gotJSON != wantJSON {
		t.Fatalf("response mismatch\n got:  %s\nwant: %s", gotJSON, wantJSON)
	}
}

// TestGoldenPRNoReview replays testdata/pr_no_review.json: a merged PR
// with no first_review_at (no wait frame) and no reviews at all (no rework
// windows), title NULL.
func TestGoldenPRNoReview(t *testing.T) {
	client := fakeQueryClient{t: t, handler: func(t *testing.T, query string, bindings []dhclickhouse.Binding) (dhclickhouse.RowScanner, error) {
		switch {
		case strings.Contains(query, "FROM git_pull_requests FINAL"):
			return &fixtureRowScanner{rows: [][]any{{
				nil, "merged",
				day(2024, 1, 5, 9, 0, 0), nil, day(2024, 1, 5, 20, 0, 0), nil,
			}}}, nil
		case strings.Contains(query, "FROM git_pull_request_reviews FINAL"):
			return &fixtureRowScanner{}, nil
		}
		t.Fatalf("unexpected query for pr_no_review fixture:\n%s", query)
		return nil, nil
	}}

	got, err := BuildResponse(context.Background(), client, "org-1", Params{EntityType: "pr", EntityID: repoID + ":7"})
	if err != nil {
		t.Fatalf("BuildResponse: %v", err)
	}
	want := loadGolden(t, "pr_no_review.json")
	if gotJSON, wantJSON := mustMarshal(t, got), mustMarshal(t, want); gotJSON != wantJSON {
		t.Fatalf("response mismatch\n got:  %s\nwant: %s", gotJSON, wantJSON)
	}
}

// TestGoldenIssueWithBacklogWait replays testdata/issue_with_backlog_wait.json:
// started_at > created_at produces a Backlog waiting frame.
func TestGoldenIssueWithBacklogWait(t *testing.T) {
	client := fakeQueryClient{t: t, handler: func(t *testing.T, query string, bindings []dhclickhouse.Binding) (dhclickhouse.RowScanner, error) {
		if !strings.Contains(query, "FROM work_item_cycle_times FINAL") {
			t.Fatalf("unexpected query for issue_with_backlog_wait fixture:\n%s", query)
		}
		return &fixtureRowScanner{rows: [][]any{{
			"github", "bug", "done",
			day(2024, 1, 5, 8, 0, 0), day(2024, 1, 6, 9, 0, 0), day(2024, 1, 9, 17, 0, 0),
		}}}, nil
	}}

	got, err := BuildResponse(context.Background(), client, "org-1", Params{EntityType: "issue", EntityID: "issue-123"})
	if err != nil {
		t.Fatalf("BuildResponse: %v", err)
	}
	want := loadGolden(t, "issue_with_backlog_wait.json")
	if gotJSON, wantJSON := mustMarshal(t, got), mustMarshal(t, want); gotJSON != wantJSON {
		t.Fatalf("response mismatch\n got:  %s\nwant: %s", gotJSON, wantJSON)
	}
}

// TestGoldenIssueNoBacklogWait replays testdata/issue_no_backlog_wait.json:
// started_at == created_at, no wait frame.
func TestGoldenIssueNoBacklogWait(t *testing.T) {
	client := fakeQueryClient{t: t, handler: func(t *testing.T, query string, bindings []dhclickhouse.Binding) (dhclickhouse.RowScanner, error) {
		if !strings.Contains(query, "FROM work_item_cycle_times FINAL") {
			t.Fatalf("unexpected query for issue_no_backlog_wait fixture:\n%s", query)
		}
		return &fixtureRowScanner{rows: [][]any{{
			"jira", "task", "in_progress",
			day(2024, 1, 5, 8, 0, 0), day(2024, 1, 5, 8, 0, 0), day(2024, 1, 5, 18, 0, 0),
		}}}, nil
	}}

	got, err := BuildResponse(context.Background(), client, "org-1", Params{EntityType: "issue", EntityID: "issue-456"})
	if err != nil {
		t.Fatalf("BuildResponse: %v", err)
	}
	want := loadGolden(t, "issue_no_backlog_wait.json")
	if gotJSON, wantJSON := mustMarshal(t, got), mustMarshal(t, want); gotJSON != wantJSON {
		t.Fatalf("response mismatch\n got:  %s\nwant: %s", gotJSON, wantJSON)
	}
}

// TestGoldenDeploymentWithQueueAndDeploy replays
// testdata/deployment_with_queue_and_deploy.json: merged_at before
// started_at (Queue waiting frame) and deployed_at strictly between the
// pipeline's own start/end (Deploy sub-frame).
func TestGoldenDeploymentWithQueueAndDeploy(t *testing.T) {
	client := fakeQueryClient{t: t, handler: func(t *testing.T, query string, bindings []dhclickhouse.Binding) (dhclickhouse.RowScanner, error) {
		if !strings.Contains(query, "FROM deployments FINAL") {
			t.Fatalf("unexpected query for deployment_with_queue_and_deploy fixture:\n%s", query)
		}
		return &fixtureRowScanner{rows: [][]any{{
			"success", "production",
			day(2024, 1, 12, 1, 0, 0), day(2024, 1, 12, 1, 20, 0), day(2024, 1, 12, 1, 15, 0), day(2024, 1, 11, 23, 0, 0),
		}}}, nil
	}}

	got, err := BuildResponse(context.Background(), client, "org-1", Params{EntityType: "deployment", EntityID: repoID + ":deploy-1"})
	if err != nil {
		t.Fatalf("BuildResponse: %v", err)
	}
	want := loadGolden(t, "deployment_with_queue_and_deploy.json")
	if gotJSON, wantJSON := mustMarshal(t, got), mustMarshal(t, want); gotJSON != wantJSON {
		t.Fatalf("response mismatch\n got:  %s\nwant: %s", gotJSON, wantJSON)
	}
}

// TestGoldenDeploymentPipelineOnly replays
// testdata/deployment_pipeline_only.json: merged_at AFTER started_at (no
// queue frame) and deployed_at NULL (no deploy sub-frame).
func TestGoldenDeploymentPipelineOnly(t *testing.T) {
	client := fakeQueryClient{t: t, handler: func(t *testing.T, query string, bindings []dhclickhouse.Binding) (dhclickhouse.RowScanner, error) {
		if !strings.Contains(query, "FROM deployments FINAL") {
			t.Fatalf("unexpected query for deployment_pipeline_only fixture:\n%s", query)
		}
		return &fixtureRowScanner{rows: [][]any{{
			"failure", "staging",
			day(2024, 1, 13, 2, 0, 0), day(2024, 1, 13, 2, 5, 0), nil, day(2024, 1, 13, 1, 0, 0),
		}}}, nil
	}}

	got, err := BuildResponse(context.Background(), client, "org-1", Params{EntityType: "deployment", EntityID: repoID + ":deploy-2"})
	if err != nil {
		t.Fatalf("BuildResponse: %v", err)
	}
	want := loadGolden(t, "deployment_pipeline_only.json")
	if gotJSON, wantJSON := mustMarshal(t, got), mustMarshal(t, want); gotJSON != wantJSON {
		t.Fatalf("response mismatch\n got:  %s\nwant: %s", gotJSON, wantJSON)
	}
}

// TestBuildResponseUnknownEntityTypeIs404 pins the final fallthrough
// (services/flame.py:426).
func TestBuildResponseUnknownEntityTypeIs404(t *testing.T) {
	client := fakeQueryClient{t: t, handler: func(t *testing.T, query string, bindings []dhclickhouse.Binding) (dhclickhouse.RowScanner, error) {
		t.Fatalf("unexpected query for unknown entity_type: %s", query)
		return nil, nil
	}}
	_, err := BuildResponse(context.Background(), client, "org-1", Params{EntityType: "bogus", EntityID: "whatever"})
	reqErr, ok := AsRequestError(err)
	if !ok || reqErr.Status != 404 || reqErr.Message != "Unknown entity type" {
		t.Fatalf("err = %v, want *RequestError{404, \"Unknown entity type\"}", err)
	}
}

// TestBuildResponsePRMissingRepoPrefixIs400 pins _parse_repo_entity's
// missing-separator branch (services/flame.py:52-55) -- no ClickHouse call
// happens.
func TestBuildResponsePRMissingRepoPrefixIs400(t *testing.T) {
	client := fakeQueryClient{t: t, handler: func(t *testing.T, query string, bindings []dhclickhouse.Binding) (dhclickhouse.RowScanner, error) {
		t.Fatalf("unexpected query: %s", query)
		return nil, nil
	}}
	_, err := BuildResponse(context.Background(), client, "org-1", Params{EntityType: "pr", EntityID: "not-a-prefixed-id"})
	reqErr, ok := AsRequestError(err)
	if !ok || reqErr.Status != 400 || reqErr.Message != "Entity id must include repo_id prefix" {
		t.Fatalf("err = %v, want *RequestError{400, \"Entity id must include repo_id prefix\"}", err)
	}
}

// TestBuildResponsePRInvalidRepoUUIDIs400 pins _parse_repo_entity's
// invalid-UUID branch (services/flame.py:57-58).
func TestBuildResponsePRInvalidRepoUUIDIs400(t *testing.T) {
	client := fakeQueryClient{t: t, handler: func(t *testing.T, query string, bindings []dhclickhouse.Binding) (dhclickhouse.RowScanner, error) {
		t.Fatalf("unexpected query: %s", query)
		return nil, nil
	}}
	_, err := BuildResponse(context.Background(), client, "org-1", Params{EntityType: "pr", EntityID: "not-a-uuid:5"})
	reqErr, ok := AsRequestError(err)
	if !ok || reqErr.Status != 400 || reqErr.Message != "Invalid repo id" {
		t.Fatalf("err = %v, want *RequestError{400, \"Invalid repo id\"}", err)
	}
}

// TestBuildResponsePRNonNumericSuffixIs400 pins build_flame_response's
// `int(suffix)` guard (services/flame.py:373-378) -- reached only after
// the repo id itself validates, and answers before any ClickHouse call.
func TestBuildResponsePRNonNumericSuffixIs400(t *testing.T) {
	client := fakeQueryClient{t: t, handler: func(t *testing.T, query string, bindings []dhclickhouse.Binding) (dhclickhouse.RowScanner, error) {
		t.Fatalf("unexpected query: %s", query)
		return nil, nil
	}}
	_, err := BuildResponse(context.Background(), client, "org-1", Params{EntityType: "pr", EntityID: repoID + ":abc"})
	reqErr, ok := AsRequestError(err)
	if !ok || reqErr.Status != 400 || reqErr.Message != "PR id must be numeric" {
		t.Fatalf("err = %v, want *RequestError{400, \"PR id must be numeric\"}", err)
	}
}

// TestBuildResponsePRNotFoundIs404 pins build_flame_response's `if not pr:
// raise HTTPException(404, "PR not found")` (services/flame.py:386-387).
func TestBuildResponsePRNotFoundIs404(t *testing.T) {
	client := fakeQueryClient{t: t, handler: func(t *testing.T, query string, bindings []dhclickhouse.Binding) (dhclickhouse.RowScanner, error) {
		return &fixtureRowScanner{}, nil
	}}
	_, err := BuildResponse(context.Background(), client, "org-1", Params{EntityType: "pr", EntityID: repoID + ":99"})
	reqErr, ok := AsRequestError(err)
	if !ok || reqErr.Status != 404 || reqErr.Message != "PR not found" {
		t.Fatalf("err = %v, want *RequestError{404, \"PR not found\"}", err)
	}
}

// TestBuildResponseIssueNotFoundIs404 pins the issue branch's own 404
// (services/flame.py:404-405).
func TestBuildResponseIssueNotFoundIs404(t *testing.T) {
	client := fakeQueryClient{t: t, handler: func(t *testing.T, query string, bindings []dhclickhouse.Binding) (dhclickhouse.RowScanner, error) {
		return &fixtureRowScanner{}, nil
	}}
	_, err := BuildResponse(context.Background(), client, "org-1", Params{EntityType: "issue", EntityID: "missing-issue"})
	reqErr, ok := AsRequestError(err)
	if !ok || reqErr.Status != 404 || reqErr.Message != "Issue not found" {
		t.Fatalf("err = %v, want *RequestError{404, \"Issue not found\"}", err)
	}
}

// TestBuildResponseDeploymentNotFoundIs404 pins the deployment branch's
// own 404 (services/flame.py:417-418).
func TestBuildResponseDeploymentNotFoundIs404(t *testing.T) {
	client := fakeQueryClient{t: t, handler: func(t *testing.T, query string, bindings []dhclickhouse.Binding) (dhclickhouse.RowScanner, error) {
		return &fixtureRowScanner{}, nil
	}}
	_, err := BuildResponse(context.Background(), client, "org-1", Params{EntityType: "deployment", EntityID: repoID + ":missing"})
	reqErr, ok := AsRequestError(err)
	if !ok || reqErr.Status != 404 || reqErr.Message != "Deployment not found" {
		t.Fatalf("err = %v, want *RequestError{404, \"Deployment not found\"}", err)
	}
}

// TestBuildResponseDeploymentTimelineUnavailableIs404 pins
// _build_deployment_flame_response's own `if start is None` guard
// (services/flame.py:288-289): every one of started_at/merged_at/
// deployed_at is NULL.
func TestBuildResponseDeploymentTimelineUnavailableIs404(t *testing.T) {
	client := fakeQueryClient{t: t, handler: func(t *testing.T, query string, bindings []dhclickhouse.Binding) (dhclickhouse.RowScanner, error) {
		return &fixtureRowScanner{rows: [][]any{{
			"pending", "production", nil, nil, nil, nil,
		}}}, nil
	}}
	_, err := BuildResponse(context.Background(), client, "org-1", Params{EntityType: "deployment", EntityID: repoID + ":deploy-3"})
	reqErr, ok := AsRequestError(err)
	if !ok || reqErr.Status != 404 || reqErr.Message != "Deployment timeline unavailable" {
		t.Fatalf("err = %v, want *RequestError{404, \"Deployment timeline unavailable\"}", err)
	}
}

// TestBuildResponseIssueTimelineUsesNowFallback pins _now_like's fallback
// (services/flame.py:19-23, reached when completed_at is NULL): the
// package-level `now` var is overridden so the fallback end is a fixed,
// asserted value rather than merely "close to real time".
func TestBuildResponseIssueTimelineUsesNowFallback(t *testing.T) {
	fixedNow := day(2026, 9, 16, 12, 0, 0)
	original := now
	now = func() time.Time { return fixedNow }
	defer func() { now = original }()

	client := fakeQueryClient{t: t, handler: func(t *testing.T, query string, bindings []dhclickhouse.Binding) (dhclickhouse.RowScanner, error) {
		return &fixtureRowScanner{rows: [][]any{{
			"github", "bug", "in_progress",
			day(2024, 1, 5, 8, 0, 0), day(2024, 1, 6, 9, 0, 0), nil,
		}}}, nil
	}}

	got, err := BuildResponse(context.Background(), client, "org-1", Params{EntityType: "issue", EntityID: "issue-789"})
	if err != nil {
		t.Fatalf("BuildResponse: %v", err)
	}
	if !got.Timeline.End.Equal(fixedNow) {
		t.Fatalf("Timeline.End = %v, want the overridden now() = %v", got.Timeline.End, fixedNow)
	}
}

// TestIssueEntityNeverCarriesTeamAttributionFields pins the OTHER half of
// the dead-join simplification (see sqlshape_test.go's
// TestFetchIssueQueryOmitsTheDeadTeamAttributionJoin for the query-shape
// half): even if a future edit's fixture row carried a team_id-shaped
// value, buildIssueFlameResponse has no field to read it from --
// issueRow itself declares no TeamID/WorkScopeID member -- so the
// response entity is exactly the four keys _build_issue_flame_response
// (services/flame.py:268-273) actually sets, never more. This is a
// structural guarantee (the type has no such field to leak), and this
// test pins the OBSERVABLE half of that guarantee: the wire shape.
func TestIssueEntityNeverCarriesTeamAttributionFields(t *testing.T) {
	client := fakeQueryClient{t: t, handler: func(t *testing.T, query string, bindings []dhclickhouse.Binding) (dhclickhouse.RowScanner, error) {
		if !strings.Contains(query, "FROM work_item_cycle_times FINAL") {
			t.Fatalf("unexpected query: %s", query)
		}
		return &fixtureRowScanner{rows: [][]any{{
			"github", "bug", "done",
			day(2024, 1, 5, 8, 0, 0), day(2024, 1, 6, 9, 0, 0), day(2024, 1, 9, 17, 0, 0),
		}}}, nil
	}}

	got, err := BuildResponse(context.Background(), client, "org-1", Params{EntityType: "issue", EntityID: "issue-123"})
	if err != nil {
		t.Fatalf("BuildResponse: %v", err)
	}
	wantKeys := map[string]bool{"work_item_id": true, "provider": true, "type": true, "status": true}
	if got.Entity.Len() != len(wantKeys) {
		t.Fatalf("entity has %d keys, want %d: %+v", got.Entity.Len(), len(wantKeys), got.Entity.Keys())
	}
	for key := range got.Entity.All() {
		if !wantKeys[key] {
			t.Fatalf("entity carries unexpected key %q (team-attribution leak?): %+v", key, got.Entity.Keys())
		}
	}
	if _, ok := got.Entity.Get("team_id"); ok {
		t.Fatal("entity must never carry team_id -- the team-attribution join is dead code and is not ported")
	}
	if _, ok := got.Entity.Get("work_scope_id"); ok {
		t.Fatal("entity must never carry work_scope_id -- it is never read by _build_issue_flame_response")
	}
}
