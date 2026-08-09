package providersync

import (
	"context"
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
)

var oracleGitLabDeploymentsNormalizedAt = time.Date(2026, 8, 9, 12, 0, 0, 0, time.UTC)

var oracleGitLabDeploymentsGoOnlyFields = map[string]string{
	"org_id":      "carried from the Go claim to keep ClickHouse writes tenant-scoped",
	"last_synced": "stamped from normalizedAt by the Go complete-route handler",
}

func decodeGitLabDeploymentOracleObject(t *testing.T, value any) map[string]any {
	t.Helper()
	encoded, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	object, include, err := decodeGitLabDeploymentObject(encoded)
	if err != nil {
		t.Fatal(err)
	}
	if !include {
		t.Fatalf("oracle input %T is not a GitLab JSON object", value)
	}
	return object
}

func buildGitLabDeploymentRowForOracle(t *testing.T, input map[string]any) deploymentRow {
	t.Helper()
	payload := decodeGitLabDeploymentOracleObject(t, input["raw_deployment"])
	releaseValues, ok := input["releases"].([]map[string]any)
	if !ok {
		t.Fatalf("releases=%T", input["releases"])
	}
	releases := make([]map[string]any, 0, len(releaseValues))
	for _, release := range releaseValues {
		releases = append(releases, decodeGitLabDeploymentOracleObject(t, release))
	}
	row, include, err := normalizeGitLabDeployment(
		nativeTestClaim("gitlab", "deployments"), input["repo_id"].(string), payload,
		releases, oracleGitLabDeploymentsNormalizedAt,
	)
	if err != nil {
		t.Fatal(err)
	}
	if !include {
		t.Fatal("oracle deployment did not produce a row")
	}
	mergeRequestsBySHA, ok := input["merge_requests_by_sha"].(map[string][]map[string]any)
	if !ok {
		t.Fatalf("merge_requests_by_sha=%T", input["merge_requests_by_sha"])
	}
	mergeRequests := make([]map[string]any, 0)
	for _, value := range mergeRequestsBySHA[stringValue(payload["sha"])] {
		mergeRequests = append(mergeRequests, decodeGitLabDeploymentOracleObject(t, value))
	}
	row.PullRequestNumber, row.MergedAt = resolveGitLabDeploymentMergeRequest(mergeRequests)
	return row
}

func oracleGitLabDeploymentCases() []oracleCase {
	return []oracleCase{
		{
			ID: "release_match_prefers_first_merged_mr",
			Input: map[string]any{
				"repo_id":         "c7198fbc-1945-3717-05d8-eb78866b4e79",
				"max_deployments": 1000,
				"since":           "2026-07-01T00:00:00Z",
				"releases":        []map[string]any{{"tag_name": "v1.2.3"}},
				"raw_deployment": map[string]any{
					"id": 501, "iid": 7, "status": "success",
					"environment": map[string]any{"name": "production"},
					"created_at":  "2026-07-22T10:00:00Z", "finished_at": "2026-07-22T10:05:00Z",
					"sha": "abc", "ref": "v1.2.3",
				},
				"deployments": []map[string]any{{
					"id": 501, "iid": 7, "status": "success",
					"environment": map[string]any{"name": "production"},
					"created_at":  "2026-07-22T10:00:00Z", "finished_at": "2026-07-22T10:05:00Z",
					"sha": "abc", "ref": "v1.2.3",
				}},
				"merge_requests_by_sha": map[string][]map[string]any{
					"abc": {
						{"iid": 44, "state": "opened", "merged_at": ""},
						{"iid": 45, "state": "merged", "merged_at": "2026-07-21T10:00:00Z"},
					},
				},
			},
		},
		{
			ID: "explicit_nested_release_ref_clamps_confidence",
			Input: map[string]any{
				"repo_id":         "c7198fbc-1945-3717-05d8-eb78866b4e79",
				"max_deployments": 1000,
				"since":           "2026-07-01T00:00:00Z",
				"releases":        []map[string]any{},
				"raw_deployment": map[string]any{
					"id": "502", "iid": 8, "status": "pending", "environment": nil,
					"created_at": "2026-07-22T11:00:00Z", "finished_at": nil,
					"payload": map[string]any{
						"release_ref": "release-from-payload", "release_ref_confidence": 1.5,
					},
				},
				"deployments": []map[string]any{{
					"id": "502", "iid": 8, "status": "pending", "environment": nil,
					"created_at": "2026-07-22T11:00:00Z", "finished_at": nil,
					"payload": map[string]any{
						"release_ref": "release-from-payload", "release_ref_confidence": 1.5,
					},
				}},
				"merge_requests_by_sha": map[string][]map[string]any{},
			},
		},
		{
			ID: "iid_fallback_keeps_first_nonmerged_mr",
			Input: map[string]any{
				"repo_id":         "c7198fbc-1945-3717-05d8-eb78866b4e79",
				"max_deployments": 1000,
				"since":           "2026-07-01T00:00:00Z",
				"releases":        []map[string]any{},
				"raw_deployment": map[string]any{
					"id": 503, "iid": "nine", "status": nil,
					"environment": map[string]any{"name": "staging"},
					"created_at":  "2026-07-22T12:00:00Z", "sha": "def",
				},
				"deployments": []map[string]any{{
					"id": 503, "iid": "nine", "status": nil,
					"environment": map[string]any{"name": "staging"},
					"created_at":  "2026-07-22T12:00:00Z", "sha": "def",
				}},
				"merge_requests_by_sha": map[string][]map[string]any{
					"def": {{"iid": 88, "state": "opened", "merged_at": "2026-07-21T12:00:00Z"}},
				},
			},
		},
	}
}

func TestGenericOracleMatchesLivePythonForGitLabDeploymentRowConstruction(t *testing.T) {
	compareRowsAgainstPythonOracle(
		t,
		"gitlab/deployments/row",
		oracleGitLabDeploymentCases(),
		buildGitLabDeploymentRowForOracle,
		oracleGitLabDeploymentsGoOnlyFields,
	)
}

type gitLabDeploymentTraceRow struct {
	DeploymentID         string  `json:"deployment_id"`
	ReleaseRef           string  `json:"release_ref"`
	ReleaseRefConfidence float64 `json:"release_ref_confidence"`
	PullRequestNumber    *int    `json:"pull_request_number"`
}

type gitLabDeploymentTraversalTrace struct {
	ProducerRequests  []string                   `json:"producer_requests"`
	UsageRequestCount int                        `json:"usage_request_count"`
	Rows              []gitLabDeploymentTraceRow `json:"rows"`
}

func oracleGitLabDeploymentTraversalCases() []oracleCase {
	return []oracleCase{
		{
			ID: "single_page_window_and_all_mr_requests",
			Input: map[string]any{
				"repo_id":              "c7198fbc-1945-3717-05d8-eb78866b4e79",
				"max_deployments":      1000,
				"since":                "2026-07-01T00:00:00Z",
				"until":                "2026-07-31T23:59:59Z",
				"releases":             []any{map[string]any{"tag_name": "v1.2.3"}},
				"release_next_page":    "2",
				"deployment_next_page": "2",
				"deployments": []any{
					map[string]any{"id": 501, "iid": 7, "created_at": "2026-08-01T10:00:00Z", "sha": "future", "ref": "future"},
					map[string]any{"id": 502, "iid": 8, "created_at": "2026-07-22T10:00:00Z", "sha": "main", "ref": "v1.2.3"},
					map[string]any{"id": 503, "iid": 9, "created_at": "2026-06-30T10:00:00Z", "sha": "old", "ref": "old"},
				},
				"merge_requests_by_sha": map[string]any{
					"future": []any{map[string]any{"iid": 11, "state": "merged", "merged_at": "2026-08-01T09:00:00Z"}},
					"main": []any{
						map[string]any{"iid": 44, "state": "opened", "merged_at": ""},
						map[string]any{"iid": 45, "state": "merged", "merged_at": "2026-07-21T10:00:00Z"},
					},
					"old": []any{map[string]any{"iid": 99, "state": "merged", "merged_at": "2026-06-30T09:00:00Z"}},
				},
			},
		},
		{
			ID: "cap_one_page_ignores_next_header",
			Input: map[string]any{
				"repo_id":              "c7198fbc-1945-3717-05d8-eb78866b4e79",
				"max_deployments":      1,
				"since":                "2026-07-01T00:00:00Z",
				"until":                "2026-07-31T23:59:59Z",
				"releases":             []any{},
				"release_next_page":    "2",
				"deployment_next_page": "2",
				"deployments": []any{
					map[string]any{"id": 601, "iid": 61, "created_at": "2026-07-22T10:00:00Z", "sha": "first"},
					map[string]any{"id": 602, "iid": 62, "created_at": "2026-07-21T10:00:00Z", "sha": "second"},
				},
				"merge_requests_by_sha": map[string]any{
					"first":  []any{map[string]any{"iid": 61, "state": "merged", "merged_at": "2026-07-21T10:00:00Z"}},
					"second": []any{map[string]any{"iid": 62, "state": "merged", "merged_at": "2026-07-20T10:00:00Z"}},
				},
			},
		},
		{
			ID: "non_object_is_skipped_and_later_rows_continue",
			Input: map[string]any{
				"repo_id":         "c7198fbc-1945-3717-05d8-eb78866b4e79",
				"max_deployments": 1000,
				"since":           "2026-07-01T00:00:00Z",
				"until":           "2026-07-31T23:59:59Z",
				"releases":        []any{},
				"deployments": []any{
					map[string]any{"id": 701, "iid": 71, "created_at": "2026-07-22T10:00:00Z", "sha": "first"},
					"not-an-object",
					map[string]any{"id": 702, "iid": 72, "created_at": "2026-07-21T10:00:00Z", "sha": "second"},
				},
				"merge_requests_by_sha": map[string]any{
					"first":  []any{map[string]any{"iid": 71, "state": "merged", "merged_at": "2026-07-21T10:00:00Z"}},
					"second": []any{map[string]any{"iid": 72, "state": "merged", "merged_at": "2026-07-20T10:00:00Z"}},
				},
			},
		},
		{
			ID: "core_error_is_empty_success",
			Input: map[string]any{
				"repo_id":           "c7198fbc-1945-3717-05d8-eb78866b4e79",
				"max_deployments":   1000,
				"since":             "2026-07-01T00:00:00Z",
				"until":             "2026-07-31T23:59:59Z",
				"releases":          []any{},
				"deployments":       []any{},
				"deployment_status": http.StatusBadRequest,
			},
		},
		{
			ID: "release_and_mr_errors_are_best_effort",
			Input: map[string]any{
				"repo_id":                     "c7198fbc-1945-3717-05d8-eb78866b4e79",
				"max_deployments":             1000,
				"since":                       "2026-07-01T00:00:00Z",
				"until":                       "2026-07-31T23:59:59Z",
				"releases":                    []any{},
				"release_status":              http.StatusBadRequest,
				"deployments":                 []any{map[string]any{"id": 801, "iid": 81, "created_at": "2026-07-22T10:00:00Z", "sha": "unavailable"}},
				"merge_requests_by_sha":       map[string]any{"unavailable": []any{}},
				"merge_request_status_by_sha": map[string]any{"unavailable": http.StatusBadRequest},
			},
		},
	}
}

func TestGenericOracleMatchesLivePythonForGitLabDeploymentTraversal(t *testing.T) {
	compareRowsAgainstPythonOracle(
		t,
		"gitlab/deployments/trace",
		oracleGitLabDeploymentTraversalCases(),
		buildGitLabDeploymentTraversalTrace,
		nil,
	)
}

func buildGitLabDeploymentTraversalTrace(t *testing.T, input map[string]any) gitLabDeploymentTraversalTrace {
	t.Helper()
	doer := &gitLabDeploymentsDoer{t: t, responses: gitLabDeploymentTraceResponses(t, input)}
	claim := nativeTestClaim("gitlab", "deployments")
	claim.SinceAt = oracleGitLabDeploymentTraceTime(t, input, "since")
	claim.BeforeAt = oracleGitLabDeploymentTraceTime(t, input, "until")
	batch, err := (GitLabDeploymentsRouteHandler{MaxDeployments: input["max_deployments"].(int)}).Collect(
		context.Background(), claim, providerfoundation.Credential{},
		gitLabRepositoryClient(t, doer, "https://gitlab.test"), oracleGitLabDeploymentsNormalizedAt,
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(doer.requests) < 1 || batch.Evidence.Requests != len(doer.requests) {
		t.Fatalf("physical evidence=%+v requests=%d", batch.Evidence, len(doer.requests))
	}
	trace := gitLabDeploymentTraversalTrace{
		ProducerRequests:  make([]string, 0, len(doer.requests)-1),
		UsageRequestCount: batch.Evidence.Requests - 1,
		Rows:              make([]gitLabDeploymentTraceRow, 0),
	}
	for _, request := range doer.requests[1:] {
		trace.ProducerRequests = append(trace.ProducerRequests, request.URL.RequestURI())
	}
	for _, effect := range batch.Effects {
		if effect.Destination != "deployments" {
			continue
		}
		for _, raw := range effect.Rows {
			var row deploymentRow
			if err := json.Unmarshal(raw, &row); err != nil {
				t.Fatal(err)
			}
			trace.Rows = append(trace.Rows, gitLabDeploymentTraceRow{
				DeploymentID:         row.DeploymentID,
				ReleaseRef:           row.ReleaseRef,
				ReleaseRefConfidence: row.ReleaseRefConfidence,
				PullRequestNumber:    row.PullRequestNumber,
			})
		}
	}
	return trace
}

func gitLabDeploymentTraceResponses(t *testing.T, input map[string]any) []gitLabDeploymentsResponse {
	t.Helper()
	responses := []gitLabDeploymentsResponse{
		{body: gitLabRepositoryFixture},
		{
			status:  oracleGitLabDeploymentTraceStatus(t, input, "release", ""),
			body:    oracleGitLabDeploymentTraceJSON(t, input["releases"]),
			headers: oracleGitLabDeploymentTraceHeaders(input, "release"),
		},
		{
			status:  oracleGitLabDeploymentTraceStatus(t, input, "deployment", ""),
			body:    oracleGitLabDeploymentTraceJSON(t, input["deployments"]),
			headers: oracleGitLabDeploymentTraceHeaders(input, "deployment"),
		},
	}
	if oracleGitLabDeploymentTraceStatus(t, input, "deployment", "") >= http.StatusBadRequest {
		return responses
	}
	deployments, ok := input["deployments"].([]any)
	if !ok {
		t.Fatalf("deployments=%T", input["deployments"])
	}
	maxDeployments := input["max_deployments"].(int)
	seen := 0
	for _, value := range deployments {
		deployment, object := value.(map[string]any)
		if !object {
			continue
		}
		if seen >= maxDeployments {
			break
		}
		seen++
		sha := stringValue(deployment["sha"])
		if sha == "" {
			continue
		}
		responses = append(responses, gitLabDeploymentsResponse{
			status:  oracleGitLabDeploymentTraceStatus(t, input, "merge_request", sha),
			body:    oracleGitLabDeploymentTraceJSON(t, oracleGitLabDeploymentTraceMergeRequests(input, sha)),
			headers: oracleGitLabDeploymentTraceHeaders(input, "merge_request:"+sha),
		})
	}
	return responses
}

func oracleGitLabDeploymentTraceJSON(t *testing.T, value any) string {
	t.Helper()
	encoded, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return string(encoded)
}

func oracleGitLabDeploymentTraceTime(t *testing.T, input map[string]any, key string) *time.Time {
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

func oracleGitLabDeploymentTraceStatus(t *testing.T, input map[string]any, endpoint, sha string) int {
	t.Helper()
	if endpoint == "merge_request" {
		if values, ok := input["merge_request_status_by_sha"].(map[string]any); ok {
			if value, exists := values[sha]; exists {
				status, ok := value.(int)
				if !ok {
					t.Fatalf("merge_request_status_by_sha[%q]=%T", sha, value)
				}
				return status
			}
		}
		return http.StatusOK
	}
	value, exists := input[endpoint+"_status"]
	if !exists {
		return http.StatusOK
	}
	status, ok := value.(int)
	if !ok {
		t.Fatalf("%s_status=%T", endpoint, value)
	}
	return status
}

func oracleGitLabDeploymentTraceHeaders(input map[string]any, endpoint string) http.Header {
	value, ok := input[endpoint+"_next_page"].(string)
	if !ok || value == "" {
		return nil
	}
	return http.Header{"X-Next-Page": []string{value}}
}

func oracleGitLabDeploymentTraceMergeRequests(input map[string]any, sha string) any {
	values, ok := input["merge_requests_by_sha"].(map[string]any)
	if !ok {
		return []any{}
	}
	if value, exists := values[sha]; exists {
		return value
	}
	return []any{}
}
