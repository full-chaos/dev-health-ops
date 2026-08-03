package providersync

import (
	"context"
	"encoding/json"
	"net/url"
	"sort"
	"strconv"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
)

type gitLabIncidentTraversalCall struct {
	ProjectID     int    `json:"project_id"`
	IssueType     string `json:"issue_type"`
	UpdatedAfter  string `json:"updated_after"`
	UpdatedBefore string `json:"updated_before"`
	State         string `json:"state"`
	Page          int    `json:"page"`
	PerPage       int    `json:"per_page"`
	OrderBy       string `json:"order_by"`
	Sort          string `json:"sort"`
}

type gitLabIncidentTraversalTrace struct {
	Calls               []gitLabIncidentTraversalCall `json:"calls"`
	IncidentExternalIDs []string                      `json:"incident_external_ids"`
}

func oracleGitLabIncidentCases() []oracleCase {
	return []oracleCase{
		{ID: "opened_sev1", Input: map[string]any{
			"org_id": "org-acme", "project_id": 123,
			"repo_id":        "c7198fbc-1945-3717-05d8-eb78866b4e79",
			"repo_full_name": "Acme/API", "provider_instance_id": "https://GITLAB.example:443",
			"since": "2026-07-01T00:00:00Z", "until": "2026-07-31T23:59:59Z",
			"normalized_at": "2026-08-03T12:00:00.123456Z",
			"pages": []any{[]any{map[string]any{
				"id": 9001, "iid": 7, "issue_type": "incident", "state": "opened",
				"title": "API <café &> unavailable", "description": "edge down",
				"created_at": "2026-07-20T10:00:00.123456Z",
				"updated_at": "2026-07-21T11:00:00.654321Z",
				"web_url":    "https://gitlab.example/Acme/API/-/issues/7", "severity": "sev-1",
			}}},
		}},
		{ID: "closed_nullable", Input: map[string]any{
			"org_id": "org-acme", "project_id": 123,
			"repo_id":        "c7198fbc-1945-3717-05d8-eb78866b4e79",
			"repo_full_name": "Acme/API", "provider_instance_id": "https://gitlab.example",
			"since": "2026-07-01T00:00:00Z", "until": "2026-07-31T23:59:59Z",
			"normalized_at": "2026-08-03T12:00:00Z",
			"pages": []any{[]any{map[string]any{
				"id": "9002", "issue_type": "INCIDENT", "state": "closed",
				"title": nil, "description": nil, "created_at": "2026-07-22T10:00:00Z",
				"updated_at": nil, "closed_at": "2026-07-23T11:00:00Z",
				"url": "https://gitlab.example/fallback", "severity": "unknown",
			}}},
		}},
	}
}

func oracleGitLabIncidentTraversalCases() []oracleCase {
	return []oracleCase{{ID: "multi_page_filter_and_dedup", Input: map[string]any{
		"org_id": "org-acme", "project_id": 123,
		"repo_id":        "c7198fbc-1945-3717-05d8-eb78866b4e79",
		"repo_full_name": "Acme/API", "provider_instance_id": "https://gitlab.example",
		"since": "2026-07-01T00:00:00Z", "until": "2026-07-31T23:59:59Z",
		"normalized_at": "2026-08-03T12:00:00Z", "max_issues": 3,
		"pages": []any{
			[]any{
				map[string]any{"id": 9003, "iid": 9, "issue_type": "incident", "state": "opened", "created_at": "2026-07-20T10:00:00Z"},
				map[string]any{"id": 9003, "iid": 9, "issue_type": "incident", "state": "opened", "created_at": "2026-07-20T10:00:00Z"},
				map[string]any{"id": 9005, "iid": 11, "issue_type": "incident", "state": "opened", "created_at": "2026-07-20T11:00:00Z"},
			},
			[]any{map[string]any{"id": 9004, "iid": 10, "issue_type": "INCIDENT", "state": "closed", "created_at": "2026-07-21T10:00:00Z"}},
		},
	}}}
}

func buildGitLabIncidentOracleBatch(t *testing.T, input map[string]any) CompleteRouteBatch {
	t.Helper()
	project := map[string]any{
		"id": input["project_id"], "name": "api",
		"path_with_namespace": input["repo_full_name"],
	}
	projectJSON, err := json.Marshal(project)
	if err != nil {
		t.Fatal(err)
	}
	pages, err := json.Marshal(input["pages"])
	if err != nil {
		t.Fatal(err)
	}
	var pageValues []json.RawMessage
	if err := json.Unmarshal(pages, &pageValues); err != nil {
		t.Fatal(err)
	}
	responses := []gitLabCommitsResponse{{body: string(projectJSON)}}
	for _, page := range pageValues {
		responses = append(responses, gitLabCommitsResponse{body: string(page)})
	}
	doer := &gitLabCommitsDoer{t: t, responses: responses}
	normalizedAt, err := time.Parse(time.RFC3339Nano, input["normalized_at"].(string))
	if err != nil {
		t.Fatal(err)
	}
	claim := nativeTestClaim("gitlab", "incidents")
	batch, err := (GitLabIncidentsRouteHandler{}).Collect(
		context.Background(), claim, providerfoundation.Credential{},
		gitLabRepositoryClient(t, doer, input["provider_instance_id"].(string)), normalizedAt,
	)
	if err != nil {
		t.Fatal(err)
	}
	return batch
}

func oracleEffectRow[T any](t *testing.T, batch CompleteRouteBatch, destination string) T {
	t.Helper()
	for _, effect := range batch.Effects {
		if effect.Destination != destination {
			continue
		}
		if len(effect.Rows) != 1 {
			t.Fatalf("%s rows=%d", destination, len(effect.Rows))
		}
		var row T
		if err := json.Unmarshal(effect.Rows[0], &row); err != nil {
			t.Fatal(err)
		}
		return row
	}
	t.Fatalf("missing effect %s", destination)
	var zero T
	return zero
}

func TestGenericOracleMatchesLivePythonGitLabIncidentRows(t *testing.T) {
	cases := oracleGitLabIncidentCases()
	compareRowsAgainstPythonOracle(t, "gitlab/incidents/service", cases,
		func(t *testing.T, input map[string]any) gitLabOperationalServiceRow {
			return oracleEffectRow[gitLabOperationalServiceRow](t, buildGitLabIncidentOracleBatch(t, input), "operational_services")
		}, nil)
	compareRowsAgainstPythonOracle(t, "gitlab/incidents/mapping", cases,
		func(t *testing.T, input map[string]any) gitLabServiceRepositoryMappingRow {
			return oracleEffectRow[gitLabServiceRepositoryMappingRow](t, buildGitLabIncidentOracleBatch(t, input), "operational_service_repository_mappings")
		}, nil)
	compareRowsAgainstPythonOracle(t, "gitlab/incidents/incident", cases,
		func(t *testing.T, input map[string]any) jiraIncidentRow {
			return oracleEffectRow[jiraIncidentRow](t, buildGitLabIncidentOracleBatch(t, input), "operational_incidents")
		}, nil)
}

func TestGenericOracleMatchesLivePythonGitLabIncidentTraversal(t *testing.T) {
	compareRowsAgainstPythonOracle(
		t, "gitlab/incidents/traversal", oracleGitLabIncidentTraversalCases(),
		func(t *testing.T, input map[string]any) gitLabIncidentTraversalTrace {
			return buildGitLabIncidentTraversalTrace(t, input)
		}, nil,
	)
}

func buildGitLabIncidentTraversalTrace(
	t *testing.T, input map[string]any,
) gitLabIncidentTraversalTrace {
	t.Helper()
	projectID := input["project_id"].(int)
	projectJSON, err := json.Marshal(map[string]any{
		"id": projectID, "name": "api", "path_with_namespace": input["repo_full_name"],
	})
	if err != nil {
		t.Fatal(err)
	}
	pageJSON, err := json.Marshal(input["pages"])
	if err != nil {
		t.Fatal(err)
	}
	var pages []json.RawMessage
	if err := json.Unmarshal(pageJSON, &pages); err != nil {
		t.Fatal(err)
	}
	responses := []gitLabCommitsResponse{{body: string(projectJSON)}}
	for _, page := range pages {
		responses = append(responses, gitLabCommitsResponse{body: string(page)})
	}
	doer := &gitLabCommitsDoer{t: t, responses: responses}
	normalizedAt, err := time.Parse(time.RFC3339Nano, input["normalized_at"].(string))
	if err != nil {
		t.Fatal(err)
	}
	maxIssues := input["max_issues"].(int)
	batch, err := (GitLabIncidentsRouteHandler{MaxIssues: maxIssues}).Collect(
		context.Background(), nativeTestClaim("gitlab", "incidents"),
		providerfoundation.Credential{},
		gitLabRepositoryClient(t, doer, input["provider_instance_id"].(string)),
		normalizedAt,
	)
	if err != nil {
		t.Fatal(err)
	}
	trace := gitLabIncidentTraversalTrace{}
	for _, request := range doer.requests[1:] {
		trace.Calls = append(trace.Calls, traversalCallFromQuery(t, projectID, request.URL.Query()))
	}
	for _, effect := range batch.Effects {
		if effect.Destination != "operational_incidents" {
			continue
		}
		for _, raw := range effect.Rows {
			var row jiraIncidentRow
			if err := json.Unmarshal(raw, &row); err != nil {
				t.Fatal(err)
			}
			trace.IncidentExternalIDs = append(trace.IncidentExternalIDs, row.ExternalID)
		}
	}
	sort.Strings(trace.IncidentExternalIDs)
	return trace
}

func traversalCallFromQuery(
	t *testing.T, projectID int, query url.Values,
) gitLabIncidentTraversalCall {
	t.Helper()
	page, err := strconv.Atoi(query.Get("page"))
	if err != nil {
		t.Fatal(err)
	}
	perPage, err := strconv.Atoi(query.Get("per_page"))
	if err != nil {
		t.Fatal(err)
	}
	return gitLabIncidentTraversalCall{
		ProjectID: projectID, IssueType: query.Get("issue_type"),
		UpdatedAfter: query.Get("updated_after"), UpdatedBefore: query.Get("updated_before"),
		State: query.Get("state"), Page: page, PerPage: perPage,
		OrderBy: query.Get("order_by"), Sort: query.Get("sort"),
	}
}
