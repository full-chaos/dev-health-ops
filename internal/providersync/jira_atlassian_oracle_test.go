package providersync

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
)

// This boundary deliberately compares the two Atlassian-client enrichment
// lists only. WorkItem/transition semantics remain covered by the existing
// legacy-family producer pair; this pair owns the distinct worklog and board
// sprint contract and invokes JiraAtlassianRouteHandler.Collect, including its
// real REST pagination and reference-cache branch.
type jiraAtlassianOracleSurfaces struct {
	Worklogs []jiraWorklogRow `json:"worklogs"`
	Sprints  []jiraSprintRow  `json:"sprints"`
}

func TestJiraAtlassianSurfacesMatchLivePythonProducer(t *testing.T) {
	compareRowsAgainstPythonOracle(
		t,
		"jira/work-items/atlassian",
		jiraAtlassianOracleCases(),
		buildJiraAtlassianOracleSurfaces,
		nil,
	)
}

func jiraAtlassianOracleCases() []oracleCase {
	return []oracleCase{
		{
			ID: "rest_worklog_and_board_sprint",
			Input: map[string]any{
				"org_id": "org-acme", "since": "2026-08-01T00:00:00Z", "until": "2026-08-10T00:00:00Z",
				"project_key": "OPS", "fetch_worklogs": true, "fetch_board_sprints": true,
				"issue":             jiraAtlassianOracleIssue("OPS-301"),
				"worklogs":          []any{map[string]any{"id": "wl-301", "author": map[string]any{"accountId": "account-301", "displayName": "Worker"}, "started": "2026-08-02T10:00:00.123456Z", "timeSpentSeconds": 3600, "created": "2026-08-02T10:01:00.123456Z", "updated": "2026-08-02T10:02:00.123456Z"}},
				"board_sprints":     []any{map[string]any{"id": "9301", "name": "August board", "state": "active", "startDate": "2026-08-01T00:00:00Z", "endDate": "2026-08-31T00:00:00Z", "completeDate": nil}},
				"reference_sprints": []any{},
			},
		},
		{
			ID: "reference_sprint_skips_board_fetch",
			Input: map[string]any{
				"org_id": "org-acme", "since": "2026-08-01T00:00:00Z", "until": "2026-08-10T00:00:00Z",
				"project_key": "OPS", "fetch_worklogs": false, "fetch_board_sprints": true,
				"issue":             jiraAtlassianOracleIssue("OPS-302"),
				"worklogs":          []any{},
				"reference_sprints": []any{map[string]any{"id": "9302", "name": "Cached sprint", "state": "closed", "startDate": "2026-07-01T00:00:00Z", "endDate": "2026-07-31T00:00:00Z", "completeDate": "2026-08-01T00:00:00Z"}},
				"board_sprints":     []any{map[string]any{"id": "9999", "name": "must-not-fetch", "state": "active"}},
			},
		},
		{
			ID: "graphql_failure_falls_back_to_rest_worklog",
			Input: map[string]any{
				"org_id": "org-acme", "since": "2026-08-01T00:00:00Z", "until": "2026-08-10T00:00:00Z",
				"project_key": "OPS", "fetch_worklogs": true, "fetch_board_sprints": false,
				"graphql_fallback": true, "issue": jiraAtlassianOracleIssue("OPS-303"),
				"worklogs":      []any{map[string]any{"id": "wl-303", "author": map[string]any{"accountId": "account-303", "name": "GraphQL Worker"}, "started": "2026-08-03T10:00:00.123456Z", "timeSpentSeconds": 1200, "created": "2026-08-03T10:01:00.123456Z", "updated": "2026-08-03T10:02:00.123456Z"}},
				"board_sprints": []any{}, "reference_sprints": []any{},
			},
		},
	}
}

func jiraAtlassianOracleIssue(key string) map[string]any {
	return map[string]any{"key": key, "self": "https://acme.atlassian.net/rest/api/3/issue/" + key, "fields": map[string]any{
		"project": map[string]any{"key": "OPS", "id": "10001", "name": "Operations"},
		"summary": "Atlassian canonical issue", "status": map[string]any{"name": "In Progress", "statusCategory": map[string]any{"key": "indeterminate"}},
		"issuetype": map[string]any{"name": "Task"}, "labels": []any{"support"},
		"created": "2026-08-01T08:00:00.123456Z", "updated": "2026-08-02T09:00:00.123456Z",
		"customfield_10020": []any{map[string]any{"id": "9301", "name": "August board"}},
	}}
}

func buildJiraAtlassianOracleSurfaces(t *testing.T, input map[string]any) jiraAtlassianOracleSurfaces {
	t.Helper()
	claim := nativeTestClaim("jira", "work-items")
	claim.OrgID = stringFrom(input["org_id"])
	claim.SourceExternalID = "OPS"
	claim.DatasetOptions = map[string]any{
		"fetch_worklogs": jiraBatchBool(input["fetch_worklogs"], false), "fetch_board_sprints": jiraBatchBool(input["fetch_board_sprints"], false),
		"atlassian_gql_enabled": jiraBatchBool(input["graphql_fallback"], false),
		"sprint_field":          "customfield_10020",
	}
	normalizedAt := jiraProducerBatchNormalizedAt()
	issue := input["issue"].(map[string]any)
	worklogs := jiraBatchMaps(t, input["worklogs"])
	boardSprints := jiraBatchMaps(t, input["board_sprints"])
	refs := make([]jiraSprintRow, 0)
	for _, raw := range jiraBatchMaps(t, input["reference_sprints"]) {
		row, err := normalizeJiraSprint(claim, raw, normalizedAt)
		if err != nil {
			t.Fatal(err)
		}
		refs = append(refs, row)
	}
	doer := &jiraAtlassianOracleDoer{t: t, issue: issue, worklogs: worklogs, boardSprints: boardSprints}
	client := jiraWorkItemsTestClient(t, doer, providerfoundation.LeaseGuardFunc(func(context.Context) error { return nil }))
	var graphqlClient *providerfoundation.HTTPClient
	if jiraBatchBool(input["graphql_fallback"], false) {
		graphqlClient = jiraWorkItemsTestClient(t, &jiraAtlassianOracleDoer{t: t, graphqlFailure: true}, providerfoundation.LeaseGuardFunc(func(context.Context) error { return nil }))
	}
	batch, err := (JiraAtlassianRouteHandler{StatusMapping: loadRealStatusMapping(t), Identity: jiraOracleIdentity, ReferenceSprints: refs, CloudID: "cloud-301", GraphQLClient: graphqlClient}).Collect(context.Background(), claim, providerfoundation.Credential{}, client, normalizedAt)
	if err != nil {
		t.Fatal(err)
	}
	result := jiraAtlassianOracleSurfaces{Worklogs: make([]jiraWorklogRow, 0), Sprints: make([]jiraSprintRow, 0)}
	for _, effect := range batch.Effects {
		for _, raw := range effect.Rows {
			switch effect.Destination {
			case "worklogs":
				var row jiraWorklogRow
				if err := json.Unmarshal(raw, &row); err != nil {
					t.Fatal(err)
				}
				result.Worklogs = append(result.Worklogs, row)
			case "sprints":
				var row jiraSprintRow
				if err := json.Unmarshal(raw, &row); err != nil {
					t.Fatal(err)
				}
				result.Sprints = append(result.Sprints, row)
			}
		}
	}
	return result
}

type jiraAtlassianOracleDoer struct {
	t                      *testing.T
	issue                  map[string]any
	worklogs, boardSprints []map[string]any
	graphqlFailure         bool
}

func (doer *jiraAtlassianOracleDoer) Do(request *http.Request) (*http.Response, error) {
	doer.t.Helper()
	var value any
	switch {
	case request.URL.Path == "/graphql" && doer.graphqlFailure:
		return &http.Response{StatusCode: http.StatusBadGateway, Header: http.Header{"Content-Type": []string{"application/json"}}, Body: io.NopCloser(strings.NewReader(`{"errors":[{"message":"temporary"}]}`)), Request: request}, nil
	case request.URL.Path == "/rest/api/3/search":
		value = map[string]any{"issues": []any{doer.issue}, "startAt": 0, "total": 1}
	case strings.HasSuffix(request.URL.Path, "/changelog"):
		value = map[string]any{"values": []any{}, "total": 0, "isLast": true}
	case strings.HasSuffix(request.URL.Path, "/worklog"):
		value = map[string]any{"worklogs": doer.worklogs, "total": len(doer.worklogs)}
	case request.URL.Path == "/rest/agile/1.0/board":
		value = map[string]any{"values": []any{map[string]any{"id": 77, "name": "Operations"}}, "isLast": true}
	case strings.HasSuffix(request.URL.Path, "/sprint"):
		value = map[string]any{"values": doer.boardSprints, "isLast": true}
	default:
		doer.t.Fatalf("unexpected Atlassian oracle request %s", request.URL.String())
	}
	raw, err := json.Marshal(value)
	if err != nil {
		return nil, err
	}
	return &http.Response{StatusCode: http.StatusOK, Header: http.Header{"Content-Type": []string{"application/json"}}, Body: io.NopCloser(strings.NewReader(string(raw))), Request: request}, nil
}
