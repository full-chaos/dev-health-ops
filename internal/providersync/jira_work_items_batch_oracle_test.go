package providersync

import (
	"fmt"
	"sort"
	"strings"
	"testing"
	"time"
)

// jiraBatchOracleRow is the complete six-list return boundary of Python's
// fetch_jira_work_items_with_extras. WorkItem and transition rows intentionally
// use semantic structs without Go's sink-only LastSynced field; the other four
// models carry LastSynced in Python and therefore compare it at the same
// deterministic unit timestamp Go writes into its effect rows.
type jiraBatchOracleRow struct {
	WorkItems    []jiraWorkItemOraclePrepRow  `json:"work_items"`
	Transitions  []jiraBatchTransitionRow     `json:"transitions"`
	Dependencies []jiraWorkItemDependencyRow  `json:"dependencies"`
	ReopenEvents []jiraWorkItemReopenRow      `json:"reopen_events"`
	Interactions []jiraWorkItemInteractionRow `json:"interactions"`
	Sprints      []jiraSprintRow              `json:"sprints"`
}

type jiraBatchTransitionRow struct {
	WorkItemID    string    `json:"work_item_id"`
	Provider      string    `json:"provider"`
	OccurredAt    time.Time `json:"occurred_at"`
	FromStatusRaw *string   `json:"from_status_raw"`
	ToStatusRaw   *string   `json:"to_status_raw"`
	FromStatus    string    `json:"from_status"`
	ToStatus      string    `json:"to_status"`
	Actor         *string   `json:"actor"`
	OrgID         string    `json:"org_id"`
}

func TestJiraProducerBatchMatchesLivePython(t *testing.T) {
	compareRowsAgainstPythonOracle(
		t,
		"jira/work-items/batch",
		jiraWorkItemsBatchOracleCases(),
		buildJiraProducerBatchOracleRow,
		nil,
	)
}

func buildJiraProducerBatchOracleRow(
	t *testing.T,
	input map[string]any,
) jiraBatchOracleRow {
	t.Helper()
	claim := nativeTestClaim("jira", "work-items")
	claim.OrgID = stringFrom(input["org_id"])
	claim.SourceExternalID = "OPS"
	claim.DatasetOptions = map[string]any{
		"story_points_field": "customfield_10016",
		"sprint_field":       "customfield_10020",
		"epic_link_field":    "customfield_10014",
	}
	normalizedAt := jiraProducerBatchNormalizedAt()
	statusMapping := loadRealStatusMapping(t)

	result := jiraBatchOracleRow{
		WorkItems:    make([]jiraWorkItemOraclePrepRow, 0),
		Transitions:  make([]jiraBatchTransitionRow, 0),
		Dependencies: make([]jiraWorkItemDependencyRow, 0),
		ReopenEvents: make([]jiraWorkItemReopenRow, 0),
		Interactions: make([]jiraWorkItemInteractionRow, 0),
		Sprints:      make([]jiraSprintRow, 0),
	}
	comments := make(map[string][]map[string]any)
	if rawComments, ok := input["comments"].(map[string]any); ok {
		for key, raw := range rawComments {
			comments[key] = jiraBatchMaps(t, raw)
		}
	}
	fetchComments := jiraBatchBool(input["fetch_comments"], true)
	commentsLimit := jiraBatchInt(input["comments_limit"])
	sprintPayloads := make(map[string]map[string]any)
	if rawSprints, ok := input["sprints"].(map[string]any); ok {
		for id, raw := range rawSprints {
			sprintPayloads[id] = jiraBatchMap(t, raw)
		}
	}
	sprintCache := make(map[string]jiraSprintRow)
	if rawReference, ok := input["reference_sprints"].([]any); ok {
		for _, raw := range rawReference {
			payload := jiraBatchMap(t, raw)
			sprint, err := normalizeJiraSprint(claim, payload, normalizedAt)
			if err != nil {
				t.Fatalf("normalize reference sprint: %v", err)
			}
			sprintCache[sprint.SprintID] = sprint
		}
	}
	sprintIDs := make(map[string]struct{})
	rawIssues, ok := input["issues"].([]any)
	if !ok || len(rawIssues) < 2 {
		t.Fatalf("batch case needs at least two issues, got %T/%d", input["issues"], len(rawIssues))
	}
	for _, raw := range rawIssues {
		issue := jiraBatchMap(t, raw)
		item, transitions, err := normalizeJiraWorkItem(
			claim,
			jiraWorkItemFixtureInput{Raw: issue},
			statusMapping,
			jiraOracleIdentity,
			normalizedAt,
		)
		if err != nil {
			t.Fatalf("normalize Jira batch issue: %v", err)
		}
		result.WorkItems = append(result.WorkItems, jiraBatchSemanticWorkItem(item))
		for _, transition := range transitions {
			result.Transitions = append(result.Transitions, jiraBatchSemanticTransition(transition))
		}
		result.Dependencies = append(
			result.Dependencies,
			normalizeJiraDependencies(claim, item.WorkItemID, issue, normalizedAt)...,
		)
		result.ReopenEvents = append(
			result.ReopenEvents,
			jiraReopenEvents(claim, transitions, normalizedAt)...,
		)
		if fetchComments {
			issueKey := stringFrom(issue["key"])
			issueComments := comments[issueKey]
			if commentsLimit > 0 && len(issueComments) > commentsLimit {
				issueComments = issueComments[:commentsLimit]
			}
			result.Interactions = append(
				result.Interactions,
				normalizeJiraInteractions(claim, item.WorkItemID, issueComments, jiraOracleIdentity, normalizedAt)...,
			)
		}
		if item.SprintID != nil && *item.SprintID != "" {
			sprintIDs[*item.SprintID] = struct{}{}
		}
	}
	ids := make([]string, 0, len(sprintIDs))
	for id := range sprintIDs {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	for _, id := range ids {
		if sprint, ok := sprintCache[id]; ok {
			result.Sprints = append(result.Sprints, sprint)
			continue
		}
		payload, ok := sprintPayloads[id]
		if !ok {
			t.Fatalf("missing sprint payload for %s", id)
		}
		sprint, err := normalizeJiraSprint(claim, payload, normalizedAt)
		if err != nil {
			t.Fatalf("normalize Jira sprint %s: %v", id, err)
		}
		result.Sprints = append(result.Sprints, sprint)
	}
	return result
}

func jiraProducerBatchNormalizedAt() time.Time {
	return time.Date(2026, 8, 10, 12, 0, 0, 123456000, time.UTC)
}

func jiraBatchSemanticWorkItem(row jiraWorkItemRow) jiraWorkItemOraclePrepRow {
	return jiraWorkItemOraclePrepRow{
		WorkItemID: row.WorkItemID, Provider: row.Provider, Title: row.Title,
		Type: row.Type, Status: row.Status, StatusRaw: row.StatusRaw,
		Description: row.Description, RepoID: row.RepoID,
		NativeTeamKey: row.NativeTeamKey, ProjectKey: row.ProjectKey,
		ProjectID: row.ProjectID, ProjectName: row.ProjectName,
		Assignees: row.Assignees, Reporter: row.Reporter,
		CreatedAt: row.CreatedAt, UpdatedAt: row.UpdatedAt, StartedAt: row.StartedAt,
		CompletedAt: row.CompletedAt, ClosedAt: row.ClosedAt, Labels: row.Labels,
		StoryPoints: row.StoryPoints, SprintID: row.SprintID, SprintName: row.SprintName,
		ParentID: row.ParentID, EpicID: row.EpicID, URL: row.URL,
		PriorityRaw: row.PriorityRaw, ServiceClass: row.ServiceClass, DueAt: row.DueAt,
		OrgID: row.OrgID,
	}
}

func jiraBatchSemanticTransition(row jiraWorkItemTransitionRow) jiraBatchTransitionRow {
	return jiraBatchTransitionRow{
		WorkItemID: row.WorkItemID, Provider: row.Provider, OccurredAt: row.OccurredAt,
		FromStatusRaw: row.FromStatusRaw, ToStatusRaw: row.ToStatusRaw,
		FromStatus: row.FromStatus, ToStatus: row.ToStatus, Actor: row.Actor,
		OrgID: row.OrgID,
	}
}

func jiraBatchMap(t *testing.T, value any) map[string]any {
	t.Helper()
	row, ok := value.(map[string]any)
	if !ok {
		t.Fatalf("Jira batch value has type %T, want map[string]any", value)
	}
	return row
}

func jiraBatchMaps(t *testing.T, value any) []map[string]any {
	t.Helper()
	values, ok := value.([]any)
	if !ok {
		t.Fatalf("Jira batch list has type %T, want []any", value)
	}
	rows := make([]map[string]any, 0, len(values))
	for _, value := range values {
		rows = append(rows, jiraBatchMap(t, value))
	}
	return rows
}

func jiraBatchBool(value any, fallback bool) bool {
	if parsed, ok := value.(bool); ok {
		return parsed
	}
	return fallback
}

func jiraBatchInt(value any) int {
	switch parsed := value.(type) {
	case int:
		return parsed
	case float64:
		return int(parsed)
	case string:
		value := strings.TrimSpace(parsed)
		if value == "" {
			return 0
		}
		var result int
		_, _ = fmt.Sscanf(value, "%d", &result)
		return result
	default:
		return 0
	}
}

func jiraWorkItemsBatchOracleCases() []oracleCase {
	issues := []any{jiraBatchIssue("OPS-101", false), jiraBatchIssue("OPS-102", true)}
	comments := map[string]any{
		"OPS-101": []any{
			map[string]any{
				"created": "2026-08-03T10:00:00Z",
				"author":  map[string]any{"accountId": "jira-commenter"},
				"body":    "verified ✅",
			},
			map[string]any{
				"created": "2026-08-03T11:00:00Z",
				"author":  map[string]any{"displayName": "Second commenter"},
				"body":    "follow-up",
			},
		},
		"OPS-102": []any{map[string]any{
			"created": "2026-08-04T13:00:00Z",
			"author":  map[string]any{"accountId": "jira-commenter-2"},
			"body":    "closed",
		}},
	}
	sprint := map[string]any{
		"id": 9001, "name": "August support", "state": "closed",
		"startDate": "2026-08-01T00:00:00Z", "endDate": "2026-08-08T00:00:00Z",
		"completeDate": "2026-08-09T00:00:00Z",
	}
	base := map[string]any{
		"org_id":         "org-acme",
		"since":          "2026-08-01T00:00:00Z",
		"until":          "2026-08-10T00:00:00Z",
		"project_keys":   []any{"OPS"},
		"issues":         issues,
		"comments":       comments,
		"sprints":        map[string]any{"9001": sprint},
		"fetch_comments": true,
		"comments_limit": 1,
	}
	window := cloneJiraBatchMap(base)
	window["fetch_all"] = false
	all := cloneJiraBatchMap(base)
	all["fetch_all"] = true
	all["fetch_comments"] = false
	all["reference_sprints"] = []any{sprint}
	return []oracleCase{
		{ID: "window_with_all_enrichment", Input: window},
		{ID: "fetch_all_reuses_reference_sprint", Input: all},
	}
}

func cloneJiraBatchMap(value map[string]any) map[string]any {
	clone := make(map[string]any, len(value))
	for key, item := range value {
		clone[key] = item
	}
	return clone
}

func jiraBatchIssue(key string, done bool) map[string]any {
	if !done {
		return map[string]any{
			"key": key, "self": "https://jira.example/rest/api/3/issue/" + key,
			"fields": map[string]any{
				"project": map[string]any{"key": "OPS", "id": "10001", "name": "Operations"},
				"summary": "Repair the delivery path", "description": "Customer-visible repair",
				"status":    map[string]any{"name": "In Progress", "statusCategory": map[string]any{"key": "indeterminate"}},
				"issuetype": map[string]any{"name": "Bug"}, "labels": []any{"bug", "customer-impact"},
				"priority": map[string]any{"name": "Highest"},
				"created":  "2026-08-01T08:00:00.123+0000", "updated": "2026-08-03T09:30:00.654+0000",
				"duedate": "2026-08-10", "resolutiondate": nil, "customfield_10016": 5.0,
				"customfield_10020": []any{map[string]any{"id": 9001, "name": "August support"}},
				"customfield_10014": "EPIC-9", "parent": map[string]any{"key": "OPS-90"},
				"issuelinks": []any{map[string]any{
					"type":         map[string]any{"outward": "blocks", "inward": "is blocked by"},
					"outwardIssue": map[string]any{"key": "OPS-102"},
				}},
			},
			"changelog": map[string]any{"histories": []any{
				map[string]any{"created": "2026-08-03T09:00:00Z", "author": map[string]any{"accountId": "jira-account-1"}, "items": []any{map[string]any{"field": "status", "fromString": "Done", "toString": "To Do"}}},
				map[string]any{"created": "2026-08-02T09:00:00Z", "author": map[string]any{"accountId": "jira-account-1"}, "items": []any{map[string]any{"field": "status", "fromString": "To Do", "toString": "In Progress"}}},
				map[string]any{"created": "2026-08-01T08:30:00Z", "author": map[string]any{"displayName": "Operator One"}, "items": []any{map[string]any{"field": "status", "fromString": nil, "toString": "To Do"}}},
			}},
		}
	}
	return map[string]any{
		"key": key, "self": "https://jira.example/rest/api/3/issue/" + key,
		"fields": map[string]any{
			"project": map[string]any{"key": "OPS", "id": "10001", "name": "Operations"},
			"summary": "Close repaired incident", "description": "Done after verification",
			"status":    map[string]any{"name": "Resolved", "statusCategory": map[string]any{"key": "done"}},
			"issuetype": map[string]any{"name": "Task"}, "labels": []any{"maintenance"},
			"priority": map[string]any{"name": "Low"},
			"created":  "2026-08-02T00:00:00Z", "updated": "2026-08-04T12:00:00Z",
			"resolutiondate": "2026-08-04T11:30:00Z", "customfield_10016": 2,
			"customfield_10020": []any{map[string]any{"id": "9001", "name": "August support"}},
			"customfield_10014": "EPIC-10", "parent": map[string]any{"key": "OPS-91"},
			"issuelinks": []any{map[string]any{
				"type":        map[string]any{"outward": "is blocked by", "inward": "blocks"},
				"inwardIssue": map[string]any{"key": "OPS-101"},
			}},
		},
		"changelog": map[string]any{"histories": []any{map[string]any{
			"created": "2026-08-04T11:00:00Z", "author": map[string]any{"accountId": "jira-account-2"},
			"items": []any{map[string]any{"field": "status", "fromString": "In Progress", "toString": "Resolved"}},
		}}},
	}
}
