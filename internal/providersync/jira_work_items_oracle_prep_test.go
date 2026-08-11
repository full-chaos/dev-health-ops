package providersync

import (
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
)

// jiraWorkItemOraclePrepRow is the semantic WorkItem boundary. The provider
// row carries LastSynced for sink idempotency, so this oracle projection keeps
// the dataclass's exact field set and intentionally omits that sink-only field.
type jiraWorkItemOraclePrepRow struct {
	WorkItemID    string     `json:"work_item_id"`
	Provider      string     `json:"provider"`
	Title         string     `json:"title"`
	Type          string     `json:"type"`
	Status        string     `json:"status"`
	StatusRaw     *string    `json:"status_raw"`
	Description   *string    `json:"description"`
	RepoID        *uuid.UUID `json:"repo_id"`
	NativeTeamKey *string    `json:"native_team_key"`
	ProjectKey    *string    `json:"project_key"`
	ProjectID     *string    `json:"project_id"`
	ProjectName   *string    `json:"project_name"`
	Assignees     []string   `json:"assignees"`
	Reporter      *string    `json:"reporter"`
	CreatedAt     time.Time  `json:"created_at"`
	UpdatedAt     time.Time  `json:"updated_at"`
	StartedAt     *time.Time `json:"started_at"`
	CompletedAt   *time.Time `json:"completed_at"`
	ClosedAt      *time.Time `json:"closed_at"`
	Labels        []string   `json:"labels"`
	StoryPoints   *float64   `json:"story_points"`
	SprintID      *string    `json:"sprint_id"`
	SprintName    *string    `json:"sprint_name"`
	ParentID      *string    `json:"parent_id"`
	EpicID        *string    `json:"epic_id"`
	URL           *string    `json:"url"`
	PriorityRaw   *string    `json:"priority_raw"`
	ServiceClass  *string    `json:"service_class"`
	DueAt         *time.Time `json:"due_at"`
	OrgID         string     `json:"org_id"`
}

func TestJiraWorkItemMatchesLivePythonProductionRow(t *testing.T) {
	compareRowsAgainstPythonOracle(
		t,
		"jira/work-items/issue",
		jiraWorkItemOraclePrepCases(),
		buildJiraWorkItemOraclePrepRow,
		nil,
	)
}

func jiraWorkItemOraclePrepCases() []oracleCase {
	return []oracleCase{
		{
			ID: "legacy_json_issue_with_history",
			Input: map[string]any{
				"org_id":             "org-acme",
				"issue_shape":        "json",
				"story_points_field": "customfield_10016",
				"sprint_field":       "customfield_10020",
				"epic_link_field":    "customfield_10014",
				"raw_issue": map[string]any{
					"key":  "OPS-101",
					"self": "https://jira.example/rest/api/3/issue/OPS-101",
					"fields": map[string]any{
						"project": map[string]any{
							"key": "OPS", "id": "10001", "name": "Operations",
						},
						"summary":     "Repair the delivery path",
						"description": "Customer-visible repair",
						"status": map[string]any{
							"name":           "In Progress",
							"statusCategory": map[string]any{"key": "indeterminate"},
						},
						"issuetype":         map[string]any{"name": "Bug"},
						"labels":            []any{"bug", "customer-impact"},
						"priority":          map[string]any{"name": "Highest"},
						"created":           "2026-08-01T08:00:00.123+0000",
						"updated":           "2026-08-03T09:30:00.654+0000",
						"duedate":           "2026-08-10",
						"resolutiondate":    nil,
						"customfield_10016": 5.0,
						"customfield_10020": []any{
							map[string]any{"id": 9001, "name": "August support"},
						},
						"customfield_10014": "EPIC-9",
						"parent":            map[string]any{"key": "OPS-90"},
					},
					"changelog": map[string]any{
						// Deliberately newest first: production sorts histories.
						"histories": []any{
							map[string]any{
								"created": "2026-08-02T09:00:00.000+0000",
								"author": map[string]any{
									"accountId":   "jira-account-1",
									"displayName": "Operator One",
								},
								"items": []any{map[string]any{
									"field": "status", "fromString": "To Do", "toString": "In Progress",
								}},
							},
							map[string]any{
								"created": "2026-08-01T08:30:00.000+0000",
								"author":  map[string]any{"displayName": "Operator One"},
								"items": []any{map[string]any{
									"field": "status", "fromString": nil, "toString": "To Do",
								}},
							},
						},
					},
				},
			},
		},
		{
			ID: "adapter_object_done_issue_with_identity",
			Input: map[string]any{
				"org_id":             "org-acme",
				"issue_shape":        "object",
				"story_points_field": "customfield_10016",
				"sprint_field":       "customfield_10020",
				"epic_link_field":    "customfield_10014",
				"raw_issue": map[string]any{
					"key":  "OPS-102",
					"self": "https://jira.example/rest/api/3/issue/OPS-102",
					"fields": map[string]any{
						"project": map[string]any{
							"key": "OPS", "id": "10001", "name": "Operations",
						},
						"summary":     "Close repaired incident",
						"description": "Done after verification",
						"status": map[string]any{
							"name":           "Resolved",
							"statusCategory": map[string]any{"key": "done"},
						},
						"issuetype": map[string]any{"name": "Task"},
						"labels":    []any{"maintenance"},
						"priority":  map[string]any{"name": "Low"},
						"assignee": map[string]any{
							"emailAddress": "engineer@example.com",
							"accountId":    "jira-account-2",
							"displayName":  "Engineer Two",
						},
						"reporter": map[string]any{
							"accountId": "jira-account-3", "displayName": "Reporter Three",
						},
						"created":           "2026-07-20T00:00:00Z",
						"updated":           "2026-08-04T12:00:00Z",
						"duedate":           nil,
						"resolutiondate":    "2026-08-04T11:30:00Z",
						"customfield_10016": 2,
						"customfield_10020": []any{
							map[string]any{"id": "9002", "name": "August closeout"},
						},
						"customfield_10014": "EPIC-10",
						"parent":            map[string]any{"key": "OPS-91"},
					},
					"changelog": map[string]any{
						"histories": []any{map[string]any{
							"created": "2026-08-04T11:00:00Z",
							"author": map[string]any{
								"emailAddress": "engineer@example.com",
								"displayName":  "Engineer Two",
							},
							"items": []any{map[string]any{
								"field": "status", "fromString": "In Progress", "toString": "Resolved",
							}},
						}},
					},
				},
			},
		},
	}
}

func buildJiraWorkItemOraclePrepRow(
	t *testing.T,
	input map[string]any,
) jiraWorkItemOraclePrepRow {
	t.Helper()
	claim := nativeTestClaim("jira", "work-items")
	claim.OrgID = input["org_id"].(string)
	claim.DatasetOptions = map[string]any{
		"story_points_field": input["story_points_field"],
		"sprint_field":       input["sprint_field"],
		"epic_link_field":    input["epic_link_field"],
	}
	raw, ok := input["raw_issue"].(map[string]any)
	if !ok {
		t.Fatalf("raw_issue has unexpected type %T", input["raw_issue"])
	}
	statusMapping := loadRealStatusMapping(t)
	row, _, err := normalizeJiraWorkItem(
		claim,
		jiraWorkItemFixtureInput{Raw: raw, ObjectShape: input["issue_shape"] == "object"},
		statusMapping,
		jiraOracleIdentity,
		time.Date(2026, 8, 10, 12, 0, 0, 123456000, time.UTC),
	)
	if err != nil {
		t.Fatalf("normalize Jira issue: %v", err)
	}
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

func jiraOracleIdentity(email, accountID, displayName string) string {
	if email != "" {
		return strings.ToLower(strings.TrimSpace(email))
	}
	if accountID != "" {
		return "jira:accountid:" + strings.TrimSpace(accountID)
	}
	if displayName != "" {
		return strings.TrimSpace(displayName)
	}
	return "unknown"
}
