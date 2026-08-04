package providersync

import (
	"encoding/json"
	"reflect"
	"testing"

	"github.com/google/uuid"
)

func TestGitHubPullRequestWorkItemMatchesLivePythonProductionRow(t *testing.T) {
	compareRowsAgainstPythonOracle(
		t,
		"github/work-items/pr",
		[]oracleCase{{
			ID: "merged_priority_pr",
			Input: githubPullRequestSemanticOracleInput(map[string]any{
				"number": 17, "title": "Repair delivery path",
				"body": "Routine repair", "state": "closed", "merged": true,
				"created_at": "2026-08-01T08:00:00.123456Z",
				"updated_at": "2026-08-03T09:30:00.654321Z",
				"closed_at":  "2026-08-03T09:30:00Z",
				"merged_at":  "2026-08-03T09:29:00Z",
				"labels":     []any{map[string]any{"name": "p1"}},
				"assignees":  []any{map[string]any{"login": "reviewer"}},
				"user":       map[string]any{"login": "author"},
				"head":       map[string]any{"ref": "feature/repair"},
				"html_url":   "https://github.com/acme/api/pull/17",
			}, []any{}),
		}},
		buildGitHubPullRequestWorkItemOracleRow,
		nil,
	)
}

func TestGitHubWorkItemStatusTransitionMatchesLivePythonProductionRow(t *testing.T) {
	events := []any{
		map[string]any{"event": "reopened", "created_at": "2026-08-02T09:00:00Z"},
		map[string]any{"event": "merged", "created_at": "2026-08-03T09:29:00Z"},
	}
	input := githubPullRequestSemanticOracleInput(map[string]any{
		"number": 17, "title": "Repair delivery path", "body": "Routine repair",
		"state": "closed", "merged": true,
		"created_at": "2026-08-01T08:00:00Z",
		"updated_at": "2026-08-03T09:30:00Z",
		"closed_at":  "2026-08-03T09:30:00Z",
		"merged_at":  "2026-08-03T09:29:00Z",
		"labels":     []any{}, "assignees": []any{},
		"user": map[string]any{"login": "author"},
		"head": map[string]any{"ref": "feature/repair"},
	}, events)
	input["transition_index"] = 1
	issueInput := githubIssueSemanticOracleInput(map[string]any{
		"number": 42, "title": "Repair delivery path", "state": "open",
		"created_at": "2026-08-01T08:00:00Z",
		"updated_at": "2026-08-03T09:30:00Z",
		"labels":     []any{}, "assignees": []any{},
	}, []any{map[string]any{
		"event": "labeled", "created_at": "2026-08-02T09:00:00Z",
		"label": map[string]any{"name": "doing"},
	}}, nil)
	issueInput["transition_index"] = 0
	compareRowsAgainstPythonOracle(
		t,
		"github/work-items/status-transition",
		[]oracleCase{
			{ID: "merged_after_reopen", Input: input},
			{ID: "issue_labeled_in_progress", Input: issueInput},
		},
		buildGitHubStatusTransitionOracleRow,
		nil,
	)
}

func TestGitHubWorkItemReopenMatchesLivePythonProductionRow(t *testing.T) {
	input := githubIssueSemanticOracleInput(
		map[string]any{
			"number": 42, "title": "Repair delivery path", "state": "open",
			"created_at": "2026-08-01T08:00:00Z",
			"updated_at": "2026-08-03T09:30:00Z",
			"labels":     []any{}, "assignees": []any{},
		},
		[]any{map[string]any{
			"event": "reopened", "created_at": "2026-08-03T08:00:00Z",
			"actor": map[string]any{"login": "maintainer"},
		}}, nil,
	)
	input["work_item_id"] = "gh:acme/api#42"
	compareRowsAgainstPythonOracle(
		t,
		"github/work-items/reopen",
		[]oracleCase{{ID: "reopened_by_maintainer", Input: input}},
		buildGitHubReopenOracleRow,
		nil,
	)
}

func TestGitHubWorkItemInteractionMatchesLivePythonProductionRow(t *testing.T) {
	comment := map[string]any{
		"id": 99, "body": "Looks good 👋",
		"created_at": "2026-08-03T08:30:00Z",
		"user":       map[string]any{"login": "reviewer"},
	}
	input := githubIssueSemanticOracleInput(
		map[string]any{
			"number": 42, "title": "Repair delivery path", "state": "open",
			"created_at": "2026-08-01T08:00:00Z",
			"updated_at": "2026-08-03T09:30:00Z",
			"labels":     []any{}, "assignees": []any{},
		}, nil, []any{comment},
	)
	input["work_item_id"] = "gh:acme/api#42"
	input["raw_comment"] = comment
	compareRowsAgainstPythonOracle(
		t,
		"github/work-items/interaction",
		[]oracleCase{{ID: "unicode_comment", Input: input}},
		buildGitHubInteractionOracleRow,
		nil,
	)
}

func TestGitHubWorkItemDependencyMatchesLivePythonProductionRows(t *testing.T) {
	bodyInput := githubIssueSemanticOracleInput(
		map[string]any{
			"number": 42, "title": "Repair delivery path",
			"body": "Blocked by #7", "state": "open",
			"created_at": "2026-08-01T08:00:00Z",
			"updated_at": "2026-08-03T09:30:00Z",
			"labels":     []any{}, "assignees": []any{},
		}, nil, nil,
	)
	bodyInput["producer"] = "body"
	bodyInput["work_item_id"] = "gh:acme/api#42"
	comment := map[string]any{
		"id":         101,
		"body":       "Blocked by https://linear.app/fullchaos/issue/CHAOS-99/task",
		"created_at": "2026-08-03T08:00:00Z",
		"user":       map[string]any{"login": "linear[bot]"},
	}
	commentInput := githubPullRequestSemanticOracleInput(map[string]any{
		"number": 17, "title": "Repair delivery path", "body": "Routine repair",
		"state": "open", "created_at": "2026-08-01T08:00:00Z",
		"updated_at": "2026-08-03T09:30:00Z",
		"labels":     []any{}, "assignees": []any{},
		"user": map[string]any{"login": "author"},
		"head": map[string]any{"ref": "feature/repair"},
	}, nil)
	commentInput["producer"] = "comment"
	commentInput["work_item_id"] = "ghpr:acme/api#17"
	commentInput["raw_comments"] = []any{comment}
	compareRowsAgainstPythonOracle(
		t,
		"github/work-items/dependency",
		[]oracleCase{
			{ID: "body_reverse_blocker", Input: bodyInput},
			{ID: "trusted_linear_comment", Input: commentInput},
		},
		buildGitHubDependencyOracleRow,
		nil,
	)
}

func TestGitHubAIAttributionMatchesLivePythonProductionRows(t *testing.T) {
	base := map[string]any{
		"number": 17, "title": "Repair delivery path", "body": "Routine repair",
		"state": "open", "created_at": "2026-08-01T08:00:00Z",
		"updated_at": "2026-08-03T09:30:00Z", "assignees": []any{},
		"head": map[string]any{"ref": "feature/repair"},
	}
	labelPR := cloneSemanticOracleMap(base)
	labelPR["labels"] = []any{map[string]any{"name": "codex"}}
	labelPR["user"] = map[string]any{"login": "author", "type": "User"}
	labelInput := githubPullRequestSemanticOracleInput(labelPR, nil)
	labelInput["signal_source"] = "pr_label"
	botPR := cloneSemanticOracleMap(base)
	botPR["labels"] = []any{}
	botPR["user"] = map[string]any{"login": "chatgpt-codex[bot]", "type": "Bot"}
	botInput := githubPullRequestSemanticOracleInput(botPR, nil)
	botInput["signal_source"] = "bot_author"
	compareRowsAgainstPythonOracle(
		t,
		"github/work-items/ai-attribution",
		[]oracleCase{
			{ID: "codex_label", Input: labelInput},
			{ID: "known_bot_author", Input: botInput},
		},
		buildGitHubAIAttributionOracleRow,
		nil,
	)
}

func TestGitHubSemanticRowsAreStableAcrossCompleteUnitRetry(t *testing.T) {
	comment := map[string]any{
		"id": 101, "body": "Looks good",
		"created_at": "2026-08-03T08:00:00Z",
		"user":       map[string]any{"login": "reviewer"},
	}
	input := githubPullRequestSemanticOracleInput(map[string]any{
		"number": 17, "title": "Repair delivery path",
		"body": "Blocked by #7", "state": "open",
		"created_at": "2026-08-01T08:00:00Z",
		"updated_at": "2026-08-03T09:30:00Z",
		"labels":     []any{map[string]any{"name": "codex"}},
		"assignees":  []any{}, "user": map[string]any{"login": "author"},
		"head": map[string]any{"ref": "feature/repair"},
	}, []any{map[string]any{
		"event": "reopened", "created_at": "2026-08-02T09:00:00Z",
		"actor": map[string]any{"login": "maintainer"},
	}})
	input["raw_comments"] = []any{comment}

	first := buildGitHubPullRequestSemanticOracleRows(t, input)
	second := buildGitHubPullRequestSemanticOracleRows(t, input)
	if !reflect.DeepEqual(first, second) {
		t.Fatalf("complete semantic rows changed across retry:\nfirst=%+v\nsecond=%+v", first, second)
	}
}

func githubPullRequestSemanticOracleInput(rawPR map[string]any, events []any) map[string]any {
	return map[string]any{
		"repo_full_name": "acme/api",
		"repo_id":        "c7198fbc-1945-3717-05d8-eb78866b4e79",
		"org_id":         "77777777-7777-4777-8777-777777777777",
		"raw_pr":         rawPR, "raw_events": events,
	}
}

func githubIssueSemanticOracleInput(rawIssue map[string]any, events, comments []any) map[string]any {
	return map[string]any{
		"repo_full_name": "acme/api",
		"repo_id":        "c7198fbc-1945-3717-05d8-eb78866b4e79",
		"org_id":         "77777777-7777-4777-8777-777777777777",
		"raw_issue":      rawIssue, "raw_events": events, "raw_comments": comments,
	}
}

func buildGitHubPullRequestWorkItemOracleRow(t *testing.T, input map[string]any) githubWorkItemRow {
	return buildGitHubPullRequestSemanticOracleRows(t, input).WorkItems[0]
}

func buildGitHubStatusTransitionOracleRow(t *testing.T, input map[string]any) githubWorkItemTransitionRow {
	if _, isPullRequest := input["raw_pr"]; isPullRequest {
		return buildGitHubPullRequestSemanticOracleRows(t, input).StatusTransitions[int(input["transition_index"].(int))]
	}
	return buildGitHubIssueSemanticOracleRows(t, input).StatusTransitions[int(input["transition_index"].(int))]
}

func buildGitHubReopenOracleRow(t *testing.T, input map[string]any) githubWorkItemReopenRow {
	return buildGitHubIssueSemanticOracleRows(t, input).ReopenEvents[0]
}

func buildGitHubInteractionOracleRow(t *testing.T, input map[string]any) githubWorkItemInteractionRow {
	return buildGitHubIssueSemanticOracleRows(t, input).Interactions[0]
}

func buildGitHubDependencyOracleRow(t *testing.T, input map[string]any) githubWorkItemDependencyRow {
	if input["producer"] == "body" {
		return buildGitHubIssueSemanticOracleRows(t, input).Dependencies[0]
	}
	return buildGitHubPullRequestSemanticOracleRows(t, input).Dependencies[0]
}

func buildGitHubAIAttributionOracleRow(t *testing.T, input map[string]any) githubAIAttributionRow {
	rows := buildGitHubPullRequestSemanticOracleRows(t, input).AIAttributions
	for _, row := range rows {
		if row.Source == input["signal_source"] {
			return row
		}
	}
	t.Fatalf("missing Go attribution source %q", input["signal_source"])
	return githubAIAttributionRow{}
}

func buildGitHubPullRequestSemanticOracleRows(t *testing.T, input map[string]any) githubWorkItemRows {
	t.Helper()
	claim := githubWorkItemOracleClaim()
	claim.OrgID = input["org_id"].(string)
	rows, err := normalizeGitHubPullRequestBundle(
		claim, input["repo_full_name"].(string), uuid.MustParse(input["repo_id"].(string)),
		marshalSemanticOracleValue(t, input["raw_pr"]),
		marshalSemanticOracleValues(t, input["raw_events"]),
		marshalSemanticOracleValues(t, input["raw_comments"]),
		nil, githubWorkItemOracleNormalizedAt,
	)
	if err != nil {
		t.Fatal(err)
	}
	return rows
}

func buildGitHubIssueSemanticOracleRows(t *testing.T, input map[string]any) githubWorkItemRows {
	t.Helper()
	claim := githubWorkItemOracleClaim()
	claim.OrgID = input["org_id"].(string)
	rows, err := normalizeGitHubIssueBundle(
		claim, input["repo_full_name"].(string), uuid.MustParse(input["repo_id"].(string)),
		marshalSemanticOracleValue(t, input["raw_issue"]),
		marshalSemanticOracleValues(t, input["raw_events"]),
		marshalSemanticOracleValues(t, input["raw_comments"]),
		nil, githubWorkItemOracleNormalizedAt,
	)
	if err != nil {
		t.Fatal(err)
	}
	return rows
}

func marshalSemanticOracleValues(t *testing.T, value any) []json.RawMessage {
	t.Helper()
	if value == nil {
		return nil
	}
	items := value.([]any)
	rows := make([]json.RawMessage, 0, len(items))
	for _, item := range items {
		rows = append(rows, marshalSemanticOracleValue(t, item))
	}
	return rows
}

func marshalSemanticOracleValue(t *testing.T, value any) json.RawMessage {
	t.Helper()
	raw, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

func cloneSemanticOracleMap(source map[string]any) map[string]any {
	result := make(map[string]any, len(source))
	for key, value := range source {
		result[key] = value
	}
	return result
}
