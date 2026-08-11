package providersync

import (
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"
)

// Jira uses the same six Python sink methods as the existing direct adapter,
// but this test runs the real producer with Jira-shaped rows and keeps the
// provider value distinct. That prevents a future provider-specific shortcut
// from silently changing a shared projection while the GitHub fixture stays
// green.
func TestJiraDirectAdapterProjectionsMatchLivePythonSink(t *testing.T) {
	python := pythonExecutable(t)
	normalizedAt := time.Date(2026, 8, 31, 23, 30, 0, 123000000, time.UTC)
	orgID := "org-acme"
	points := 3.5
	started := normalizedAt.Add(-72 * time.Hour)
	completed := normalizedAt.Add(-70 * time.Hour)
	closed := normalizedAt.Add(-69 * time.Hour)
	workItem := jiraWorkItemRow{
		WorkItemID: "jira:OPS-101", Provider: "jira", Title: "Repair path", Type: "bug", Status: "done",
		StatusRaw: stringPointer("Resolved"), Description: stringPointer("body"),
		ProjectKey: stringPointer("project-key"), ProjectID: stringPointer("project-id"),
		ProjectName: stringPointer("project-name"), Assignees: []string{"dev", "second"},
		Reporter: stringPointer("reporter"), CreatedAt: normalizedAt.Add(-72 * time.Hour),
		UpdatedAt: normalizedAt.Add(-71 * time.Hour), StartedAt: &started,
		CompletedAt: &completed, ClosedAt: &closed, Labels: []string{"bug", "priority"},
		StoryPoints: &points, SprintID: stringPointer("9001"), SprintName: stringPointer("sprint-name"),
		ParentID: stringPointer("jira:OPS-90"), EpicID: stringPointer("jira:EPIC-9"),
		URL:         stringPointer("https://acme.atlassian.net/browse/OPS-101"),
		PriorityRaw: stringPointer("Highest"), ServiceClass: stringPointer("expedite"),
		DueAt: func() *time.Time { value := normalizedAt.Add(48 * time.Hour); return &value }(),
		OrgID: orgID, LastSynced: normalizedAt,
	}
	transition := jiraWorkItemTransitionRow{
		WorkItemID: workItem.WorkItemID, Provider: "jira", OccurredAt: normalizedAt.Add(-2 * time.Hour),
		FromStatus: "todo", ToStatus: "done", FromStatusRaw: stringPointer("To Do"),
		ToStatusRaw: stringPointer("Resolved"), Actor: stringPointer("dev"), OrgID: orgID, LastSynced: normalizedAt,
	}
	dependency := jiraWorkItemDependencyRow{
		SourceWorkItemID: workItem.WorkItemID, TargetWorkItemID: "jira:OPS-102", RelationshipType: "blocks",
		RelationshipTypeRaw: "blocks", RelationshipSemanticsVersion: "canonical-blocks.v2", LastSynced: normalizedAt, OrgID: orgID,
	}
	reopen := jiraWorkItemReopenRow{
		WorkItemID: workItem.WorkItemID, OccurredAt: normalizedAt.Add(-3 * time.Hour), FromStatus: "done", ToStatus: "todo",
		FromStatusRaw: stringPointer("Resolved"), ToStatusRaw: stringPointer("To Do"), Actor: stringPointer("dev"), LastSynced: normalizedAt, OrgID: orgID,
	}
	interaction := jiraWorkItemInteractionRow{
		WorkItemID: workItem.WorkItemID, Provider: "jira", InteractionType: "comment",
		OccurredAt: normalizedAt.Add(-4 * time.Hour), Actor: stringPointer("dev"), BodyLength: 128, LastSynced: normalizedAt, OrgID: orgID,
	}
	sprint := jiraSprintRow{
		Provider: "jira", SprintID: "9001", Name: stringPointer("Sprint 9001"), State: stringPointer("closed"),
		StartedAt:   func() *time.Time { value := normalizedAt.Add(-240 * time.Hour); return &value }(),
		EndedAt:     func() *time.Time { value := normalizedAt.Add(-239 * time.Hour); return &value }(),
		CompletedAt: func() *time.Time { value := normalizedAt.Add(-238 * time.Hour); return &value }(),
		LastSynced:  normalizedAt, OrgID: orgID,
	}
	cases := []struct {
		id, destination, columns string
		rows                     []any
		goRows                   [][]any
	}{
		{"work_items_jira", "work_items", gitHubWorkItemsInsert, []any{workItem}, [][]any{projectWorkItem(workItem).values()}},
		{"work_item_transitions", "work_item_transitions", gitHubWorkItemTransitionsInsert, []any{transition}, [][]any{projectWorkItemTransition(transition).values()}},
		{"jira_work_item_dependencies", "work_item_dependencies", gitHubWorkItemDependenciesInsert, []any{dependency}, [][]any{workItemDependencyValues(dependency)}},
		{"jira_work_item_reopens", "work_item_reopen_events", gitHubWorkItemReopenEventsInsert, []any{reopen}, [][]any{workItemReopenValues(reopen)}},
		{"jira_work_item_interactions", "work_item_interactions", gitHubWorkItemInteractionsInsert, []any{interaction}, [][]any{workItemInteractionValues(interaction)}},
		{"jira_sprints", "sprints", gitHubSprintsInsert, []any{sprint}, [][]any{sprintValues(sprint)}},
	}
	payload := make([]map[string]any, 0, len(cases))
	for _, testCase := range cases {
		rows := make([]map[string]any, 0, len(testCase.rows))
		for _, source := range testCase.rows {
			encoded, err := json.Marshal(source)
			if err != nil {
				t.Fatal(err)
			}
			var row map[string]any
			if err := json.Unmarshal(encoded, &row); err != nil {
				t.Fatal(err)
			}
			rows = append(rows, row)
		}
		payload = append(payload, map[string]any{"id": testCase.id, "destination": testCase.destination, "org_id": orgID, "rows": rows})
	}
	_, currentFile, _, _ := runtime.Caller(0)
	casesFile := filepath.Join(t.TempDir(), "jira-sink-cases.json")
	encoded, err := json.Marshal(payload)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(casesFile, encoded, 0o600); err != nil {
		t.Fatal(err)
	}
	output, err := exec.Command(python, filepath.Join(filepath.Dir(currentFile), "testdata", "python_work_item_sink_oracle.py"), casesFile).CombinedOutput()
	if err != nil {
		t.Fatalf("execute live Python sink oracle: %v: %s", err, output)
	}
	var decoded struct {
		Cases []struct {
			ID          string              `json:"id"`
			ColumnNames []string            `json:"column_names"`
			Rows        [][]json.RawMessage `json:"rows"`
		} `json:"cases"`
	}
	if err := json.Unmarshal(output, &decoded); err != nil {
		t.Fatalf("decode sink oracle: %v: %s", err, output)
	}
	if len(decoded.Cases) != len(cases) {
		t.Fatalf("oracle returned %d cases want %d", len(decoded.Cases), len(cases))
	}
	byID := make(map[string]int, len(cases))
	for index, testCase := range cases {
		byID[testCase.id] = index
	}
	for _, result := range decoded.Cases {
		index, ok := byID[result.ID]
		if !ok {
			t.Fatalf("oracle returned unknown case %q", result.ID)
		}
		testCase := cases[index]
		t.Run(result.ID, func(t *testing.T) {
			goColumns := insertColumns(t, testCase.columns)
			if strings.Join(goColumns, ",") != strings.Join(result.ColumnNames, ",") {
				t.Fatalf("column list diverges: python=%v go=%v", result.ColumnNames, goColumns)
			}
			if len(result.Rows) != len(testCase.goRows) {
				t.Fatalf("row count diverges python=%d go=%d", len(result.Rows), len(testCase.goRows))
			}
			for rowIndex, pythonRow := range result.Rows {
				compareSinkRow(t, result.ID, rowIndex, result.ColumnNames, pythonRow, testCase.goRows[rowIndex])
			}
		})
	}
}
