package providersync

import (
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
)

// The per-destination differential for the effects layer.
//
// The existing oracle_pairs cover the SEMANTIC row -- what the provider
// normalises. They stop before the sink, so nothing until now compared the
// INSERT PROJECTION: which columns this unit writes, in what order, with which
// coercions. That projection is where a port silently diverges, because every
// omission and every `or ""` is invisible to a row-level comparison.
//
// testdata/python_work_item_sink_oracle.py executes the real production sink
// (ClickHouseMetricsSink, constructed with a recording client) and reports the
// column list and value matrix it would have inserted. This test runs the Go
// adapter's projection over the same row and compares both, with types
// preserved on the wire so an int cannot pass as a float or a UUID as a string.
func TestDirectAdapterProjectionsMatchTheLivePythonSink(t *testing.T) {
	python := pythonExecutable(t)
	// A negative-offset evening: the local date and the UTC date disagree, so
	// any accidental local-time coercion on either side shows up as a wrong day
	// rather than passing by coincidence.
	normalizedAt := time.Date(2026, 8, 31, 23, 30, 0, 0, time.UTC)
	orgID := "org-acme"
	repoID := uuid.MustParse("c7198fbc-1945-3717-05d8-eb78866b4e79")

	// Every value in a positionally-indistinguishable group is DISTINCT on
	// purpose. work_items writes eight consecutive String columns
	// (status_raw, project_key, project_id, native_team_key, project_name,
	// reporter, sprint_id, sprint_name, parent_id, epic_id, url) and five
	// consecutive timestamps; if any two carried the same value a transposition
	// of those columns would compare equal and the oracle would pass while the
	// projection was wrong.
	storyPoints := 100000.0 // integral on purpose: repr() vs 'g' disagree here
	workItem := githubWorkItemRow{
		WorkItemID: "gh:acme/api#42", Provider: "github", Title: "Repair path",
		Type: "issue", Status: "doing", StatusRaw: stringPointer("open"),
		ProjectKey:    stringPointer("project-key-value"),
		NativeTeamKey: stringPointer("native-team-key-value"),
		ProjectName:   stringPointer("project-name-value"),
		SprintID:      stringPointer("sprint-id-value"),
		SprintName:    stringPointer("sprint-name-value"),
		ParentID:      stringPointer("parent-id-value"),
		EpicID:        stringPointer("epic-id-value"),
		StoryPoints:   &storyPoints,
		// Set on purpose: write_work_items must NOT carry these four to the
		// table, and a projection that "helpfully" added them diverges here.
		Description: stringPointer("body"), PriorityRaw: stringPointer("p1"),
		ServiceClass: stringPointer("expedite"),
		RepoID:       &repoID, ProjectID: stringPointer("acme/api"),
		Assignees: []string{"dev", "second-assignee"},
		Reporter:  stringPointer("reporter"),
		CreatedAt: normalizedAt.Add(-72 * time.Hour),
		UpdatedAt: normalizedAt.Add(-71 * time.Hour),
		Labels:    []string{"bug", "p1", "third-label"},
		URL:       stringPointer("https://github.com/acme/api/issues/42"),
		OrgID:     orgID, LastSynced: normalizedAt,
	}
	dueAt := normalizedAt.Add(48 * time.Hour)
	workItem.DueAt = &dueAt
	startedAtItem := normalizedAt.Add(-70 * time.Hour)
	completedAtItem := normalizedAt.Add(-69 * time.Hour)
	closedAtItem := normalizedAt.Add(-68 * time.Hour)
	workItem.StartedAt = &startedAtItem
	workItem.CompletedAt = &completedAtItem
	workItem.ClosedAt = &closedAtItem

	transition := githubWorkItemTransitionRow{
		WorkItemID: "gh:acme/api#42", Provider: "github",
		OccurredAt: normalizedAt.Add(-2 * time.Hour), FromStatus: "todo",
		ToStatus: "doing", ToStatusRaw: stringPointer("doing"),
		OrgID: orgID, LastSynced: normalizedAt,
	}
	dependency := githubWorkItemDependencyRow{
		SourceWorkItemID: "gh:acme/api#42", TargetWorkItemID: "gh:acme/api#7",
		RelationshipType: "blocks", RelationshipTypeRaw: "blocks",
		RelationshipSemanticsVersion: "canonical-blocks.v2",
		LastSynced:                   normalizedAt, OrgID: orgID,
	}
	reopen := githubWorkItemReopenRow{
		WorkItemID: "gh:acme/api#42", OccurredAt: normalizedAt.Add(-3 * time.Hour),
		FromStatus: "done", ToStatus: "doing", LastSynced: normalizedAt,
		OrgID: orgID,
	}
	interaction := githubWorkItemInteractionRow{
		WorkItemID: "gh:acme/api#42", Provider: "github",
		InteractionType: "comment", OccurredAt: normalizedAt.Add(-4 * time.Hour),
		Actor: stringPointer("dev"), BodyLength: 128, LastSynced: normalizedAt,
		OrgID: orgID,
	}
	startedAt := normalizedAt.Add(-240 * time.Hour)
	endedAt := normalizedAt.Add(-239 * time.Hour)
	completedAt := normalizedAt.Add(-238 * time.Hour)
	sprint := githubSprintRow{
		Provider: "github", SprintID: "gh:acme/api/milestone/3",
		Name: stringPointer("Sprint 3"), State: stringPointer("open"),
		NativeTeamKey: stringPointer("sprint-team-key"),
		StartedAt:     &startedAt, EndedAt: &endedAt, CompletedAt: &completedAt,
		LastSynced: normalizedAt, OrgID: orgID,
	}

	// A second, DIFFERENT work item so the multi-row case can catch a
	// projection that is right for one row and wrong for the next (a captured
	// loop variable, a shared buffer, a per-batch value hoisted by mistake).
	secondItem := workItem
	secondItem.WorkItemID = "gh:acme/api#43"
	secondItem.Title = "Second item"
	secondItem.Status = "todo"
	secondItem.Assignees = []string{"other"}
	secondItem.Labels = []string{}
	secondPoints := 0.5
	secondItem.StoryPoints = &secondPoints

	cases := []struct {
		id          string
		destination string
		sourceRows  []any
		columns     string
		rows        [][]any
	}{
		{"work_items", "work_items", []any{workItem}, gitHubWorkItemsInsert,
			[][]any{projectWorkItem(workItem).values()}},
		{"work_items_multi_row", "work_items", []any{workItem, secondItem}, gitHubWorkItemsInsert,
			[][]any{projectWorkItem(workItem).values(), projectWorkItem(secondItem).values()}},
		{"work_item_transitions", "work_item_transitions", []any{transition}, gitHubWorkItemTransitionsInsert,
			[][]any{projectWorkItemTransition(transition).values()}},
		{"work_item_dependencies", "work_item_dependencies", []any{dependency}, gitHubWorkItemDependenciesInsert,
			[][]any{workItemDependencyValues(dependency)}},
		{"work_item_reopen_events", "work_item_reopen_events", []any{reopen}, gitHubWorkItemReopenEventsInsert,
			[][]any{workItemReopenValues(reopen)}},
		{"work_item_interactions", "work_item_interactions", []any{interaction}, gitHubWorkItemInteractionsInsert,
			[][]any{workItemInteractionValues(interaction)}},
		{"sprints", "sprints", []any{sprint}, gitHubSprintsInsert,
			[][]any{sprintValues(sprint)}},
	}

	payload := make([]map[string]any, 0, len(cases))
	for _, testCase := range cases {
		rows := make([]map[string]any, 0, len(testCase.sourceRows))
		for _, source := range testCase.sourceRows {
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
		payload = append(payload, map[string]any{
			"id": testCase.id, "destination": testCase.destination,
			"org_id": orgID, "rows": rows,
		})
	}

	_, currentFile, _, _ := runtime.Caller(0)
	packageDir := filepath.Dir(currentFile)
	casesFile := filepath.Join(t.TempDir(), "sink-cases.json")
	encoded, err := json.Marshal(payload)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(casesFile, encoded, 0o600); err != nil {
		t.Fatal(err)
	}
	output, err := exec.Command(
		python,
		filepath.Join(packageDir, "testdata", "python_work_item_sink_oracle.py"),
		casesFile,
	).CombinedOutput()
	if err != nil {
		t.Fatalf("execute Python sink oracle: %v: %s", err, output)
	}
	var decoded struct {
		Cases []struct {
			ID          string              `json:"id"`
			Table       string              `json:"table"`
			ColumnNames []string            `json:"column_names"`
			Rows        [][]json.RawMessage `json:"rows"`
		} `json:"cases"`
	}
	if err := json.Unmarshal(output, &decoded); err != nil {
		t.Fatalf("decode Python sink oracle output: %v: %s", err, output)
	}
	if len(decoded.Cases) != len(cases) {
		t.Fatalf("oracle returned %d cases, expected %d -- a missing case would "+
			"silently drop a destination from this comparison",
			len(decoded.Cases), len(cases))
	}

	byID := map[string]int{}
	for index, testCase := range cases {
		byID[testCase.id] = index
	}
	for _, result := range decoded.Cases {
		index, known := byID[result.ID]
		if !known {
			t.Fatalf("oracle returned unknown case %q", result.ID)
		}
		testCase := cases[index]
		t.Run(result.ID, func(t *testing.T) {
			goColumns := insertColumns(t, testCase.columns)
			if strings.Join(goColumns, ",") != strings.Join(result.ColumnNames, ",") {
				t.Fatalf("column list diverges from the live Python sink\npython=%v\ngo    =%v",
					result.ColumnNames, goColumns)
			}
			if len(result.Rows) != len(testCase.rows) {
				t.Fatalf("row count diverges: python=%d go=%d",
					len(result.Rows), len(testCase.rows))
			}
			for rowIndex, pythonRow := range result.Rows {
				compareSinkRow(t, result.ID, rowIndex, result.ColumnNames,
					pythonRow, testCase.rows[rowIndex])
			}
		})
	}
}

func compareSinkRow(
	t *testing.T,
	caseID string,
	rowIndex int,
	columns []string,
	pythonRow []json.RawMessage,
	goRow []any,
) {
	t.Helper()
	if len(pythonRow) != len(goRow) {
		t.Fatalf("row %d value count diverges: python=%d go=%d", rowIndex,
			len(pythonRow), len(goRow))
	}
	for position, pythonValue := range pythonRow {
		column := columns[position]
		goRendered, err := renderSinkValue(goRow[position])
		if err != nil {
			t.Fatalf("row %d column %q: %v", rowIndex, column, err)
		}
		pythonRendered := renderPythonSinkValue(t, pythonValue)
		// last_synced on the two shape-A destinations is the one column Python
		// stamps from wall-clock inside the writer, so its value is
		// unreproducible by construction. Everything about it except the
		// instant is still compared: it must be present, in this position, and
		// a datetime on both sides.
		if column == "last_synced" && (strings.HasPrefix(caseID, "work_items") || caseID == "work_item_transitions") {
			if !strings.HasPrefix(pythonRendered, "datetime:") ||
				!strings.HasPrefix(goRendered, "datetime:") {
				t.Fatalf("row %d column %q: expected a datetime on both sides, python=%s go=%s",
					rowIndex, column, pythonRendered, goRendered)
			}
			continue
		}
		if pythonRendered != goRendered {
			t.Fatalf("row %d column %q diverges\npython=%s\ngo    =%s",
				rowIndex, column, pythonRendered, goRendered)
		}
	}
}

// insertColumns extracts the column list from an INSERT statement so the
// comparison uses the very string the writer executes, not a second copy of it.
func insertColumns(t *testing.T, statement string) []string {
	t.Helper()
	open := strings.Index(statement, "(")
	close := strings.LastIndex(statement, ")")
	if open < 0 || close < open {
		t.Fatalf("cannot parse column list from %q", statement)
	}
	parts := strings.Split(statement[open+1:close], ",")
	columns := make([]string, 0, len(parts))
	for _, part := range parts {
		columns = append(columns, strings.TrimSpace(part))
	}
	return columns
}

// renderSinkValue renders a Go insert value in the oracle's type-tagged shape.
// Unknown types are refused rather than formatted with %v, which would let a
// type change pass as long as its text happened to match.
func renderSinkValue(value any) (string, error) {
	switch typed := value.(type) {
	case nil:
		return "null", nil
	case string:
		return "str:" + typed, nil
	case bool:
		return "bool:" + strconv.FormatBool(typed), nil
	case int:
		return "int:" + strconv.Itoa(typed), nil
	case uint32:
		return "int:" + strconv.FormatUint(uint64(typed), 10), nil
	case float64:
		return canonicalFloat(typed), nil
	case *float64:
		if typed == nil {
			return "null", nil
		}
		return canonicalFloat(*typed), nil
	case *string:
		if typed == nil {
			return "null", nil
		}
		return "str:" + *typed, nil
	case []string:
		return "list:[" + strings.Join(typed, ",") + "]", nil
	case time.Time:
		return "datetime:" + typed.UTC().Format(time.RFC3339Nano), nil
	case *time.Time:
		if typed == nil {
			return "null", nil
		}
		return "datetime:" + typed.UTC().Format(time.RFC3339Nano), nil
	case uuid.UUID:
		return "uuid:" + typed.String(), nil
	case *uuid.UUID:
		if typed == nil {
			return "null", nil
		}
		return "uuid:" + typed.String(), nil
	default:
		return "", fmt.Errorf("unrenderable Go insert value type %T", value)
	}
}

// canonicalFloat renders a float in a form that round-trips exactly and does
// not depend on either language's default formatting. Python's repr() and Go's
// 'g' verb disagree on integral values (repr(100000.0) is "100000.0", 'g' is
// "100000"), so comparing the two languages' text would report a divergence
// where the numbers are equal. The hex form is exact and unambiguous.
func canonicalFloat(value float64) string {
	return "float:" + strconv.FormatFloat(value, 'x', -1, 64)
}

func renderPythonSinkValue(t *testing.T, raw json.RawMessage) string {
	t.Helper()
	trimmed := strings.TrimSpace(string(raw))
	if trimmed == "null" {
		return "null"
	}
	var list []struct {
		Tag   string `json:"t"`
		Value string `json:"v"`
	}
	if err := json.Unmarshal(raw, &list); err == nil {
		items := make([]string, 0, len(list))
		for _, item := range list {
			items = append(items, item.Value)
		}
		return "list:[" + strings.Join(items, ",") + "]"
	}
	var tagged struct {
		Tag   string `json:"t"`
		Value string `json:"v"`
	}
	if err := json.Unmarshal(raw, &tagged); err != nil {
		t.Fatalf("decode python value %s: %v", raw, err)
	}
	switch tagged.Tag {
	case "str":
		return "str:" + tagged.Value
	case "bool":
		return "bool:" + tagged.Value
	case "int":
		return "int:" + tagged.Value
	case "float":
		parsed, err := strconv.ParseFloat(tagged.Value, 64)
		if err != nil {
			t.Fatalf("parse python float %q: %v", tagged.Value, err)
		}
		return canonicalFloat(parsed)
	case "uuid":
		return "uuid:" + tagged.Value
	case "datetime":
		parsed, err := time.Parse(time.RFC3339Nano, tagged.Value)
		if err != nil {
			t.Fatalf("parse python datetime %q: %v", tagged.Value, err)
		}
		return "datetime:" + parsed.UTC().Format(time.RFC3339Nano)
	default:
		t.Fatalf("unknown python value tag %q", tagged.Tag)
		return ""
	}
}
