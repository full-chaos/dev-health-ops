package providersync

import (
	"context"
	"encoding/json"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/full-chaos/dev-health-ops/internal/storedversion"
	"github.com/google/uuid"
)

type heldVersionColumnType struct {
	driver.ColumnType
	name string
	scan reflect.Type
}

func (column heldVersionColumnType) Name() string           { return column.name }
func (column heldVersionColumnType) ScanType() reflect.Type { return column.scan }

type heldVersionRows struct {
	driver.Rows
	types []heldVersionColumnType
	rows  [][]any
	next  int
}

func (rows *heldVersionRows) Next() bool   { rows.next++; return rows.next <= len(rows.rows) }
func (rows *heldVersionRows) Close() error { return nil }
func (rows *heldVersionRows) Err() error   { return nil }
func (rows *heldVersionRows) ColumnTypes() []driver.ColumnType {
	out := make([]driver.ColumnType, len(rows.types))
	for i, column := range rows.types {
		out[i] = column
	}
	return out
}

func (rows *heldVersionRows) Scan(dest ...any) error {
	for i, target := range dest {
		value := reflect.ValueOf(target).Elem()
		if held := rows.rows[rows.next-1][i]; held != nil {
			value.Set(reflect.ValueOf(held))
		}
	}
	return nil
}

type heldVersionBatch struct {
	driver.Batch
	appended *[][]any
}

func (batch heldVersionBatch) Append(values ...any) error {
	*batch.appended = append(*batch.appended, values)
	return nil
}
func (batch heldVersionBatch) Abort() error { return nil }
func (batch heldVersionBatch) Send() error  { return nil }

// heldVersionConn serves the held version of each key from heldDueAt and
// description/priority_raw/service_class values a push wrote, and records the
// rows the work_items writer appends.
type heldVersionConn struct {
	driver.Conn
	held     map[string]*time.Time
	repos    map[string]uuid.UUID
	appended [][]any
}

func (conn *heldVersionConn) Query(_ context.Context, query string, _ ...any) (driver.Rows, error) {
	selected := strings.Split(strings.TrimSuffix(strings.SplitN(strings.TrimPrefix(query, "SELECT "), " FROM ", 2)[0], " "), ", ")
	scans := map[string]reflect.Type{
		"repo_id": reflect.TypeOf(uuid.UUID{}), "work_item_id": reflect.TypeOf(""),
		"description": reflect.TypeOf((*string)(nil)), "priority_raw": reflect.TypeOf((*string)(nil)),
		"service_class": reflect.TypeOf((*string)(nil)), "due_at": reflect.TypeOf((*time.Time)(nil)),
	}
	rows := &heldVersionRows{}
	for _, column := range selected {
		rows.types = append(rows.types, heldVersionColumnType{name: column, scan: scans[column]})
	}
	pushed := "pushed body"
	for workItemID, due := range conn.held {
		line := make([]any, len(selected))
		for i, column := range selected {
			switch column {
			case "repo_id":
				line[i] = conn.repos[workItemID]
			case "work_item_id":
				line[i] = workItemID
			case "description":
				line[i] = &pushed
			case "priority_raw":
				p0 := "p0"
				line[i] = &p0
			case "service_class":
				expedite := "expedite"
				line[i] = &expedite
			case "due_at":
				line[i] = due
			}
		}
		rows.rows = append(rows.rows, line)
	}
	return rows, nil
}

func (conn *heldVersionConn) PrepareBatch(context.Context, string, ...driver.PrepareBatchOption) (driver.Batch, error) {
	return heldVersionBatch{appended: &conn.appended}, nil
}

func dueAtDate(year int, month time.Month, day int) *time.Time {
	value := time.Date(year, month, day, 0, 0, 0, 0, time.UTC)
	return &value
}

// storedDueAt runs rows through the production work_items writer against a
// store that holds heldDueAt for every key, and returns the due_at each
// appended row carries, keyed by work item id.
func storedDueAt(t *testing.T, insert string, held map[string]*time.Time, rows ...workItemStoredRow) map[string]*time.Time {
	t.Helper()
	conn := &heldVersionConn{held: held, repos: map[string]uuid.UUID{}}
	for _, row := range rows {
		conn.repos[row.WorkItemID] = row.RepoID
	}
	if err := writeWorkItemsKeepingHeldColumns(context.Background(), conn, "org-acme", insert, rows); err != nil {
		t.Fatal(err)
	}
	positions, err := storedversion.Positions(withHeldWorkItemColumns(insert))
	if err != nil {
		t.Fatal(err)
	}
	stored := map[string]*time.Time{}
	for _, values := range conn.appended {
		id := values[positions["work_item_id"]].(string)
		switch due := values[positions["due_at"]].(type) {
		case nil:
			stored[id] = nil
		case time.Time:
			stored[id] = &due
		case *time.Time:
			stored[id] = due
		default:
			t.Fatalf("%s: due_at is %T", id, due)
		}
	}
	return stored
}

func sameDueAt(a, b *time.Time) bool {
	if a == nil || b == nil {
		return a == nil && b == nil
	}
	return a.Equal(*b)
}

type dueAtProviderCase struct {
	name   string
	insert string
	// build returns the provider's normalizer row for an issue stating the
	// due date (empty: the issue states none), projected as the writer stores it.
	build func(t *testing.T, due string) workItemStoredRow
}

func gitlabDueAtRow(t *testing.T, due string) workItemStoredRow {
	issue := map[string]any{
		"iid": 51, "title": "Ship the due date", "state": "opened",
		"created_at": "2026-08-04T08:00:00Z", "updated_at": "2026-08-05T09:00:00Z",
		"labels": []any{}, "assignees": []any{},
	}
	if due != "" {
		issue["due_date"] = due
	}
	row := buildGitLabFetchWorkItemOracleRow(t, map[string]any{
		"repo_full_name": "acme/api", "repo_id": "c7198fbc-1945-3717-05d8-eb78866b4e79", "org_id": "org-acme",
		"since": "2026-08-01T00:00:00Z", "raw_issue": issue,
	})
	encoded, err := json.Marshal(row)
	if err != nil {
		t.Fatal(err)
	}
	var semantic githubWorkItemRow
	if err := json.Unmarshal(encoded, &semantic); err != nil {
		t.Fatal(err)
	}
	return projectWorkItem(semantic)
}

func jiraDueAtRow(t *testing.T, due string) workItemStoredRow {
	input := map[string]any{}
	for key, value := range jiraWorkItemOraclePrepCases()[0].Input {
		input[key] = value
	}
	raw := input["raw_issue"].(map[string]any)
	fields := map[string]any{}
	for key, value := range raw["fields"].(map[string]any) {
		fields[key] = value
	}
	fields["duedate"] = nil
	if due != "" {
		fields["duedate"] = due
	}
	claim := nativeTestClaim("jira", "work-items")
	claim.OrgID = input["org_id"].(string)
	row, _, err := normalizeJiraWorkItem(
		claim, jiraWorkItemFixtureInput{Raw: map[string]any{"key": raw["key"], "self": raw["self"], "fields": fields}},
		loadRealStatusMapping(t), jiraOracleIdentity, time.Date(2026, 8, 10, 12, 0, 0, 0, time.UTC),
	)
	if err != nil {
		t.Fatal(err)
	}
	return projectWorkItem(row)
}

func linearDueAtRow(t *testing.T, due string) workItemStoredRow {
	input := linearLongDescriptionInput("body")
	raw := input["raw_issue"].(map[string]any)
	raw["dueDate"] = nil
	if due != "" {
		raw["dueDate"] = due
	}
	return projectLinearWorkItem(buildLinearWorkItemOracleRow(t, input))
}

func dueAtProviderCases() []dueAtProviderCase {
	return []dueAtProviderCase{
		{"gitlab", gitHubWorkItemsInsert, gitlabDueAtRow},
		{"jira", gitHubWorkItemsInsert, jiraDueAtRow},
		{"linear", linearWorkItemsInsert, linearDueAtRow},
	}
}

// A due date a provider states reaches the stored row as that day at 00:00
// UTC, replaces a held value, and a later sync with a changed date replaces it
// again; a date-only value does not shift a day.
func TestProviderDueDateReachesTheStoredRow(t *testing.T) {
	for _, tc := range dueAtProviderCases() {
		t.Run(tc.name, func(t *testing.T) {
			stated := tc.build(t, "2026-08-20")
			id := stated.WorkItemID
			for name, held := range map[string]map[string]*time.Time{
				"nothing held": nil, "an older date held": {id: dueAtDate(2026, time.July, 1)},
			} {
				got := storedDueAt(t, tc.insert, held, stated)[id]
				if want := dueAtDate(2026, time.August, 20); !sameDueAt(got, want) {
					t.Errorf("%s: stored due_at=%v want %v", name, got, want)
				}
			}
			changed := tc.build(t, "2026-09-03")
			if got, want := storedDueAt(t, tc.insert, map[string]*time.Time{id: dueAtDate(2026, time.August, 20)}, changed)[id], dueAtDate(2026, time.September, 3); !sameDueAt(got, want) {
				t.Errorf("changed date: stored due_at=%v want %v", got, want)
			}
		})
	}
}

// A due date removed at the provider clears the stored value, whoever wrote
// it; a key with no value stays empty.
func TestProviderRemovedDueDateClearsTheStoredRow(t *testing.T) {
	for _, tc := range dueAtProviderCases() {
		t.Run(tc.name, func(t *testing.T) {
			removed := tc.build(t, "")
			id := removed.WorkItemID
			if removed.DueAt != nil {
				t.Fatalf("the normalizer states %v for an issue with no due date", removed.DueAt)
			}
			if got := storedDueAt(t, tc.insert, map[string]*time.Time{id: dueAtDate(2026, time.August, 20)}, removed)[id]; got != nil {
				t.Errorf("held date survived a sync that states none: %v", got)
			}
			if got, ok := storedDueAt(t, tc.insert, nil, removed)[id]; !ok || got != nil {
				t.Errorf("nothing held: stored due_at=%v present=%v want an empty row", got, ok)
			}
		})
	}
}

// GitHub issues have no due date; a milestone's due_on is the sprint's end.
// Nothing is invented from it, and a value another writer holds is kept.
func TestGitHubIssueStatesNoDueDateAndKeepsTheHeldOne(t *testing.T) {
	raw := map[string]any{
		"number": 9, "title": "In a milestone", "state": "open",
		"created_at": "2026-07-04T00:00:00Z", "updated_at": "2026-07-05T00:00:00Z",
		"labels": []any{}, "assignees": []any{}, "due_on": "2026-09-30T00:00:00Z",
		"milestone": map[string]any{"number": 3, "title": "Q3", "due_on": "2026-09-30T00:00:00Z"},
	}
	semantic := buildGitHubIssueWorkItemOracleRow(t, map[string]any{
		"repo_full_name": "acme/api", "repo_id": "c7198fbc-1945-3717-05d8-eb78866b4e79", "org_id": "org-acme", "raw_issue": raw,
	})
	if semantic.DueAt != nil {
		t.Fatalf("the github normalizer states due_at=%v for a milestone's due_on", semantic.DueAt)
	}
	row := projectWorkItem(semantic)
	stray := dueAtDate(2026, time.September, 30)
	carrying := row
	carrying.DueAt = stray
	if got, ok := storedDueAt(t, gitHubWorkItemsInsert, nil, carrying)[row.WorkItemID]; !ok || got != nil {
		t.Errorf("a github row carrying a date: stored due_at=%v present=%v want an empty row", got, ok)
	}
	if got, ok := storedDueAt(t, gitHubWorkItemsInsert, nil, row)[row.WorkItemID]; !ok || got != nil {
		t.Errorf("nothing held: stored due_at=%v present=%v want an empty row", got, ok)
	}
	pushed := dueAtDate(2026, time.June, 3)
	if got := storedDueAt(t, gitHubWorkItemsInsert, map[string]*time.Time{row.WorkItemID: pushed}, row)[row.WorkItemID]; !sameDueAt(got, pushed) {
		t.Errorf("held due_at=%v after a github sync, want %v kept", got, pushed)
	}
}

// One write can hold rows of both kinds: the github row keeps its held value
// and the gitlab row of the same write states its own.
func TestMixedWriteKeepsGitHubDueAtAndStatesGitLabs(t *testing.T) {
	gitlab := gitlabDueAtRow(t, "")
	github := projectWorkItem(buildGitHubIssueWorkItemOracleRow(t, map[string]any{
		"repo_full_name": "acme/api", "repo_id": "c7198fbc-1945-3717-05d8-eb78866b4e79", "org_id": "org-acme",
		"raw_issue": map[string]any{
			"number": 9, "title": "Plain", "state": "open", "created_at": "2026-07-04T00:00:00Z",
			"updated_at": "2026-07-05T00:00:00Z", "labels": []any{}, "assignees": []any{},
		},
	}))
	held := map[string]*time.Time{github.WorkItemID: dueAtDate(2026, time.June, 3), gitlab.WorkItemID: dueAtDate(2026, time.June, 4)}
	stored := storedDueAt(t, gitHubWorkItemsInsert, held, github, gitlab)
	if !sameDueAt(stored[github.WorkItemID], held[github.WorkItemID]) || stored[gitlab.WorkItemID] != nil {
		t.Errorf("github due_at=%v (want kept), gitlab due_at=%v (want cleared)", stored[github.WorkItemID], stored[gitlab.WorkItemID])
	}
}

// Every provider turns a date-only due date into the same instant, with no
// time-zone shift, in a process whose local zone is west of UTC.
func TestProviderDateOnlyDueDatesAreUTCMidnight(t *testing.T) {
	previous := time.Local
	time.Local = time.FixedZone("test-west", -7*60*60)
	t.Cleanup(func() { time.Local = previous })
	want := dueAtDate(2026, time.August, 20)
	for _, tc := range dueAtProviderCases() {
		if got := tc.build(t, "2026-08-20").DueAt; !sameDueAt(got, want) {
			t.Errorf("%s: due_at=%v want %v", tc.name, got, want)
		}
	}
}

// The prepared-route projection keeps due_at on the rows it commits, so the
// sink sees the date the provider stated.
func TestPreparedRouteProjectionKeepsDueAt(t *testing.T) {
	for _, provider := range []string{"github", "gitlab", "jira", "linear"} {
		keys, ok := preparedRouteColumns(provider, "work_items")
		if !ok || !keys["due_at"] {
			t.Errorf("%s: work_items projection keys=%v, want due_at kept", provider, keys)
		}
	}
}
