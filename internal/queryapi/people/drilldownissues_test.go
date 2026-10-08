package people

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"

	dhclickhouse "github.com/full-chaos/dev-health-go/clickhouse"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/sqlshape"
)

// issuesRowScanner replays a fixed slice of pre-built
// fetchPersonIssuesQuery result rows, in SELECT order (work_item_id,
// provider, status, team_id, cycle_time_hours, lead_time_hours,
// started_at, completed_at).
type issuesRowScanner struct {
	rows  [][]any
	index int
}

func (s *issuesRowScanner) Next() bool {
	if s.index >= len(s.rows) {
		return false
	}
	s.index++
	return true
}

func (s *issuesRowScanner) Scan(dest ...any) error {
	row := s.rows[s.index-1]
	*dest[0].(*string) = row[0].(string)
	*dest[1].(*string) = row[1].(string)
	*dest[2].(*string) = row[2].(string)
	*dest[3].(**string) = nilableString(row[3])
	*dest[4].(**float64) = nilableFloat(row[4])
	*dest[5].(**float64) = nilableFloat(row[5])
	*dest[6].(**time.Time) = nilableTime(row[6])
	*dest[7].(**time.Time) = nilableTime(row[7])
	return nil
}

func (s *issuesRowScanner) Err() error   { return nil }
func (s *issuesRowScanner) Close() error { return nil }

// TestFetchPersonIssuesQueryDedupsBothReads pins that both
// ReplacingMergeTree reads this query touches (work_item_cycle_times,
// work_item_team_attributions) are FINAL, matching internal/drilldown/
// issues.go's own fetchIssuesQuery for the same two tables.
func TestFetchPersonIssuesQueryDedupsBothReads(t *testing.T) {
	if !strings.Contains(fetchPersonIssuesQuery, "FROM work_item_cycle_times AS wct FINAL") {
		t.Fatalf("fetchPersonIssuesQuery missing wct FINAL:\n%s", fetchPersonIssuesQuery)
	}
	if !strings.Contains(primaryWorkItemTeamAttributionSourceForPerson, "FROM work_item_team_attributions FINAL") {
		t.Fatalf("primaryWorkItemTeamAttributionSourceForPerson missing FINAL:\n%s", primaryWorkItemTeamAttributionSourceForPerson)
	}
}

// TestFetchPersonIssuesQueryOrgFilterSameDepthAsFinal pins the class
// ruling this query follows: wct.org_id sits at the SAME nesting depth as
// the wct FINAL read it scopes -- the top-level statement, not a subquery
// an outer WHERE only narrows afterward.
func TestFetchPersonIssuesQueryOrgFilterSameDepthAsFinal(t *testing.T) {
	const finalMarker = "FROM work_item_cycle_times AS wct FINAL"
	const orgPredicate = "wct.org_id = {org_id:String}"
	depths := sqlshape.Depths(fetchPersonIssuesQuery)

	finalIdx := strings.Index(fetchPersonIssuesQuery, finalMarker)
	if finalIdx == -1 {
		t.Fatalf("fetchPersonIssuesQuery missing %q:\n%s", finalMarker, fetchPersonIssuesQuery)
	}
	orgIdx := strings.Index(fetchPersonIssuesQuery, orgPredicate)
	if orgIdx == -1 {
		t.Fatalf("fetchPersonIssuesQuery missing %q:\n%s", orgPredicate, fetchPersonIssuesQuery)
	}
	if depths[finalIdx] != depths[orgIdx] {
		t.Fatalf("%q sits at nesting depth %d but %q sits at depth %d", orgPredicate, depths[orgIdx], finalMarker, depths[finalIdx])
	}
}

// TestPrimaryWorkItemTeamAttributionSourceForPersonOrgFilterInside pins
// that the team-attribution subquery's own org_id predicate sits inside
// ITS OWN FINAL-deduped statement, matching internal/drilldown/issues.go's
// own identical copy.
func TestPrimaryWorkItemTeamAttributionSourceForPersonOrgFilterInside(t *testing.T) {
	const finalMarker = "FROM work_item_team_attributions FINAL"
	const orgPredicate = "org_id = {org_id:String}"
	depths := sqlshape.Depths(primaryWorkItemTeamAttributionSourceForPerson)
	finalIdx := strings.Index(primaryWorkItemTeamAttributionSourceForPerson, finalMarker)
	orgIdx := strings.Index(primaryWorkItemTeamAttributionSourceForPerson, orgPredicate)
	if finalIdx == -1 || orgIdx == -1 {
		t.Fatalf("expected both %q and %q in:\n%s", finalMarker, orgPredicate, primaryWorkItemTeamAttributionSourceForPerson)
	}
	if depths[finalIdx] != depths[orgIdx] {
		t.Fatalf("org_id predicate depth %d != FINAL marker depth %d", depths[orgIdx], depths[finalIdx])
	}
}

func TestBuildDrilldownIssuesResponseNilReader(t *testing.T) {
	_, err := BuildDrilldownIssuesResponse(context.Background(), nil, "org-1", DrilldownIssuesParams{})
	if !errors.Is(err, ErrUnavailable) {
		t.Fatalf("err = %v, want ErrUnavailable", err)
	}
}

// TestBuildDrilldownIssuesResponsePersonNotFound pins the 404-shaped
// RequestError build_person_drilldown_issues_response's own `raise
// ValueError("person not found")` maps to (main.py:1175-1176).
func TestBuildDrilldownIssuesResponsePersonNotFound(t *testing.T) {
	t.Setenv("IDENTITY_MAPPING_PATH", t.TempDir()+"/missing.yaml")
	client := personIdentityDispatchClient{identity: "", t: t, handler: func(t *testing.T, query string, bindings []dhclickhouse.Binding) (dhclickhouse.RowScanner, error) {
		t.Fatalf("unexpected query for an unresolved identity: %s", query)
		return nil, nil
	}}
	reader, err := NewReader(client)
	if err != nil {
		t.Fatalf("NewReader: %v", err)
	}
	_, err = BuildDrilldownIssuesResponse(context.Background(), reader, "org-1", DrilldownIssuesParams{PersonID: "nobody", RangeDays: 14, Limit: 50, Now: dt(2024, 6, 15, 0, 0, 0)})
	reqErr, ok := AsRequestError(err)
	if !ok || reqErr.Status != 404 || reqErr.Message != "Person not found" {
		t.Fatalf("err = %v, want RequestError{404, Person not found}", err)
	}
}

func TestBuildDrilldownIssuesResponseQueryError(t *testing.T) {
	t.Setenv("IDENTITY_MAPPING_PATH", t.TempDir()+"/missing.yaml")
	boom := errors.New("boom")
	client := personIdentityDispatchClient{identity: "alice@example.com", t: t, handler: func(t *testing.T, query string, bindings []dhclickhouse.Binding) (dhclickhouse.RowScanner, error) {
		return nil, boom
	}}
	reader, err := NewReader(client)
	if err != nil {
		t.Fatalf("NewReader: %v", err)
	}
	_, err = BuildDrilldownIssuesResponse(context.Background(), reader, "org-1", DrilldownIssuesParams{PersonID: "anyone", RangeDays: 14, Limit: 50, Now: dt(2024, 6, 15, 0, 0, 0)})
	if err == nil || !strings.Contains(err.Error(), "boom") {
		t.Fatalf("err = %v, want wrapped boom", err)
	}
}

// TestBuildDrilldownIssuesResponseEmptyItemsNotNil matches Python's
// PersonDrilldownResponse(items=[]) -- an empty result must marshal as
// "items": [], never "items": null.
func TestBuildDrilldownIssuesResponseEmptyItemsNotNil(t *testing.T) {
	t.Setenv("IDENTITY_MAPPING_PATH", t.TempDir()+"/missing.yaml")
	client := personIdentityDispatchClient{identity: "alice@example.com", t: t, handler: func(t *testing.T, query string, bindings []dhclickhouse.Binding) (dhclickhouse.RowScanner, error) {
		return &issuesRowScanner{rows: nil}, nil
	}}
	reader, err := NewReader(client)
	if err != nil {
		t.Fatalf("NewReader: %v", err)
	}
	resp, err := BuildDrilldownIssuesResponse(context.Background(), reader, "org-1", DrilldownIssuesParams{PersonID: "anyone", RangeDays: 14, Limit: 50, Now: dt(2024, 6, 15, 0, 0, 0)})
	if err != nil {
		t.Fatalf("BuildDrilldownIssuesResponse: %v", err)
	}
	if resp.Items == nil {
		t.Fatalf("Items is nil, want empty non-nil slice")
	}
	body, err := json.Marshal(resp)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if string(body) != `{"items":[],"next_cursor":null}` {
		t.Fatalf("marshal = %s, want {\"items\":[],\"next_cursor\":null}", body)
	}
}

// TestBuildDrilldownIssuesResponseCursorFilterBound pins that a non-nil
// Cursor param both appends the SQL filter and binds it -- Python's own
// `if cursor is not None: cursor_filter = "AND wct.completed_at <
// %(cursor)s"` (queries/people.py:274-275).
func TestBuildDrilldownIssuesResponseCursorFilterBound(t *testing.T) {
	t.Setenv("IDENTITY_MAPPING_PATH", t.TempDir()+"/missing.yaml")
	var captured string
	var capturedBindings []dhclickhouse.Binding
	client := personIdentityDispatchClient{identity: "alice@example.com", t: t, handler: func(t *testing.T, query string, bindings []dhclickhouse.Binding) (dhclickhouse.RowScanner, error) {
		captured = query
		capturedBindings = bindings
		return &issuesRowScanner{rows: nil}, nil
	}}
	reader, err := NewReader(client)
	if err != nil {
		t.Fatalf("NewReader: %v", err)
	}
	cursor := dt(2024, 6, 10, 0, 0, 0)
	_, err = BuildDrilldownIssuesResponse(context.Background(), reader, "org-1", DrilldownIssuesParams{
		PersonID: "anyone", RangeDays: 14, Limit: 50, Cursor: &cursor, Now: dt(2024, 6, 15, 0, 0, 0),
	})
	if err != nil {
		t.Fatalf("BuildDrilldownIssuesResponse: %v", err)
	}
	if !strings.Contains(captured, "wct.completed_at < {cursor:DateTime64(3, 'UTC')}") {
		t.Fatalf("query missing cursor filter:\n%s", captured)
	}
	v, ok := bindingValue(capturedBindings, "cursor")
	if !ok || v.(time.Time) != cursor {
		t.Fatalf("cursor binding = %v, want %v", v, cursor)
	}
}

// loadDrilldownIssuesGolden decodes a testdata JSON file -- captured from
// the REAL Python build_person_drilldown_issues_response (monkeypatched
// clickhouse_client/resolve_person_identity/fetch_person_issues/
// load_identity_aliases) via a one-off `uv run python3` invocation (see
// this PR's own TEST-EVIDENCE for the exact script), THEN hand-adjusted to
// replace each naive Python timestamp with its RFC 3339 form
// (started_at/completed_at) -- the declared divergence IssueRow's own
// doc comment explains. Every other field is byte-identical to the
// captured Python JSON.
func loadDrilldownIssuesGolden(t *testing.T, name string) DrilldownIssuesResponse {
	t.Helper()
	data, err := os.ReadFile("testdata/" + name)
	if err != nil {
		t.Fatalf("read golden %s: %v", name, err)
	}
	var resp DrilldownIssuesResponse
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&resp); err != nil {
		t.Fatalf("decode golden %s: %v", name, err)
	}
	return resp
}

// TestDrilldownIssuesGolden replays testdata/drilldown_issues_golden.json:
// two work items for one identity, one fully populated (including a
// resolved team_id from the attribution join) and one with every
// Nullable field NULL (team_id/cycle_time_hours/lead_time_hours/
// started_at/completed_at), pinning next_cursor falling back to null when
// the LAST fetched row's completed_at is NULL.
func TestDrilldownIssuesGolden(t *testing.T) {
	t.Setenv("IDENTITY_MAPPING_PATH", t.TempDir()+"/missing.yaml")
	client := personIdentityDispatchClient{identity: "alice@example.com", t: t, handler: func(t *testing.T, query string, bindings []dhclickhouse.Binding) (dhclickhouse.RowScanner, error) {
		if v, _ := bindingValue(bindings, "org_id"); v != "org-1" {
			t.Fatalf("org_id binding = %v, want org-1", v)
		}
		if strings.Contains(query, "FROM work_items FINAL") || strings.Contains(query, "FROM work_graph_issue_pr FINAL") {
			return &pairRowScanner{}, nil
		}
		return &issuesRowScanner{rows: [][]any{
			{"wi-1", "github", "done", "team-a", 12.5, 20.25, dt(2024, 6, 1, 10, 0, 0), dt(2024, 6, 3, 10, 0, 0)},
			{"wi-2", "jira", "in_progress", nil, nil, nil, nil, nil},
		}}, nil
	}}
	reader, err := NewReader(client)
	if err != nil {
		t.Fatalf("NewReader: %v", err)
	}
	got, err := BuildDrilldownIssuesResponse(context.Background(), reader, "org-1", DrilldownIssuesParams{
		PersonID: "anyone", RangeDays: 14, Limit: 50, Now: dt(2024, 6, 15, 0, 0, 0),
	})
	if err != nil {
		t.Fatalf("BuildDrilldownIssuesResponse: %v", err)
	}
	want := loadDrilldownIssuesGolden(t, "drilldown_issues_golden.json")
	gotJSON, err := json.Marshal(got)
	if err != nil {
		t.Fatalf("marshal got: %v", err)
	}
	wantJSON, err := json.Marshal(want)
	if err != nil {
		t.Fatalf("marshal want: %v", err)
	}
	if string(gotJSON) != string(wantJSON) {
		t.Fatalf("response mismatch\n got:  %s\nwant: %s", gotJSON, wantJSON)
	}
}

// pairRowScanner replays fixed (key, name) rows.
type pairRowScanner struct {
	rows  [][2]string
	index int
}

func (s *pairRowScanner) Next() bool {
	if s.index >= len(s.rows) {
		return false
	}
	s.index++
	return true
}

func (s *pairRowScanner) Scan(dest ...any) error {
	*dest[0].(*string), *dest[1].(*string) = s.rows[s.index-1][0], s.rows[s.index-1][1]
	return nil
}

func (s *pairRowScanner) Err() error   { return nil }
func (s *pairRowScanner) Close() error { return nil }

// TestDrilldownIssuesServeTheStoredTitle: a stored title is served, an absent
// one is null, and the title read is bound to the requesting org.
func TestDrilldownIssuesServeTheStoredTitle(t *testing.T) {
	t.Setenv("IDENTITY_MAPPING_PATH", t.TempDir()+"/missing.yaml")
	var titleOrg any
	client := personIdentityDispatchClient{identity: "alice@example.com", t: t, handler: func(t *testing.T, query string, bindings []dhclickhouse.Binding) (dhclickhouse.RowScanner, error) {
		if strings.Contains(query, "FROM work_graph_issue_pr FINAL") {
			return &pairRowScanner{}, nil
		}
		if strings.Contains(query, "FROM work_items FINAL") {
			titleOrg, _ = bindingValue(bindings, "org_id")
			return &pairRowScanner{rows: [][2]string{{"wi-1", "Fix login"}}}, nil
		}
		return &issuesRowScanner{rows: [][]any{
			{"wi-1", "github", "done", nil, nil, nil, nil, nil},
			{"wi-2", "jira", "done", nil, nil, nil, nil, nil},
		}}, nil
	}}
	reader, err := NewReader(client)
	if err != nil {
		t.Fatalf("NewReader: %v", err)
	}
	got, err := BuildDrilldownIssuesResponse(context.Background(), reader, "org-1", DrilldownIssuesParams{
		PersonID: "anyone", RangeDays: 14, Limit: 50, Now: dt(2024, 6, 15, 0, 0, 0),
	})
	if err != nil {
		t.Fatalf("BuildDrilldownIssuesResponse: %v", err)
	}
	if titleOrg != "org-1" {
		t.Fatalf("title read org_id = %v, want org-1", titleOrg)
	}
	if got.Items[0].Title == nil || *got.Items[0].Title != "Fix login" {
		t.Fatalf("Items[0].Title = %v, want Fix login", got.Items[0].Title)
	}
	if got.Items[1].Title != nil {
		t.Fatalf("Items[1].Title = %v, want nil", *got.Items[1].Title)
	}
}

// TestDrilldownIssuesFailOnATitleReadError: a failed title read is an error,
// never an unnamed success.
func TestDrilldownIssuesFailOnATitleReadError(t *testing.T) {
	t.Setenv("IDENTITY_MAPPING_PATH", t.TempDir()+"/missing.yaml")
	client := personIdentityDispatchClient{identity: "alice@example.com", t: t, handler: func(t *testing.T, query string, bindings []dhclickhouse.Binding) (dhclickhouse.RowScanner, error) {
		if strings.Contains(query, "FROM work_graph_issue_pr FINAL") {
			return &pairRowScanner{}, nil
		}
		if strings.Contains(query, "FROM work_items FINAL") {
			return nil, errors.New("boom")
		}
		return &issuesRowScanner{rows: [][]any{{"wi-1", "github", "done", nil, nil, nil, nil, nil}}}, nil
	}}
	reader, err := NewReader(client)
	if err != nil {
		t.Fatalf("NewReader: %v", err)
	}
	if _, err := BuildDrilldownIssuesResponse(context.Background(), reader, "org-1", DrilldownIssuesParams{
		PersonID: "anyone", RangeDays: 14, Limit: 50, Now: dt(2024, 6, 15, 0, 0, 0),
	}); err == nil {
		t.Fatal("want an error when the title read fails")
	}
}

// TestDrilldownIssuesServeTheLinkedRepoNames: one linked repo -> [name]; two
// -> both (a repeated name once); none -> null; the read is bound to the org.
func TestDrilldownIssuesServeTheLinkedRepoNames(t *testing.T) {
	t.Setenv("IDENTITY_MAPPING_PATH", t.TempDir()+"/missing.yaml")
	var linkOrg any
	client := personIdentityDispatchClient{identity: "alice@example.com", t: t, handler: func(t *testing.T, query string, bindings []dhclickhouse.Binding) (dhclickhouse.RowScanner, error) {
		if strings.Contains(query, "FROM work_items FINAL") {
			return &pairRowScanner{}, nil
		}
		if strings.Contains(query, "FROM work_graph_issue_pr FINAL") {
			linkOrg, _ = bindingValue(bindings, "org_id")
			return &pairRowScanner{rows: [][2]string{{"wi-1", "acme/api"}, {"wi-2", "acme/api"}, {"wi-2", "acme/api"}, {"wi-2", "acme/web"}}}, nil
		}
		return &issuesRowScanner{rows: [][]any{
			{"wi-1", "github", "done", nil, nil, nil, nil, nil},
			{"wi-2", "github", "done", nil, nil, nil, nil, nil},
			{"wi-3", "jira", "done", nil, nil, nil, nil, nil},
		}}, nil
	}}
	reader, err := NewReader(client)
	if err != nil {
		t.Fatalf("NewReader: %v", err)
	}
	got, err := BuildDrilldownIssuesResponse(context.Background(), reader, "org-1", DrilldownIssuesParams{
		PersonID: "anyone", RangeDays: 14, Limit: 50, Now: dt(2024, 6, 15, 0, 0, 0),
	})
	if err != nil {
		t.Fatalf("BuildDrilldownIssuesResponse: %v", err)
	}
	if linkOrg != "org-1" {
		t.Fatalf("link read org_id = %v, want org-1", linkOrg)
	}
	if !reflect.DeepEqual(got.Items[0].RepoNames, []string{"acme/api"}) ||
		!reflect.DeepEqual(got.Items[1].RepoNames, []string{"acme/api", "acme/web"}) ||
		got.Items[2].RepoNames != nil {
		t.Fatalf("RepoNames = %v / %v / %v", got.Items[0].RepoNames, got.Items[1].RepoNames, got.Items[2].RepoNames)
	}
	body, _ := json.Marshal(got.Items[2])
	if !strings.Contains(string(body), `"repo_names":null`) {
		t.Fatalf("an unlinked issue must serve repo_names null: %s", body)
	}
}

// TestDrilldownIssuesFailOnALinkedRepoReadError: a failed link read is an error.
func TestDrilldownIssuesFailOnALinkedRepoReadError(t *testing.T) {
	t.Setenv("IDENTITY_MAPPING_PATH", t.TempDir()+"/missing.yaml")
	client := personIdentityDispatchClient{identity: "alice@example.com", t: t, handler: func(t *testing.T, query string, bindings []dhclickhouse.Binding) (dhclickhouse.RowScanner, error) {
		if strings.Contains(query, "FROM work_items FINAL") {
			return &pairRowScanner{}, nil
		}
		if strings.Contains(query, "FROM work_graph_issue_pr FINAL") {
			return nil, errors.New("boom")
		}
		return &issuesRowScanner{rows: [][]any{{"wi-1", "github", "done", nil, nil, nil, nil, nil}}}, nil
	}}
	reader, err := NewReader(client)
	if err != nil {
		t.Fatalf("NewReader: %v", err)
	}
	if _, err := BuildDrilldownIssuesResponse(context.Background(), reader, "org-1", DrilldownIssuesParams{
		PersonID: "anyone", RangeDays: 14, Limit: 50, Now: dt(2024, 6, 15, 0, 0, 0),
	}); err == nil {
		t.Fatal("want an error when the link read fails")
	}
}

// TestLinkedRepoNamesQueryNeverReadsTheIssuesOwnRepo pins the entity tree: the
// repositories come from work_graph_issue_pr, never from work_items.
func TestLinkedRepoNamesQueryNeverReadsTheIssuesOwnRepo(t *testing.T) {
	if strings.Contains(linkedRepoNamesQuery, "work_items") {
		t.Fatalf("linkedRepoNamesQuery must not read work_items:\n%s", linkedRepoNamesQuery)
	}
	if strings.Count(linkedRepoNamesQuery, "org_id = {org_id:String}") != 2 {
		t.Fatalf("both reads must bind the org:\n%s", linkedRepoNamesQuery)
	}
}

// TestDrilldownIssuesFailOnALinkedRepoIterateError: an error that surfaces
// only after the link rows were read is still an error.
func TestDrilldownIssuesFailOnALinkedRepoIterateError(t *testing.T) {
	t.Setenv("IDENTITY_MAPPING_PATH", t.TempDir()+"/missing.yaml")
	client := personIdentityDispatchClient{identity: "alice@example.com", t: t, handler: func(t *testing.T, query string, bindings []dhclickhouse.Binding) (dhclickhouse.RowScanner, error) {
		if strings.Contains(query, "FROM work_items FINAL") {
			return &pairRowScanner{}, nil
		}
		if strings.Contains(query, "FROM work_graph_issue_pr FINAL") {
			return &errAfterRowsScanner{pairRowScanner: pairRowScanner{rows: [][2]string{{"wi-1", "acme/api"}}}, err: errors.New("boom")}, nil
		}
		return &issuesRowScanner{rows: [][]any{{"wi-1", "github", "done", nil, nil, nil, nil, nil}}}, nil
	}}
	reader, err := NewReader(client)
	if err != nil {
		t.Fatalf("NewReader: %v", err)
	}
	if _, err := BuildDrilldownIssuesResponse(context.Background(), reader, "org-1", DrilldownIssuesParams{
		PersonID: "anyone", RangeDays: 14, Limit: 50, Now: dt(2024, 6, 15, 0, 0, 0),
	}); err == nil {
		t.Fatal("want an error when the link rows fail while iterating")
	}
}
