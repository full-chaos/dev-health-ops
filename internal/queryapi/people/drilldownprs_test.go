package people

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"strings"
	"testing"
	"time"

	dhclickhouse "github.com/full-chaos/dev-health-go/clickhouse"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/sqlshape"
)

// dt builds a UTC time.Time with explicit time-of-day components --
// day(y,m,d)'s own sibling for this file's row fixtures, which (unlike
// search's day-granularity rows) need hour/minute/second precision.
func dt(y int, m time.Month, d, hh, mm, ss int) time.Time {
	return time.Date(y, m, d, hh, mm, ss, 0, time.UTC)
}

// prsRowScanner replays a fixed slice of pre-built
// fetchPersonPullRequestsQuery result rows, in SELECT order (repo_id,
// number, title, author_name, author_email, created_at, merged_at,
// first_review_at, review_latency_hours).
type prsRowScanner struct {
	rows  [][]any
	index int
}

func (s *prsRowScanner) Next() bool {
	if s.index >= len(s.rows) {
		return false
	}
	s.index++
	return true
}

func (s *prsRowScanner) Scan(dest ...any) error {
	row := s.rows[s.index-1]
	*dest[0].(*string) = row[0].(string)
	*dest[1].(*uint32) = row[1].(uint32)
	*dest[2].(**string) = nilableString(row[2])
	*dest[3].(**string) = nilableString(row[3])
	*dest[4].(**string) = nilableString(row[4])
	*dest[5].(*time.Time) = row[5].(time.Time)
	*dest[6].(**time.Time) = nilableTime(row[6])
	*dest[7].(**time.Time) = nilableTime(row[7])
	*dest[8].(**float64) = nilableFloat(row[8])
	return nil
}

func (s *prsRowScanner) Err() error   { return nil }
func (s *prsRowScanner) Close() error { return nil }

func nilableString(v any) *string {
	if v == nil {
		return nil
	}
	s := v.(string)
	return &s
}

func nilableTime(v any) *time.Time {
	if v == nil {
		return nil
	}
	t := v.(time.Time)
	return &t
}

func nilableFloat(v any) *float64 {
	if v == nil {
		return nil
	}
	f := v.(float64)
	return &f
}

// personIdentityDispatchClient answers resolvePersonIdentity's own
// md5-lookup query (detected by its distinctive SQL fragment, same
// convention people_summary_route_test.go's own personLookupClient
// uses) with a fixed identity, and every other query via a caller-supplied
// handler -- letting this file's tests fix identity resolution while
// asserting on the drilldown read itself.
type personIdentityDispatchClient struct {
	identity string
	handler  func(t *testing.T, query string, bindings []dhclickhouse.Binding) (dhclickhouse.RowScanner, error)
	t        *testing.T
}

func (c personIdentityDispatchClient) Query(ctx context.Context, query string, bindings []dhclickhouse.Binding) (dhclickhouse.RowScanner, error) {
	if strings.Contains(query, "lower(hex(MD5(identity)))") {
		if c.identity == "" {
			return &scalarStringScanner{}, nil
		}
		return &scalarStringScanner{values: []string{c.identity}}, nil
	}
	return c.handler(c.t, query, bindings)
}

// TestFetchPersonPullRequestsQueryDedupsBothTables pins that neither
// ReplacingMergeTree table this query reads (git_pull_requests, repos) is
// ever read raw: FINAL appears on both, matching internal/drilldown/
// prs.go's own fetchPullRequestsQuery for the SAME two tables.
func TestFetchPersonPullRequestsQueryDedupsBothTables(t *testing.T) {
	for _, want := range []string{
		"FROM git_pull_requests AS pr FINAL",
		"FROM repos FINAL",
	} {
		if !strings.Contains(fetchPersonPullRequestsQuery, want) {
			t.Fatalf("fetchPersonPullRequestsQuery missing %q:\n%s", want, fetchPersonPullRequestsQuery)
		}
	}
}

// TestFetchPersonPullRequestsQueryOrgFilterInsidePRRead pins the class
// ruling this query follows -- byte-identical shape to internal/drilldown/
// prs.go's own equivalent test, for the same reasons documented on
// fetchPersonPullRequestsQuery's own doc comment.
func TestFetchPersonPullRequestsQueryOrgFilterInsidePRRead(t *testing.T) {
	if strings.Contains(fetchPersonPullRequestsQuery, "JOIN") {
		t.Fatalf("fetchPersonPullRequestsQuery must not join repos to gate org scope:\n%s", fetchPersonPullRequestsQuery)
	}
	if strings.Contains(fetchPersonPullRequestsQuery, "pr.org_id") {
		t.Fatalf("fetchPersonPullRequestsQuery must not gate org scope via git_pull_requests.org_id directly:\n%s", fetchPersonPullRequestsQuery)
	}

	const finalMarker = "FROM repos FINAL"
	const orgPredicate = "org_id = {org_id:String}"
	depths := sqlshape.Depths(fetchPersonPullRequestsQuery)

	finalIdx := strings.Index(fetchPersonPullRequestsQuery, finalMarker)
	if finalIdx == -1 {
		t.Fatalf("fetchPersonPullRequestsQuery missing %q:\n%s", finalMarker, fetchPersonPullRequestsQuery)
	}
	orgIdx := strings.Index(fetchPersonPullRequestsQuery, orgPredicate)
	if orgIdx == -1 {
		t.Fatalf("fetchPersonPullRequestsQuery missing %q:\n%s", orgPredicate, fetchPersonPullRequestsQuery)
	}
	finalDepth, orgDepth := depths[finalIdx], depths[orgIdx]
	if orgDepth != finalDepth {
		t.Fatalf("%q sits at nesting depth %d but %q sits at depth %d", orgPredicate, orgDepth, finalMarker, finalDepth)
	}
	if finalDepth == 0 {
		t.Fatalf("%q sits at the top level (depth 0), not nested inside pr.repo_id IN (...):\n%s", finalMarker, fetchPersonPullRequestsQuery)
	}
}

func TestBuildDrilldownPRsResponseNilReader(t *testing.T) {
	_, err := BuildDrilldownPRsResponse(context.Background(), nil, "org-1", DrilldownPRsParams{})
	if !errors.Is(err, ErrUnavailable) {
		t.Fatalf("err = %v, want ErrUnavailable", err)
	}
}

// TestBuildDrilldownPRsResponsePersonNotFound pins the 404-shaped
// RequestError build_person_drilldown_prs_response's own `raise
// ValueError("person not found")` maps to (main.py:1146-1147).
func TestBuildDrilldownPRsResponsePersonNotFound(t *testing.T) {
	t.Setenv("IDENTITY_MAPPING_PATH", t.TempDir()+"/missing.yaml")
	client := personIdentityDispatchClient{identity: "", t: t, handler: func(t *testing.T, query string, bindings []dhclickhouse.Binding) (dhclickhouse.RowScanner, error) {
		t.Fatalf("unexpected query for an unresolved identity: %s", query)
		return nil, nil
	}}
	reader, err := NewReader(client)
	if err != nil {
		t.Fatalf("NewReader: %v", err)
	}
	_, err = BuildDrilldownPRsResponse(context.Background(), reader, "org-1", DrilldownPRsParams{PersonID: "nobody", RangeDays: 14, Limit: 50, Now: dt(2024, 6, 15, 0, 0, 0)})
	reqErr, ok := AsRequestError(err)
	if !ok || reqErr.Status != 404 || reqErr.Message != "Person not found" {
		t.Fatalf("err = %v, want RequestError{404, Person not found}", err)
	}
}

func TestBuildDrilldownPRsResponseQueryError(t *testing.T) {
	t.Setenv("IDENTITY_MAPPING_PATH", t.TempDir()+"/missing.yaml")
	boom := errors.New("boom")
	client := personIdentityDispatchClient{identity: "alice@example.com", t: t, handler: func(t *testing.T, query string, bindings []dhclickhouse.Binding) (dhclickhouse.RowScanner, error) {
		return nil, boom
	}}
	reader, err := NewReader(client)
	if err != nil {
		t.Fatalf("NewReader: %v", err)
	}
	_, err = BuildDrilldownPRsResponse(context.Background(), reader, "org-1", DrilldownPRsParams{PersonID: "anyone", RangeDays: 14, Limit: 50, Now: dt(2024, 6, 15, 0, 0, 0)})
	if err == nil || !strings.Contains(err.Error(), "boom") {
		t.Fatalf("err = %v, want wrapped boom", err)
	}
}

// TestBuildDrilldownPRsResponseEmptyItemsNotNil matches Python's
// PersonDrilldownResponse(items=[]) -- an empty result must marshal as
// "items": [], never "items": null.
func TestBuildDrilldownPRsResponseEmptyItemsNotNil(t *testing.T) {
	t.Setenv("IDENTITY_MAPPING_PATH", t.TempDir()+"/missing.yaml")
	client := personIdentityDispatchClient{identity: "alice@example.com", t: t, handler: func(t *testing.T, query string, bindings []dhclickhouse.Binding) (dhclickhouse.RowScanner, error) {
		return &prsRowScanner{rows: nil}, nil
	}}
	reader, err := NewReader(client)
	if err != nil {
		t.Fatalf("NewReader: %v", err)
	}
	resp, err := BuildDrilldownPRsResponse(context.Background(), reader, "org-1", DrilldownPRsParams{PersonID: "anyone", RangeDays: 14, Limit: 50, Now: dt(2024, 6, 15, 0, 0, 0)})
	if err != nil {
		t.Fatalf("BuildDrilldownPRsResponse: %v", err)
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

// TestBuildDrilldownPRsResponseCursorFilterBound pins that a non-nil
// Cursor param both appends the SQL filter and binds it -- Python's own
// `if cursor is not None: cursor_filter = "AND created_at < %(cursor)s"`
// (queries/people.py:247-248).
func TestBuildDrilldownPRsResponseCursorFilterBound(t *testing.T) {
	t.Setenv("IDENTITY_MAPPING_PATH", t.TempDir()+"/missing.yaml")
	var captured string
	var capturedBindings []dhclickhouse.Binding
	client := personIdentityDispatchClient{identity: "alice@example.com", t: t, handler: func(t *testing.T, query string, bindings []dhclickhouse.Binding) (dhclickhouse.RowScanner, error) {
		captured = query
		capturedBindings = bindings
		return &prsRowScanner{rows: nil}, nil
	}}
	reader, err := NewReader(client)
	if err != nil {
		t.Fatalf("NewReader: %v", err)
	}
	cursor := dt(2024, 6, 10, 0, 0, 0)
	_, err = BuildDrilldownPRsResponse(context.Background(), reader, "org-1", DrilldownPRsParams{
		PersonID: "anyone", RangeDays: 14, Limit: 50, Cursor: &cursor, Now: dt(2024, 6, 15, 0, 0, 0),
	})
	if err != nil {
		t.Fatalf("BuildDrilldownPRsResponse: %v", err)
	}
	if !strings.Contains(captured, "pr.created_at < {cursor:DateTime64(3, 'UTC')}") {
		t.Fatalf("query missing cursor filter:\n%s", captured)
	}
	v, ok := bindingValue(capturedBindings, "cursor")
	if !ok || v.(time.Time) != cursor {
		t.Fatalf("cursor binding = %v, want %v", v, cursor)
	}
}

// TestBuildDrilldownPRsResponseNoCursorNoFilter pins the absent-cursor
// branch: no cursor filter text, no cursor binding.
func TestBuildDrilldownPRsResponseNoCursorNoFilter(t *testing.T) {
	t.Setenv("IDENTITY_MAPPING_PATH", t.TempDir()+"/missing.yaml")
	var captured string
	var capturedBindings []dhclickhouse.Binding
	client := personIdentityDispatchClient{identity: "alice@example.com", t: t, handler: func(t *testing.T, query string, bindings []dhclickhouse.Binding) (dhclickhouse.RowScanner, error) {
		captured = query
		capturedBindings = bindings
		return &prsRowScanner{rows: nil}, nil
	}}
	reader, err := NewReader(client)
	if err != nil {
		t.Fatalf("NewReader: %v", err)
	}
	_, err = BuildDrilldownPRsResponse(context.Background(), reader, "org-1", DrilldownPRsParams{PersonID: "anyone", RangeDays: 14, Limit: 50, Now: dt(2024, 6, 15, 0, 0, 0)})
	if err != nil {
		t.Fatalf("BuildDrilldownPRsResponse: %v", err)
	}
	if strings.Contains(captured, "cursor") {
		t.Fatalf("query unexpectedly references cursor:\n%s", captured)
	}
	if _, ok := bindingValue(capturedBindings, "cursor"); ok {
		t.Fatalf("unexpected cursor binding present: %+v", capturedBindings)
	}
}

// TestBoundedDrilldownLimitClamp pins boundedDrilldownLimit's own clamp
// (maxDrilldownLimit = 200).
func TestBoundedDrilldownLimitClamp(t *testing.T) {
	cases := map[int]int{0: 50, -5: 50, 1: 1, 100: 100, 200: 200, 500: 200}
	for in, want := range cases {
		if got := boundedDrilldownLimit(in); got != want {
			t.Fatalf("boundedDrilldownLimit(%d) = %d, want %d", in, got, want)
		}
	}
}

// loadDrilldownPRsGolden decodes a testdata JSON file -- captured from the
// REAL Python build_person_drilldown_prs_response (monkeypatched
// clickhouse_client/resolve_person_identity/fetch_person_pull_requests/
// load_identity_aliases) via a one-off `uv run python3` invocation (see
// this PR's own TEST-EVIDENCE for the exact script), THEN hand-adjusted to
// replace each naive Python timestamp with its RFC 3339 form
// (created_at/merged_at/first_review_at/next_cursor) -- the declared
// divergence PullRequestRow's own doc comment explains. Every other field
// is byte-identical to the captured Python JSON. DisallowUnknownFields
// makes a field-name mismatch a hard test failure, not a silent drop.
func loadDrilldownPRsGolden(t *testing.T, name string) DrilldownPRsResponse {
	t.Helper()
	data, err := os.ReadFile("testdata/" + name)
	if err != nil {
		t.Fatalf("read golden %s: %v", name, err)
	}
	var resp DrilldownPRsResponse
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&resp); err != nil {
		t.Fatalf("decode golden %s: %v", name, err)
	}
	return resp
}

// TestDrilldownPRsGolden replays testdata/drilldown_prs_golden.json: two
// pull requests for one identity, exercising the author_email-vs-
// author_name coalesce (services/people.py:778's `row.get("author") or
// row.get("author_name")`) both ways -- a non-empty author_email wins on
// the first row, an EMPTY (falsy) author_email falls back to author_name
// on the second -- plus every Nullable field (title/merged_at/
// first_review_at/review_latency_hours) both populated and NULL.
func TestDrilldownPRsGolden(t *testing.T) {
	t.Setenv("IDENTITY_MAPPING_PATH", t.TempDir()+"/missing.yaml")
	client := personIdentityDispatchClient{identity: "alice@example.com", t: t, handler: func(t *testing.T, query string, bindings []dhclickhouse.Binding) (dhclickhouse.RowScanner, error) {
		if v, _ := bindingValue(bindings, "org_id"); v != "org-1" {
			t.Fatalf("org_id binding = %v, want org-1", v)
		}
		return &prsRowScanner{rows: [][]any{
			{
				"11111111-1111-1111-1111-111111111111", uint32(42), "Add feature", "Alice Smith", "alice@example.com",
				dt(2024, 6, 10, 12, 0, 0), dt(2024, 6, 11, 9, 0, 0), dt(2024, 6, 10, 18, 0, 0), 6.0,
			},
			{
				"22222222-2222-2222-2222-222222222222", uint32(7), nil, "Alice Smith", "",
				dt(2024, 6, 5, 8, 30, 0), nil, nil, nil,
			},
		}}, nil
	}}
	reader, err := NewReader(client)
	if err != nil {
		t.Fatalf("NewReader: %v", err)
	}
	// The second row's author_email is "" (empty string, not nil) --
	// prsRowScanner's **string branch treats a non-nil `any` as present,
	// so this exercises the EMPTY-STRING-falls-through branch, not the
	// NULL-falls-through one.
	got, err := BuildDrilldownPRsResponse(context.Background(), reader, "org-1", DrilldownPRsParams{
		PersonID: "anyone", RangeDays: 14, Limit: 50, Now: dt(2024, 6, 15, 0, 0, 0),
	})
	if err != nil {
		t.Fatalf("BuildDrilldownPRsResponse: %v", err)
	}
	want := loadDrilldownPRsGolden(t, "drilldown_prs_golden.json")
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
