package reviewedges

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-go/clickhouse"

	"github.com/full-chaos/dev-health-ops/cmd/query-api/internal/graphqldate"
)

// fakeRowScanner is one scripted response, whose values are copied into a
// Scan call's destination pointers in call order -- matching the query's
// own SELECT column order (reviewer, author, reviews_count, day, repo_id).
type fakeRowScanner struct {
	rows   [][]any
	cursor int
	err    error
}

func (f *fakeRowScanner) Next() bool {
	if f.err != nil {
		return false
	}
	return f.cursor < len(f.rows)
}

func (f *fakeRowScanner) Scan(dest ...any) error {
	row := f.rows[f.cursor]
	f.cursor++
	if len(dest) != len(row) {
		return errors.New("reviewedges test: scan arity mismatch")
	}
	for i, d := range dest {
		switch ptr := d.(type) {
		case *string:
			*ptr = row[i].(string)
		case *uint32:
			*ptr = row[i].(uint32)
		case *time.Time:
			*ptr = row[i].(time.Time)
		default:
			return errors.New("reviewedges test: unsupported scan destination")
		}
	}
	return nil
}

func (f *fakeRowScanner) Err() error   { return f.err }
func (f *fakeRowScanner) Close() error { return nil }

// fakeClient scripts one response for the single query Resolve issues
// (unlike featureflags, reviewedges has no separate count query).
type fakeClient struct {
	response  *fakeRowScanner
	err       error
	statement string
	bindings  []clickhouse.Binding
}

func (f *fakeClient) Query(_ context.Context, statement string, bindings []clickhouse.Binding) (clickhouse.RowScanner, error) {
	f.statement = statement
	f.bindings = bindings
	if f.err != nil {
		return nil, f.err
	}
	return f.response, nil
}

func mustDate(t *testing.T, s string) graphqldate.Date {
	t.Helper()
	d, err := graphqldate.Parse(s)
	if err != nil {
		t.Fatalf("graphqldate.Parse(%q): %v", s, err)
	}
	return d
}

func TestResolve_HappyPath(t *testing.T) {
	day := time.Date(2026, 8, 20, 0, 0, 0, 0, time.UTC)
	client := &fakeClient{
		response: &fakeRowScanner{rows: [][]any{
			{"reviewer@example.com", "author@example.com", uint32(3), day, "11111111-1111-1111-1111-111111111111"},
		}},
	}

	result, err := Resolve(context.Background(), client, "org-1", mustDate(t, "2026-08-01"), mustDate(t, "2026-08-31"), nil, 500)
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	if result.TotalCount != 1 {
		t.Fatalf("TotalCount = %d, want 1", result.TotalCount)
	}
	if len(result.Edges) != 1 {
		t.Fatalf("len(Edges) = %d, want 1", len(result.Edges))
	}
	edge := result.Edges[0]
	if edge.Reviewer != "reviewer@example.com" || edge.Author != "author@example.com" {
		t.Errorf("reviewer/author = %q/%q", edge.Reviewer, edge.Author)
	}
	if edge.ReviewsCount != 3 {
		t.Errorf("ReviewsCount = %d, want 3", edge.ReviewsCount)
	}
	if edge.Day.String() != "2026-08-20" {
		t.Errorf("Day = %q, want 2026-08-20", edge.Day.String())
	}
	if edge.RepoID == nil || *edge.RepoID != "11111111-1111-1111-1111-111111111111" {
		t.Errorf("RepoID = %v", edge.RepoID)
	}
}

// TestResolve_DirectedEdgesAreNotSymmetrized proves reviewer->author is
// treated as a directed pair, not merged/deduplicated with its reverse --
// the exact "seed both ends of directed relations" risk this port must
// not silently collapse (CHAOS-4368 lane brief).
func TestResolve_DirectedEdgesAreNotSymmetrized(t *testing.T) {
	day := time.Date(2026, 8, 20, 0, 0, 0, 0, time.UTC)
	client := &fakeClient{
		response: &fakeRowScanner{rows: [][]any{
			{"alice@example.com", "bob@example.com", uint32(5), day, "repo-a"},
			{"bob@example.com", "alice@example.com", uint32(2), day, "repo-a"},
		}},
	}

	result, err := Resolve(context.Background(), client, "org-1", mustDate(t, "2026-08-01"), mustDate(t, "2026-08-31"), nil, 500)
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	if result.TotalCount != 2 {
		t.Fatalf("TotalCount = %d, want 2 (directed pairs must not collapse)", result.TotalCount)
	}
	forward, reverse := result.Edges[0], result.Edges[1]
	if forward.Reviewer != "alice@example.com" || forward.Author != "bob@example.com" || forward.ReviewsCount != 5 {
		t.Errorf("forward edge = %+v", forward)
	}
	if reverse.Reviewer != "bob@example.com" || reverse.Author != "alice@example.com" || reverse.ReviewsCount != 2 {
		t.Errorf("reverse edge = %+v", reverse)
	}
}

func TestResolve_EmptyResultIsNonNilSlice(t *testing.T) {
	client := &fakeClient{response: &fakeRowScanner{rows: nil}}
	result, err := Resolve(context.Background(), client, "org-1", mustDate(t, "2026-08-01"), mustDate(t, "2026-08-31"), nil, 500)
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	if result.Edges == nil {
		t.Fatal("Edges is nil, want a non-nil empty slice (schema declares edges: [ReviewEdgeRow!]!)")
	}
	if len(result.Edges) != 0 || result.TotalCount != 0 {
		t.Fatalf("expected empty result, got %+v", result)
	}
}

// TestResolve_ErrorPropagatesNoDegradedPath is the deliberate divergence
// from featureflags: resolve_review_edges has no missing-table degraded
// path (ReviewEdgesResult has no degradedReason field at all), so any
// ClickHouse error -- missing table included -- must propagate as an
// error, never be swallowed into an empty/degraded result.
func TestResolve_ErrorPropagatesNoDegradedPath(t *testing.T) {
	client := &fakeClient{err: errors.New("UNKNOWN_TABLE: review_edges_daily")}
	_, err := Resolve(context.Background(), client, "org-1", mustDate(t, "2026-08-01"), mustDate(t, "2026-08-31"), nil, 500)
	if err == nil {
		t.Fatal("expected error to propagate, got nil")
	}
}

func TestResolve_ScanErrorPropagates(t *testing.T) {
	client := &fakeClient{response: &fakeRowScanner{rows: [][]any{
		{"reviewer@example.com"}, // missing the other 4 columns -> arity mismatch
	}}}
	_, err := Resolve(context.Background(), client, "org-1", mustDate(t, "2026-08-01"), mustDate(t, "2026-08-31"), nil, 500)
	if err == nil {
		t.Fatal("expected scan error to propagate, got nil")
	}
}

func TestClampLimit(t *testing.T) {
	cases := []struct {
		in, want int
	}{
		{in: -5, want: 1},
		{in: 0, want: 1},
		{in: 1, want: 1},
		{in: 500, want: 500},
		{in: 2000, want: 2000},
		{in: 2001, want: 2000},
		{in: 100000, want: 2000},
	}
	for _, tc := range cases {
		if got := clampLimit(tc.in); got != tc.want {
			t.Errorf("clampLimit(%d) = %d, want %d", tc.in, got, tc.want)
		}
	}
}

// TestResolve_RepoIDsFilterBindsWhenProvided proves the repo_ids filter
// actually reaches the query (both the subquery clause and the binding) --
// a resolver that silently ignored it would still pass a happy-path test
// with a nil filter.
func TestResolve_RepoIDsFilterBindsWhenProvided(t *testing.T) {
	client := &fakeClient{response: &fakeRowScanner{rows: nil}}
	repoIDs := []string{"repo-a", "repo-b"}
	if _, err := Resolve(context.Background(), client, "org-1", mustDate(t, "2026-08-01"), mustDate(t, "2026-08-31"), repoIDs, 500); err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	if !strings.Contains(client.statement, "repo_id IN (") || !strings.Contains(client.statement, "{repo_ids:Array(String)}") {
		t.Errorf("statement missing repo_ids filter clause: %s", client.statement)
	}
	found := false
	for _, b := range client.bindings {
		if b.Name == "repo_ids" {
			found = true
			got, ok := b.Value.([]string)
			if !ok || len(got) != 2 || got[0] != "repo-a" || got[1] != "repo-b" {
				t.Errorf("repo_ids binding = %v", b.Value)
			}
		}
	}
	if !found {
		t.Error("expected a repo_ids binding, found none")
	}
}

// TestResolve_NoRepoIDsOmitsFilterClause proves the absence of a filter
// does not leave a dangling/broken clause or an unused binding.
func TestResolve_NoRepoIDsOmitsFilterClause(t *testing.T) {
	client := &fakeClient{response: &fakeRowScanner{rows: nil}}
	if _, err := Resolve(context.Background(), client, "org-1", mustDate(t, "2026-08-01"), mustDate(t, "2026-08-31"), nil, 500); err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	if strings.Contains(client.statement, "repo_id IN (") {
		t.Errorf("statement unexpectedly contains repo_ids filter clause: %s", client.statement)
	}
	for _, b := range client.bindings {
		if b.Name == "repo_ids" {
			t.Errorf("unexpected repo_ids binding when no filter was requested: %v", b.Value)
		}
	}
}

// TestResolve_LimitBindingIsClamped proves the bound limit value is the
// CLAMPED value, not the raw caller-supplied one.
func TestResolve_LimitBindingIsClamped(t *testing.T) {
	client := &fakeClient{response: &fakeRowScanner{rows: nil}}
	if _, err := Resolve(context.Background(), client, "org-1", mustDate(t, "2026-08-01"), mustDate(t, "2026-08-31"), nil, 999999); err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	for _, b := range client.bindings {
		if b.Name == "limit" {
			if b.Value != MaxRows {
				t.Errorf("limit binding = %v, want clamped %d", b.Value, MaxRows)
			}
			return
		}
	}
	t.Error("expected a limit binding, found none")
}

// TestResolve_DateBindingsAreYYYYMMDDStrings proves since/until are bound
// as plain "YYYY-MM-DD" strings (matching Python's
// `input.since_date.isoformat()`), not a time.Time or a timestamp string.
func TestResolve_DateBindingsAreYYYYMMDDStrings(t *testing.T) {
	client := &fakeClient{response: &fakeRowScanner{rows: nil}}
	since := mustDate(t, "2026-01-05")
	until := mustDate(t, "2026-02-10")
	if _, err := Resolve(context.Background(), client, "org-1", since, until, nil, 500); err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	values := map[string]any{}
	for _, b := range client.bindings {
		values[b.Name] = b.Value
	}
	if values["since_date"] != "2026-01-05" {
		t.Errorf("since_date binding = %v, want %q", values["since_date"], "2026-01-05")
	}
	if values["until_date"] != "2026-02-10" {
		t.Errorf("until_date binding = %v, want %q", values["until_date"], "2026-02-10")
	}
}
