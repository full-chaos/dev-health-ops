package investment

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"

	dhclickhouse "github.com/full-chaos/dev-health-go/clickhouse"
)

// fixtureRowScanner replays a fixed slice of pre-built rows -- same shape
// quadrant/investmentexplain's own golden tests already use.
type fixtureRowScanner struct {
	rows  [][]any
	index int
}

func (s *fixtureRowScanner) Next() bool {
	if s.index >= len(s.rows) {
		return false
	}
	s.index++
	return true
}

func (s *fixtureRowScanner) Scan(dest ...any) error {
	row := s.rows[s.index-1]
	for i, d := range dest {
		switch typed := d.(type) {
		case *string:
			*typed = row[i].(string)
		case *float64:
			*typed = row[i].(float64)
		case *uint64:
			*typed = row[i].(uint64)
		}
	}
	return nil
}

func (s *fixtureRowScanner) Err() error   { return nil }
func (s *fixtureRowScanner) Close() error { return nil }

// fakeQueryClient dispatches on the SQL text's own distinguishing
// substrings -- the same technique quadrant/golden_test.go's
// fakeQueryClient uses -- rather than tracking call order, so a
// harmless reordering of this package's own query calls can never break
// this test.
type fakeQueryClient struct {
	t       *testing.T
	handler func(t *testing.T, query string, bindings []dhclickhouse.Binding) (dhclickhouse.RowScanner, error)
}

func (c fakeQueryClient) Query(_ context.Context, query string, bindings []dhclickhouse.Binding) (dhclickhouse.RowScanner, error) {
	return c.handler(c.t, query, bindings)
}

func loadGolden[T any](t *testing.T, name string) T {
	t.Helper()
	data, err := os.ReadFile("testdata/" + name)
	if err != nil {
		t.Fatalf("read golden %s: %v", name, err)
	}
	var out T
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&out); err != nil {
		t.Fatalf("decode golden %s: %v", name, err)
	}
	return out
}

// tablePresentHandler answers a table/columns-present pair of queries
// truthfully (work_unit_investments exists with every required column),
// falling through to fn for every other query.
func tablePresentHandler(fn func(t *testing.T, query string, bindings []dhclickhouse.Binding) (dhclickhouse.RowScanner, error)) func(t *testing.T, query string, bindings []dhclickhouse.Binding) (dhclickhouse.RowScanner, error) {
	return func(t *testing.T, query string, bindings []dhclickhouse.Binding) (dhclickhouse.RowScanner, error) {
		switch {
		case strings.Contains(query, "FROM system.tables"):
			return &fixtureRowScanner{rows: [][]any{{"work_unit_investments"}}}, nil
		case strings.Contains(query, "FROM system.columns"):
			return &fixtureRowScanner{rows: [][]any{
				{"from_ts"}, {"to_ts"}, {"repo_id"}, {"effort_value"},
				{"theme_distribution_json"}, {"subcategory_distribution_json"},
			}}, nil
		default:
			return fn(t, query, bindings)
		}
	}
}

// tableMissingHandler answers the table-present query with zero rows --
// every other query must never be reached (BuildResponse/
// BuildSunburstResponse return before issuing any of them).
func tableMissingHandler(t *testing.T) func(t *testing.T, query string, bindings []dhclickhouse.Binding) (dhclickhouse.RowScanner, error) {
	return func(t *testing.T, query string, bindings []dhclickhouse.Binding) (dhclickhouse.RowScanner, error) {
		if strings.Contains(query, "FROM system.tables") {
			return &fixtureRowScanner{}, nil
		}
		if strings.Contains(query, "membership_run") || strings.Contains(query, "investment_membership_scope_state") || strings.Contains(query, "org_column_transition") {
			return &fixtureRowScanner{}, nil
		}
		t.Fatalf("unexpected query once the table-present check fails:\n%s", query)
		return nil, nil
	}
}

func day(y int, m time.Month, d int) time.Time {
	return time.Date(y, m, d, 0, 0, 0, 0, time.UTC)
}

// TestGoldenInvestmentBasic replays testdata/investment_basic.json: a
// breakdown with a zero-value row excluded from both distributions, and
// a quality-stats row that trips all five quality_drivers branches at
// once.
func TestGoldenInvestmentBasic(t *testing.T) {
	client := fakeQueryClient{t: t, handler: tablePresentHandler(func(t *testing.T, query string, bindings []dhclickhouse.Binding) (dhclickhouse.RowScanner, error) {
		switch {
		case strings.Contains(query, "GROUP BY subcategory, theme"):
			return &fixtureRowScanner{rows: [][]any{
				{"engineering.backend", "engineering", 12.5},
				{"engineering.frontend", "engineering", 7.5},
				{"growth.marketing", "growth", 3.0},
				{"ops.oncall", "ops", 0.0},
			}}, nil
		case strings.Contains(query, "quality_known_count"):
			return &fixtureRowScanner{rows: [][]any{
				{uint64(20), uint64(10), 0.3, 0.3, uint64(1), uint64(1), uint64(9), uint64(3), uint64(10)},
			}}, nil
		case strings.Contains(query, "SELECT count() AS count"):
			return &fixtureRowScanner{rows: [][]any{{uint64(0)}}}, nil
		default:
			return &fixtureRowScanner{}, nil
		}
	})}

	reader, err := NewReader(client)
	if err != nil {
		t.Fatalf("NewReader: %v", err)
	}
	got, err := BuildResponse(context.Background(), reader, "org-1", Params{
		StartTS: day(2024, 1, 1), EndTS: day(2024, 1, 8), ScopeLevel: "org",
	})
	if err != nil {
		t.Fatalf("BuildResponse: %v", err)
	}
	want := loadGolden[Response](t, "investment_basic.json")
	if !reflect.DeepEqual(*got, want) {
		t.Fatalf("response mismatch\n got:  %+v\n want: %+v", *got, want)
	}
}

// TestGoldenInvestmentMissingTable replays
// testdata/investment_missing_table.json: work_unit_investments absent
// short-circuits to the all-empty/null response, matching
// build_investment_response's own _tables_present early return.
func TestGoldenInvestmentMissingTable(t *testing.T) {
	client := fakeQueryClient{t: t, handler: tableMissingHandler(t)}

	reader, err := NewReader(client)
	if err != nil {
		t.Fatalf("NewReader: %v", err)
	}
	got, err := BuildResponse(context.Background(), reader, "org-1", Params{
		StartTS: day(2024, 1, 1), EndTS: day(2024, 1, 8), ScopeLevel: "org",
	})
	if err != nil {
		t.Fatalf("BuildResponse: %v", err)
	}
	want := loadGolden[Response](t, "investment_missing_table.json")
	if !reflect.DeepEqual(*got, want) {
		t.Fatalf("response mismatch\n got:  %+v\n want: %+v", *got, want)
	}
}

// TestGoldenSunburstBasic replays testdata/sunburst_basic.json: two
// normal slices plus one row with empty theme/subcategory/scope,
// exercising build_investment_sunburst's own "Unassigned"/"Other"
// defaulting.
func TestGoldenSunburstBasic(t *testing.T) {
	client := fakeQueryClient{t: t, handler: tablePresentHandler(func(t *testing.T, query string, bindings []dhclickhouse.Binding) (dhclickhouse.RowScanner, error) {
		switch {
		case strings.Contains(query, "GROUP BY theme, subcategory, scope"):
			return &fixtureRowScanner{rows: [][]any{
				{"engineering.backend", "engineering", "repo-a", 5.0},
				{"growth.marketing", "growth", "repo-b", 2.0},
				{"", "", "", 1.0},
			}}, nil
		case strings.Contains(query, "SELECT count() AS count"):
			return &fixtureRowScanner{rows: [][]any{{uint64(0)}}}, nil
		default:
			return &fixtureRowScanner{}, nil
		}
	})}

	reader, err := NewReader(client)
	if err != nil {
		t.Fatalf("NewReader: %v", err)
	}
	got, err := BuildSunburstResponse(context.Background(), reader, "org-1", SunburstParams{
		StartTS: day(2024, 1, 1), EndTS: day(2024, 1, 31), ScopeLevel: "org", Limit: 500,
	})
	if err != nil {
		t.Fatalf("BuildSunburstResponse: %v", err)
	}
	want := loadGolden[[]SunburstSlice](t, "sunburst_basic.json")
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("response mismatch\n got:  %+v\n want: %+v", got, want)
	}
}

// TestGoldenSunburstMissingTable replays
// testdata/sunburst_missing_table.json: work_unit_investments absent
// short-circuits to an empty slice, matching build_investment_sunburst's
// own _tables_present early return.
func TestGoldenSunburstMissingTable(t *testing.T) {
	client := fakeQueryClient{t: t, handler: tableMissingHandler(t)}

	reader, err := NewReader(client)
	if err != nil {
		t.Fatalf("NewReader: %v", err)
	}
	got, err := BuildSunburstResponse(context.Background(), reader, "org-1", SunburstParams{
		StartTS: day(2024, 1, 1), EndTS: day(2024, 1, 31), ScopeLevel: "org", Limit: 500,
	})
	if err != nil {
		t.Fatalf("BuildSunburstResponse: %v", err)
	}
	want := loadGolden[[]SunburstSlice](t, "sunburst_missing_table.json")
	if len(got) != 0 || len(want) != 0 {
		t.Fatalf("expected both empty, got %+v want %+v", got, want)
	}
}
