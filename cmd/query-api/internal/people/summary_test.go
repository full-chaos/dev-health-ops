package people

import (
	"context"
	"encoding/json"
	"os"
	"strings"
	"testing"
	"time"

	dhclickhouse "github.com/full-chaos/dev-health-go/clickhouse"
)

// valueTableRow scans a single float64 column.
type scalarFloatScanner struct {
	value float64
	done  bool
}

func (s *scalarFloatScanner) Next() bool {
	if s.done {
		return false
	}
	s.done = true
	return true
}
func (s *scalarFloatScanner) Scan(dest ...any) error {
	*dest[0].(*float64) = s.value
	return nil
}
func (s *scalarFloatScanner) Err() error   { return nil }
func (s *scalarFloatScanner) Close() error { return nil }

// summaryGoldenValueTable mirrors capture_summary.py's VALUE_TABLE: column
// -> (current, previous), keyed by the metric's own {column} literal
// (unique per metric in _PERSON_METRICS, see metricconfig.go).
var summaryGoldenValueTable = map[string][2]float64{
	"cycle_time_p50_hours":         {48.0, 60.0},
	"pr_first_review_p50_hours":    {5.0, 4.0},
	"items_completed":              {20.0, 15.0},
	"loc_touched":                  {500.0, 500.0},
	"wip_count_end_of_day":         {3.0, 3.0},
	"if(status = 'blocked', 1, 0)": {2.0, 1.0},
}

func columnForSummaryQuery(query string) string {
	for column := range summaryGoldenValueTable {
		if strings.Contains(query, column) {
			return column
		}
	}
	return ""
}

// newSummaryGoldenClient replays the SAME fixture data
// testdata/summary_golden.json was captured against (see
// capture_summary.py in this PR's own TEST-EVIDENCE), dispatched by
// query-shape the same way quadrant/golden_test.go's own fakeQueryClient
// handler does.
func newSummaryGoldenClient(t *testing.T) QueryClient {
	return fakeQueryClient{t: t, handler: func(t *testing.T, query string, bindings []dhclickhouse.Binding) (dhclickhouse.RowScanner, error) {
		switch {
		case strings.Contains(query, "lower(hex(MD5(identity)))"):
			return &scalarStringScanner{values: []string{"alice@example.com"}}, nil
		case strings.Contains(query, "maxOrNull(computed_at)"):
			ts := time.Date(2024, 6, 14, 12, 0, 0, 0, time.UTC)
			return &nullableTimeScanner{value: &ts}, nil
		case strings.Contains(query, "countDistinct(id)") && strings.Contains(query, "FROM repos FINAL"):
			return &scalarFloatScanner{value: 5}, nil
		case strings.Contains(query, "countDistinct(repo_id)"):
			return &scalarFloatScanner{value: 4}, nil
		case strings.Contains(query, "countIf(work_scope_id"):
			return &pairFloatScanner{a: 6, b: 10}, nil
		case strings.Contains(query, "countIf(cycle_time_hours IS NOT NULL)"):
			return &pairFloatScanner{a: 5, b: 10}, nil
		case strings.Contains(query, "countDistinct(source)"):
			return &scalarUint64Scanner{value: 2}, nil
		case strings.Contains(query, "lower(if(type = '' OR type IS NULL, 'unknown', type))"):
			return &workMixScanner{rows: [][3]any{
				{"bug", "Bug", 5.0},
				{"unknown", "Unknown", 2.0},
			}}, nil
		case strings.Contains(query, "'Active' AS stage"):
			return &flowScanner{rows: [][3]any{
				{"Active", 12.5, "hours"},
				{"Waiting", 3.2, "hours"},
			}}, nil
		case strings.Contains(query, "latest_user_metrics"):
			return &collabScanner{rows: [][3]any{
				{"review_load", "Reviews given", 10.0},
				{"review_load", "Reviews received", 8.0},
				{"review_load", "PRs authored", 6.0},
				{"review_load", "PRs merged", 5.0},
				{"handoff_points", "Items started", 4.0},
				{"handoff_points", "Items completed", 3.0},
			}}, nil
		case strings.Contains(query, "GROUP BY day"):
			column := columnForSummaryQuery(query)
			current := summaryGoldenValueTable[column][0]
			return &seriesScanner{rows: [][2]any{
				{day(2024, 6, 10), current * 0.8},
				{day(2024, 6, 12), current},
			}}, nil
		default:
			column := columnForSummaryQuery(query)
			pair := summaryGoldenValueTable[column]
			startDay, _ := bindingValue(bindings, "start_day")
			if startDay == "2024-06-02" {
				return &scalarFloatScanner{value: pair[0]}, nil
			}
			return &scalarFloatScanner{value: pair[1]}, nil
		}
	}}
}

// nullableTimeScanner replays a single nullable DateTime column, matching
// scanNullableTime's own **time.Time destination shape.
type nullableTimeScanner struct {
	value *time.Time
	done  bool
}

func (s *nullableTimeScanner) Next() bool {
	if s.done {
		return false
	}
	s.done = true
	return true
}
func (s *nullableTimeScanner) Scan(dest ...any) error {
	*dest[0].(**time.Time) = s.value
	return nil
}
func (s *nullableTimeScanner) Err() error   { return nil }
func (s *nullableTimeScanner) Close() error { return nil }

type pairFloatScanner struct {
	a, b float64
	done bool
}

func (s *pairFloatScanner) Next() bool {
	if s.done {
		return false
	}
	s.done = true
	return true
}
func (s *pairFloatScanner) Scan(dest ...any) error {
	*dest[0].(*float64) = s.a
	*dest[1].(*float64) = s.b
	return nil
}
func (s *pairFloatScanner) Err() error   { return nil }
func (s *pairFloatScanner) Close() error { return nil }

// scalarUint64Scanner replays a single uint64 column -- countDistinct()'s
// own ClickHouse-native type, matching fetchIdentityCoverage's scan
// destination (summary.go's own doc comment there).
type scalarUint64Scanner struct {
	value uint64
	done  bool
}

func (s *scalarUint64Scanner) Next() bool {
	if s.done {
		return false
	}
	s.done = true
	return true
}
func (s *scalarUint64Scanner) Scan(dest ...any) error {
	*dest[0].(*uint64) = s.value
	return nil
}
func (s *scalarUint64Scanner) Err() error   { return nil }
func (s *scalarUint64Scanner) Close() error { return nil }

type workMixScanner struct {
	rows  [][3]any
	index int
}

func (s *workMixScanner) Next() bool {
	if s.index >= len(s.rows) {
		return false
	}
	s.index++
	return true
}
func (s *workMixScanner) Scan(dest ...any) error {
	row := s.rows[s.index-1]
	*dest[0].(*string) = row[0].(string)
	*dest[1].(*string) = row[1].(string)
	*dest[2].(*float64) = row[2].(float64)
	return nil
}
func (s *workMixScanner) Err() error   { return nil }
func (s *workMixScanner) Close() error { return nil }

type flowScanner struct {
	rows  [][3]any
	index int
}

func (s *flowScanner) Next() bool {
	if s.index >= len(s.rows) {
		return false
	}
	s.index++
	return true
}
func (s *flowScanner) Scan(dest ...any) error {
	row := s.rows[s.index-1]
	*dest[0].(*string) = row[0].(string)
	*dest[1].(*float64) = row[1].(float64)
	*dest[2].(*string) = row[2].(string)
	return nil
}
func (s *flowScanner) Err() error   { return nil }
func (s *flowScanner) Close() error { return nil }

type collabScanner struct {
	rows  [][3]any
	index int
}

func (s *collabScanner) Next() bool {
	if s.index >= len(s.rows) {
		return false
	}
	s.index++
	return true
}
func (s *collabScanner) Scan(dest ...any) error {
	row := s.rows[s.index-1]
	*dest[0].(*string) = row[0].(string)
	*dest[1].(*string) = row[1].(string)
	*dest[2].(*float64) = row[2].(float64)
	return nil
}
func (s *collabScanner) Err() error   { return nil }
func (s *collabScanner) Close() error { return nil }

type seriesScanner struct {
	rows  [][2]any
	index int
}

func (s *seriesScanner) Next() bool {
	if s.index >= len(s.rows) {
		return false
	}
	s.index++
	return true
}
func (s *seriesScanner) Scan(dest ...any) error {
	row := s.rows[s.index-1]
	*dest[0].(*time.Time) = row[0].(time.Time)
	*dest[1].(*float64) = row[1].(float64)
	return nil
}
func (s *seriesScanner) Err() error   { return nil }
func (s *seriesScanner) Close() error { return nil }

func loadSummaryGolden(t *testing.T) SummaryResponse {
	t.Helper()
	data, err := os.ReadFile("testdata/summary_golden.json")
	if err != nil {
		t.Fatalf("read golden: %v", err)
	}
	var resp SummaryResponse
	dec := json.NewDecoder(strings.NewReader(string(data)))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&resp); err != nil {
		t.Fatalf("decode golden: %v", err)
	}
	return resp
}

// TestGoldenBuildSummaryResponse replays testdata/summary_golden.json,
// captured from the REAL Python build_person_summary_response
// (monkeypatched resolve_person_identity/fetch_last_ingested_at/
// fetch_coverage/fetch_identity_coverage/fetch_person_metric_value/
// fetch_person_metric_series/fetch_person_work_mix/
// fetch_person_flow_breakdown/fetch_person_collaboration/
// clickhouse_client/utc_today) via a one-off `uv run python3`
// invocation -- see this PR's own TEST-EVIDENCE for the exact script.
//
// ONE DECLARED DIVERGENCE (same class as this binary's own established
// datetime-wire-format precedent for a similar naive-vs-canonical gap):
// every
// spark.ts value is a ClickHouse `Date` (day only, no time zone) coerced
// by Pydantic's `datetime` field into a NAIVE datetime -- the captured
// Python JSON carries "2024-06-10T00:00:00" (no "Z"/offset). This port's
// SparkPoint.Ts is a real time.Time, which Go's encoding/json always
// renders RFC3339 ("...Z"): the committed golden fixture has "Z" appended
// to every spark.ts value from the raw capture, matching Go's own
// (canonical) output rather than Python's naive-datetime baseline defect.
func TestGoldenBuildSummaryResponse(t *testing.T) {
	t.Setenv("IDENTITY_MAPPING_PATH", t.TempDir()+"/missing.yaml")

	reader, err := NewReader(newSummaryGoldenClient(t))
	if err != nil {
		t.Fatalf("NewReader: %v", err)
	}

	got, err := BuildSummaryResponse(context.Background(), reader, "org-acme", SummaryParams{
		PersonID:    "unused-input",
		RangeDays:   14,
		CompareDays: 14,
		Now:         day(2024, 6, 15),
	})
	if err != nil {
		t.Fatalf("BuildSummaryResponse: %v", err)
	}
	want := loadSummaryGolden(t)
	if gotJSON, wantJSON := mustMarshal(t, got), mustMarshal(t, want); gotJSON != wantJSON {
		t.Fatalf("response mismatch\n got:  %s\nwant: %s", gotJSON, wantJSON)
	}
}

// TestBuildSummaryResponsePersonNotFound pins `if not identity: raise
// ValueError("person not found")` (services/people.py:473-474) -> a
// *RequestError{404, "Person not found"}.
func TestBuildSummaryResponsePersonNotFound(t *testing.T) {
	t.Setenv("IDENTITY_MAPPING_PATH", t.TempDir()+"/missing.yaml")
	client := fakeQueryClient{t: t, handler: func(t *testing.T, query string, bindings []dhclickhouse.Binding) (dhclickhouse.RowScanner, error) {
		return &scalarStringScanner{}, nil
	}}
	reader, err := NewReader(client)
	if err != nil {
		t.Fatalf("NewReader: %v", err)
	}
	_, err = BuildSummaryResponse(context.Background(), reader, "org-acme", SummaryParams{
		PersonID: "no-such-person", RangeDays: 14, CompareDays: 14, Now: day(2024, 6, 15),
	})
	reqErr, ok := AsRequestError(err)
	if !ok {
		t.Fatalf("BuildSummaryResponse err = %v, want *RequestError", err)
	}
	if reqErr.Status != 404 || reqErr.Message != "Person not found" {
		t.Fatalf("got %+v, want {404 Person not found}", reqErr)
	}
}
