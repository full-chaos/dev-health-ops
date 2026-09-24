package people

import (
	"context"
	"encoding/json"
	"os"
	"strings"
	"testing"

	dhclickhouse "github.com/full-chaos/dev-health-go/clickhouse"
)

func loadMetricGolden(t *testing.T) MetricResponse {
	t.Helper()
	data, err := os.ReadFile("testdata/metric_golden.json")
	if err != nil {
		t.Fatalf("read golden: %v", err)
	}
	var resp MetricResponse
	dec := json.NewDecoder(strings.NewReader(string(data)))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&resp); err != nil {
		t.Fatalf("decode golden: %v", err)
	}
	return resp
}

// breakdownRowScanner scans (label, value) pairs -- fetchPersonBreakdown's
// own row shape.
type breakdownRowScanner struct {
	rows  [][2]any
	index int
}

func (s *breakdownRowScanner) Next() bool {
	if s.index >= len(s.rows) {
		return false
	}
	s.index++
	return true
}
func (s *breakdownRowScanner) Scan(dest ...any) error {
	row := s.rows[s.index-1]
	*dest[0].(*string) = row[0].(string)
	*dest[1].(*float64) = row[1].(float64)
	return nil
}
func (s *breakdownRowScanner) Err() error   { return nil }
func (s *breakdownRowScanner) Close() error { return nil }

// newMetricGoldenClient replays the SAME fixture data
// testdata/metric_golden.json was captured against for metric=
// "review_latency" (see capture_metric.py in this PR's own
// TEST-EVIDENCE): a timeseries with a NULL row (safe_float -> 0.0) and a
// by_repo breakdown (review_latency's only configured breakdown --
// by_work_type/by_stage stay empty, cfg.ByWorkType/cfg.ByStage are nil
// for this metric).
func newMetricGoldenClient(t *testing.T) QueryClient {
	return fakeQueryClient{t: t, handler: func(t *testing.T, query string, bindings []dhclickhouse.Binding) (dhclickhouse.RowScanner, error) {
		switch {
		case strings.Contains(query, "lower(hex(MD5(identity)))"):
			return &scalarStringScanner{values: []string{"alice@example.com"}}, nil
		case strings.Contains(query, "GROUP BY day"):
			return &seriesScanner{rows: [][2]any{
				{day(2024, 6, 10), 4.0},
				{day(2024, 6, 12), 5.0},
				{day(2024, 6, 13), 0.0},
			}}, nil
		case strings.Contains(query, "AS label"):
			return &breakdownRowScanner{rows: [][2]any{
				{"acme/web", 6.0},
				{"acme/api", 2.0},
			}}, nil
		default:
			t.Fatalf("unexpected query: %s", query)
			return nil, nil
		}
	}}
}

// TestGoldenBuildMetricResponse replays testdata/metric_golden.json,
// captured from the REAL Python build_person_metric_response
// (monkeypatched resolve_person_identity/fetch_person_metric_series/
// fetch_person_breakdown/clickhouse_client/utc_today) for metric=
// "review_latency" -- see this PR's own TEST-EVIDENCE for the exact
// script.
func TestGoldenBuildMetricResponse(t *testing.T) {
	t.Setenv("IDENTITY_MAPPING_PATH", t.TempDir()+"/missing.yaml")

	reader, err := NewReader(newMetricGoldenClient(t))
	if err != nil {
		t.Fatalf("NewReader: %v", err)
	}

	got, err := BuildMetricResponse(context.Background(), reader, "org-acme", MetricParams{
		PersonID:    "unused-input",
		Metric:      "review_latency",
		RangeDays:   14,
		CompareDays: 14,
		Now:         day(2024, 6, 15),
	})
	if err != nil {
		t.Fatalf("BuildMetricResponse: %v", err)
	}
	want := loadMetricGolden(t)
	if gotJSON, wantJSON := mustMarshal(t, got), mustMarshal(t, want); gotJSON != wantJSON {
		t.Fatalf("response mismatch\n got:  %s\nwant: %s", gotJSON, wantJSON)
	}
}

// TestBuildMetricResponseUnsupportedMetric pins `if not config: raise
// ValueError("metric not supported")` (services/people.py:641-643) ->
// a *RequestError{400, "Metric not supported"} -- checked BEFORE any
// ClickHouse call (a failing client must not error).
func TestBuildMetricResponseUnsupportedMetric(t *testing.T) {
	client := fakeQueryClient{t: t, handler: func(t *testing.T, query string, bindings []dhclickhouse.Binding) (dhclickhouse.RowScanner, error) {
		t.Fatalf("query client must not be called for an unsupported metric")
		return nil, nil
	}}
	reader, err := NewReader(client)
	if err != nil {
		t.Fatalf("NewReader: %v", err)
	}
	_, err = BuildMetricResponse(context.Background(), reader, "org-acme", MetricParams{
		PersonID: "person-1", Metric: "not-a-real-metric", RangeDays: 14, CompareDays: 14, Now: day(2024, 6, 15),
	})
	reqErr, ok := AsRequestError(err)
	if !ok {
		t.Fatalf("BuildMetricResponse err = %v, want *RequestError", err)
	}
	if reqErr.Status != 400 || reqErr.Message != "Metric not supported" {
		t.Fatalf("got %+v, want {400 Metric not supported}", reqErr)
	}
}

// TestBuildMetricResponsePersonNotFound pins the 404 branch -- reached
// only after the metric itself is validated (services/people.py:641-656:
// config lookup happens before _resolve_identity_context).
func TestBuildMetricResponsePersonNotFound(t *testing.T) {
	t.Setenv("IDENTITY_MAPPING_PATH", t.TempDir()+"/missing.yaml")
	client := fakeQueryClient{t: t, handler: func(t *testing.T, query string, bindings []dhclickhouse.Binding) (dhclickhouse.RowScanner, error) {
		return &scalarStringScanner{}, nil
	}}
	reader, err := NewReader(client)
	if err != nil {
		t.Fatalf("NewReader: %v", err)
	}
	_, err = BuildMetricResponse(context.Background(), reader, "org-acme", MetricParams{
		PersonID: "no-such-person", Metric: "churn", RangeDays: 14, CompareDays: 14, Now: day(2024, 6, 15),
	})
	reqErr, ok := AsRequestError(err)
	if !ok {
		t.Fatalf("BuildMetricResponse err = %v, want *RequestError", err)
	}
	if reqErr.Status != 404 || reqErr.Message != "Person not found" {
		t.Fatalf("got %+v, want {404 Person not found}", reqErr)
	}
}
