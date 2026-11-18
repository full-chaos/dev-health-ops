package pooledmttr

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-go/clickhouse"
)

type scanner struct {
	rows   int
	value  *float64
	cursor int
}

func (s *scanner) Next() bool { s.cursor++; return s.cursor <= s.rows }
func (s *scanner) Scan(dest ...any) error {
	*dest[0].(*string) = "total"
	*dest[1].(**string) = nil
	*dest[2].(**float64) = s.value
	return nil
}
func (s *scanner) Err() error   { return nil }
func (s *scanner) Close() error { return nil }

type querier struct {
	statements []string
	bindings   [][]clickhouse.Binding
	p50, mean  *float64
	err        error
}

func (q *querier) Query(_ context.Context, statement string, bindings []clickhouse.Binding) (clickhouse.RowScanner, error) {
	q.statements = append(q.statements, statement)
	q.bindings = append(q.bindings, bindings)
	if q.err != nil {
		return nil, q.err
	}
	if strings.Contains(statement, "pooled_incident_repairs") {
		return &scanner{rows: 1, value: q.p50}, nil
	}
	return &scanner{rows: 1, value: q.mean}, nil
}

func TestMTTRReadsTheReportSourcesOverTheWeek(t *testing.T) {
	ten, mean := 10.0, 8.4
	q := &querier{p50: &ten, mean: &mean}
	start := time.Date(2026, 2, 2, 0, 0, 0, 0, time.UTC)
	p50, got, err := MTTR(context.Background(), q, "org-1", start, start.AddDate(0, 0, 7), start.AddDate(0, 0, 7))
	if err != nil || p50 == nil || *p50 != 10 || got == nil || *got != 8.4 {
		t.Fatalf("p50 %v mean %v err %v, want 10 8.4 nil", p50, got, err)
	}
	if len(q.statements) != 2 || !strings.Contains(q.statements[0], IncidentRepairsSource) || !strings.Contains(q.statements[1], BugRepairsSource) {
		t.Fatal("the reads are not the incident then the bug repair source")
	}
	if !strings.Contains(q.statements[0], HoursMedian) || !strings.Contains(q.statements[1], HoursMean) {
		t.Error("the expressions are not the median and the mean")
	}
	names := map[string]any{}
	for _, b := range q.bindings[0] {
		names[b.Name] = b.Value
	}
	if names["org_id"] != "org-1" || names["start_date"] != "2026-02-02" || names["end_date"] != "2026-02-09" || names["as_of"] != "2026-02-09 00:00:00.000000" {
		t.Errorf("bindings %v", names)
	}
}

func TestMTTRNoSampleIsNilAndAReadErrorIsAnError(t *testing.T) {
	q := &querier{}
	p50, mean, err := MTTR(context.Background(), q, "org-1", time.Time{}, time.Time{}, time.Time{})
	if err != nil || p50 != nil || mean != nil {
		t.Errorf("p50 %v mean %v err %v, want nil nil nil", p50, mean, err)
	}
	q = &querier{err: errors.New("boom")}
	if _, _, err := MTTR(context.Background(), q, "org-1", time.Time{}, time.Time{}, time.Time{}); err == nil {
		t.Error("a failed read is no error")
	}
}
