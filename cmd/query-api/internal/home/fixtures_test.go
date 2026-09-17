package home

import (
	"context"
	"testing"
	"time"

	dhclickhouse "github.com/full-chaos/dev-health-go/clickhouse"
)

// fixtureRowScanner replays a fixed slice of pre-built rows -- same
// shape as quadrant/sankey's own test doubles.
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
		case *time.Time:
			*typed = row[i].(time.Time)
		case **time.Time:
			if row[i] == nil {
				*typed = nil
			} else {
				v := row[i].(time.Time)
				*typed = &v
			}
		case *string:
			*typed = row[i].(string)
		case *float64:
			*typed = row[i].(float64)
		case **float64:
			if row[i] == nil {
				*typed = nil
			} else {
				v := row[i].(float64)
				*typed = &v
			}
		case *int64:
			*typed = row[i].(int64)
		case *bool:
			*typed = row[i].(bool)
		}
	}
	return nil
}

func (s *fixtureRowScanner) Err() error   { return nil }
func (s *fixtureRowScanner) Close() error { return nil }

type fakeQueryClient struct {
	t       *testing.T
	handler func(t *testing.T, query string, bindings []dhclickhouse.Binding) (dhclickhouse.RowScanner, error)
}

func (c fakeQueryClient) Query(_ context.Context, query string, bindings []dhclickhouse.Binding) (dhclickhouse.RowScanner, error) {
	return c.handler(c.t, query, bindings)
}

func day(y int, m time.Month, d int) time.Time {
	return time.Date(y, m, d, 0, 0, 0, 0, time.UTC)
}
