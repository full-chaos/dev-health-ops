package heatmap

import (
	"context"
	"fmt"
	"testing"
	"time"

	dhclickhouse "github.com/full-chaos/dev-health-go/clickhouse"
)

// fixtureRowScanner replays a fixed slice of pre-built rows -- same shape
// as quadrant/drilldown's own fixtureRowScanner, extended for this
// package's own destination types.
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
	if len(dest) != len(row) {
		return fmt.Errorf("fixtureRowScanner: dest has %d columns, row has %d", len(dest), len(row))
	}
	for i, d := range dest {
		switch typed := d.(type) {
		case *string:
			v, _ := row[i].(string)
			*typed = v
		case **string:
			if row[i] == nil {
				*typed = nil
				continue
			}
			v := row[i].(string)
			*typed = &v
		case *int32:
			*typed = row[i].(int32)
		case *int64:
			*typed = row[i].(int64)
		case *uint32:
			*typed = row[i].(uint32)
		case *uint64:
			*typed = row[i].(uint64)
		case *float64:
			*typed = row[i].(float64)
		case *time.Time:
			*typed = row[i].(time.Time)
		default:
			return fmt.Errorf("fixtureRowScanner: unsupported dest type %T", d)
		}
	}
	return nil
}

func (s *fixtureRowScanner) Err() error   { return nil }
func (s *fixtureRowScanner) Close() error { return nil }

// fakeQueryClient dispatches on query text, same convention as
// quadrant/drilldown's own fakeQueryClient.
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

func datetime(y int, m time.Month, d, h, min, sec int) time.Time {
	return time.Date(y, m, d, h, min, sec, 0, time.UTC)
}

func strPtr(s string) *string { return &s }
