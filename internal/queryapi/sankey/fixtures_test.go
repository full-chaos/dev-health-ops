package sankey

import (
	"context"
	"fmt"
	"testing"

	dhclickhouse "github.com/full-chaos/dev-health-go/clickhouse"
)

// fixtureRowScanner replays a fixed slice of pre-built rows -- same
// convention as heatmap/quadrant's own fixtureRowScanner.
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
		case *bool:
			*typed = row[i].(bool)
		case *int64:
			*typed = row[i].(int64)
		case *float64:
			*typed = row[i].(float64)
		default:
			return fmt.Errorf("fixtureRowScanner: unsupported dest type %T", d)
		}
	}
	return nil
}

func (s *fixtureRowScanner) Err() error   { return nil }
func (s *fixtureRowScanner) Close() error { return nil }

// fakeQueryClient dispatches on query text, same convention as
// heatmap/quadrant/drilldown's own fakeQueryClient.
type fakeQueryClient struct {
	t       *testing.T
	handler func(t *testing.T, query string, bindings []dhclickhouse.Binding) (dhclickhouse.RowScanner, error)
}

func (c fakeQueryClient) Query(_ context.Context, query string, bindings []dhclickhouse.Binding) (dhclickhouse.RowScanner, error) {
	return c.handler(c.t, query, bindings)
}

// allTablesColumnsPresentClient answers every system.tables/system.columns
// probe with every requested name present, and otherwise defers to next.
func allTablesColumnsPresent(t *testing.T, query string, bindings []dhclickhouse.Binding, next func(t *testing.T, query string, bindings []dhclickhouse.Binding) (dhclickhouse.RowScanner, error)) (dhclickhouse.RowScanner, error) {
	for _, b := range bindings {
		if b.Name == "tables" || b.Name == "columns" {
			names, _ := b.Value.([]string)
			rows := make([][]any, 0, len(names))
			for _, n := range names {
				rows = append(rows, []any{n})
			}
			return &fixtureRowScanner{rows: rows}, nil
		}
	}
	return next(t, query, bindings)
}
