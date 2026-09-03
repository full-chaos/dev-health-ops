package investmentexplain

import (
	"context"

	dhclickhouse "github.com/full-chaos/dev-health-go/clickhouse"
)

// capturingClient is a fake analytics.QueryClient that records the last
// query/bindings it was asked to run and returns zero rows -- used for
// query-composition tests that don't need a live ClickHouse (the bigboy
// testcontainer pause means no live differential runs right now; these
// tests check the SQL text and binding shape this port builds, not what a
// real server returns for it).
type capturingClient struct {
	lastQuery    string
	lastBindings []dhclickhouse.Binding
	calls        int
}

func (c *capturingClient) Query(_ context.Context, statement string, bindings []dhclickhouse.Binding) (dhclickhouse.RowScanner, error) {
	c.calls++
	c.lastQuery = statement
	c.lastBindings = bindings
	return &emptyRowScanner{}, nil
}

// emptyRowScanner is a RowScanner with zero rows.
type emptyRowScanner struct{}

func (emptyRowScanner) Next() bool        { return false }
func (emptyRowScanner) Scan(...any) error { return nil }
func (emptyRowScanner) Err() error        { return nil }
func (emptyRowScanner) Close() error      { return nil }
