package home

import (
	"context"

	dhclickhouse "github.com/full-chaos/dev-health-go/clickhouse"
	"github.com/jackc/pgx/v5"
)

// QueryClient is the read-only ClickHouse query boundary this package
// needs -- same single-method shape every sibling operation package
// declares independently (quadrant.QueryClient, sankey.QueryClient).
type QueryClient interface {
	Query(ctx context.Context, statement string, bindings []dhclickhouse.Binding) (dhclickhouse.RowScanner, error)
}

// PGQueryClient is the narrow registry-Postgres read boundary this
// package needs for the latest-successful-sync freshness read
// (queries_sync.go) -- same shape as llmorgsettings' own
// featureRowQueryer, satisfied directly by *pgxpool.Pool.
type PGQueryClient interface {
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}
