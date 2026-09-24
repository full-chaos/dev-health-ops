// Package datahealth serves the operator data-health surface, ported from the
// Python resolvers/data_health.py and models/data_health.py logic (the Python
// resolver is deleted; the Go tests here are its guards): connector health from
// the sync tables in Postgres, identity-mapping and mapping-coverage health
// from ClickHouse, and metric lineage freshness from the metric tables.
//
// The port keeps the Python behaviours a client can observe: all three
// sections are read on every request, a failed ClickHouse read answers an
// empty section (logged), and a failed Postgres read fails the field (logged).
package datahealth

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"strings"
	"time"

	"github.com/full-chaos/dev-health-go/clickhouse"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/graph/model"
)

// Result caps, as in the Python resolver.
const (
	maxUnmappedIdentities = 25
	maxAliasSuggestions   = 25
	aliasConfidence       = 0.82
)

// Job run statuses that count as a failed run (JobRunStatus FAILED, CANCELLED).
const (
	jobRunFailed    = 3
	jobRunCancelled = 4
)

// QueryClient is the ClickHouse read surface.
type QueryClient interface {
	Query(ctx context.Context, statement string, bindings []clickhouse.Binding) (clickhouse.RowScanner, error)
}

// PGQuerier is the read-only Postgres surface; *pgxpool.Pool satisfies it.
type PGQuerier interface {
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
}

// Principal is the verified caller as far as the operator gate reads it.
type Principal struct {
	Present     bool
	Role        string
	IsSuperuser bool
}

// AccessError is a refusal to serve the request; Authentication marks the
// "no principal at all" case, distinct from "principal without the role".
type AccessError struct {
	Message string
}

func (e *AccessError) Error() string { return e.Message }

// RequireOperator ports _require_operator: no principal is refused as
// unauthenticated; a superuser flag or the admin, owner or operator role
// (compared lowercased) passes; anything else is refused. The gate reads the
// superuser flag itself, not its verified companion.
func RequireOperator(p Principal) error {
	if !p.Present {
		return &AccessError{Message: "Authentication required"}
	}
	if p.IsSuperuser {
		return nil
	}
	switch strings.ToLower(p.Role) {
	case "admin", "owner", "operator":
		return nil
	}
	return &AccessError{Message: "Data health requires operator access"}
}

// Reader reads the data-health sections for one authorized org.
type Reader struct {
	ClickHouse QueryClient
	Postgres   PGQuerier
	// Now stamps a failure that carries no timestamp of its own.
	Now func() time.Time

	degraded bool
}

// Degraded reports whether any ClickHouse read failed and answered empty.
func (r *Reader) Degraded() bool { return r.degraded }

func (r *Reader) now() time.Time {
	if r.Now != nil {
		return r.Now().UTC()
	}
	return time.Now().UTC()
}

// Resolve ports resolve_data_health after the operator and org checks: the
// three sections are always read, in this order.
func (r *Reader) Resolve(ctx context.Context, orgID, team string) (*model.DataHealth, error) {
	connectors, err := r.Connectors(ctx, orgID)
	if err != nil {
		return nil, err
	}
	identity := r.IdentityMapping(ctx, orgID, team)
	coverage := r.MappingCoverage(ctx, orgID)
	return &model.DataHealth{
		Connectors:      connectors,
		IdentityMapping: identity,
		MappingCoverage: coverage,
		Team:            team,
	}, nil
}

// queryRows runs a ClickHouse read and hands each row to scan. A failure is
// logged, marks the reader degraded and answers no rows, as _query_dicts does.
func (r *Reader) queryRows(ctx context.Context, section, statement string, bindings []clickhouse.Binding, scan func(clickhouse.RowScanner) error) {
	if r.ClickHouse == nil {
		return
	}
	if err := runRows(ctx, r.ClickHouse, statement, bindings, scan); err != nil {
		r.degraded = true
		slog.WarnContext(ctx, "query-api: data health read failed, answering empty",
			"operation", "dataHealth", "section", section, "error", err)
	}
}

func runRows(ctx context.Context, client QueryClient, statement string, bindings []clickhouse.Binding, scan func(clickhouse.RowScanner) error) error {
	rows, err := client.Query(ctx, statement, bindings)
	if err != nil {
		return fmt.Errorf("query: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		if err := scan(rows); err != nil {
			return fmt.Errorf("scan: %w", err)
		}
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("rows: %w", err)
	}
	return nil
}

// pgErrorClass names why a Postgres read failed, so a missing grant is
// distinguishable from any other failure in the log.
func pgErrorClass(err error) string {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		switch pgErr.Code {
		case "42501":
			return "permission_denied"
		case "42P01":
			return "undefined_table"
		}
		return "pg_error"
	}
	return "connection_or_context"
}

func sortStable[T any](items []T, greater func(a, b T) bool) {
	sort.SliceStable(items, func(i, j int) bool { return greater(items[i], items[j]) })
}
