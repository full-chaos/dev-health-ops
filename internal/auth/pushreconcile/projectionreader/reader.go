// Package projectionreader is the read-only projection side of the External
// Push reconcile (CHAOS-10897): it pages the external_push rows of the
// credential projection. It lives beside the pure core so the core keeps no
// path to a database.
package projectionreader

import (
	"context"
	"errors"

	"github.com/jackc/pgx/v5"

	"github.com/full-chaos/dev-health-ops/internal/auth/pushreconcile"
	"github.com/full-chaos/dev-health-ops/internal/storage/postgres/authschema"
)

const (
	DefaultPageSize = 500
	MaxPageSize     = 1000
)

type Querier interface {
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
}

type Reader struct{}

func NewReader(db Querier, schema authschema.ValidatedIdentifier, pageSize int) (*Reader, error) {
	return nil, errors.New("projectionreader: not implemented")
}

func PageSQL(schema authschema.ValidatedIdentifier) string { return "" }

func IDsSQL(schema authschema.ValidatedIdentifier) string { return "" }

func (r *Reader) ReadPage(ctx context.Context, after string) ([]pushreconcile.Projected, error) {
	return nil, errors.New("projectionreader: not implemented")
}

func (r *Reader) ReadByIDs(ctx context.Context, ids []string) ([]pushreconcile.Projected, error) {
	return nil, errors.New("projectionreader: not implemented")
}
