package acrreconcile

import (
	"context"
	"errors"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/full-chaos/dev-health-ops/internal/storage/postgres/authschema"
)

var errNotImplemented = errors.New("acrreconcile: reader not implemented")

// DefaultPageSize is the keyset page of ReadOrg.
const DefaultPageSize = 500

// Querier is the one capability the reader holds: a read.
type Querier interface {
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
}

// Reader reads the acr rows of the credential projection.
type Reader struct{}

// NewReader returns a reader over db for the auth schema.
func NewReader(db Querier, schema authschema.ValidatedIdentifier, pageSize int) (*Reader, error) {
	return nil, errNotImplemented
}

// PageSQL is the one statement of ReadPage.
func PageSQL(schema authschema.ValidatedIdentifier) string { return "" }

// Orgs lists the organizations that hold acr projection rows, ascending.
func (r *Reader) Orgs(ctx context.Context) ([]uuid.UUID, error) { return nil, errNotImplemented }

// ReadPage reads the acr rows of one organization after the reference after.
func (r *Reader) ReadPage(ctx context.Context, org uuid.UUID, after string) ([]Projected, error) {
	return nil, errNotImplemented
}

// ReadOrg reads every acr row of one organization, in reference order.
func (r *Reader) ReadOrg(ctx context.Context, org uuid.UUID) ([]Projected, error) {
	return nil, errNotImplemented
}
