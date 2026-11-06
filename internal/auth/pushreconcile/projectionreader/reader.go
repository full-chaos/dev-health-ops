// Package projectionreader is the read-only projection side of the External
// Push reconcile (CHAOS-10897): it pages the external_push rows of the
// credential projection. It lives beside the pure core so the core keeps no
// path to a database. It issues SELECT only, holds no capability to write, and
// reads only the columns the projection reader grant names
// (authschema.ProjectionReaderReads): no fingerprint and no terminal column.
package projectionreader

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/full-chaos/dev-health-ops/internal/auth/pushlifecycle"
	"github.com/full-chaos/dev-health-ops/internal/auth/pushreconcile"
	"github.com/full-chaos/dev-health-ops/internal/storage/postgres/authschema"
)

const (
	// DefaultPageSize is the page of one walk step.
	DefaultPageSize = 500
	// MaxPageSize bounds one page and one by-ids read.
	MaxPageSize = 1000
)

// Querier is the one capability the reader holds: a read.
type Querier interface {
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
}

// Reader reads the external_push rows of the credential projection.
type Reader struct {
	db       Querier
	pageSQL  string
	idsSQL   string
	pageSize int
}

// NewReader returns a reader over db for the auth schema. pageSize is 1..MaxPageSize.
func NewReader(db Querier, schema authschema.ValidatedIdentifier, pageSize int) (*Reader, error) {
	if db == nil {
		return nil, errors.New("projectionreader: nil querier")
	}
	if pageSize < 1 || pageSize > MaxPageSize {
		return nil, fmt.Errorf("projectionreader: page size %d is outside 1..%d", pageSize, MaxPageSize)
	}
	return &Reader{db: db, pageSQL: PageSQL(schema), idsSQL: IDsSQL(schema), pageSize: pageSize}, nil
}

const selectColumns = `external_reference, organization_id, source_id, status, scopes, expires_at`

// PageSQL is the one statement of ReadPage: keyset by external_reference in
// byte order (COLLATE "C"), so the order is the order of the ids the domain
// side sorts by, whatever the database collation.
func PageSQL(schema authschema.ValidatedIdentifier) string {
	return `SELECT ` + selectColumns + ` FROM ` + authschema.Quote(schema) + `.credential_projection
		WHERE domain_owner = 'external_push' AND external_reference > $1
		ORDER BY external_reference COLLATE "C" LIMIT $2`
}

// IDsSQL is the one statement of ReadByIDs.
func IDsSQL(schema authschema.ValidatedIdentifier) string {
	return `SELECT ` + selectColumns + ` FROM ` + authschema.Quote(schema) + `.credential_projection
		WHERE domain_owner = 'external_push' AND external_reference = ANY($1)
		ORDER BY external_reference COLLATE "C"`
}

// ReadPage reads one page after the reference after (empty for the first page).
func (r *Reader) ReadPage(ctx context.Context, after string) ([]pushreconcile.Projected, error) {
	return r.read(ctx, r.pageSQL, after, r.pageSize)
}

// ReadByIDs reads the rows of the given references; a reference with no row is absent from the result.
func (r *Reader) ReadByIDs(ctx context.Context, ids []string) ([]pushreconcile.Projected, error) {
	if len(ids) == 0 {
		return nil, nil
	}
	if len(ids) > r.pageSize {
		return nil, fmt.Errorf("projectionreader: %d ids exceed the page size %d", len(ids), r.pageSize)
	}
	return r.read(ctx, r.idsSQL, ids)
}

func (r *Reader) read(ctx context.Context, sql string, args ...any) ([]pushreconcile.Projected, error) {
	rows, err := r.db.Query(ctx, sql, args...)
	if err != nil {
		return nil, fmt.Errorf("projectionreader: read projection: %w", err)
	}
	defer rows.Close()
	var out []pushreconcile.Projected
	for rows.Next() {
		var (
			p       pushreconcile.Projected
			org     uuid.UUID
			source  *string
			status  string
			expires *string
		)
		if err := rows.Scan(&p.ID, &org, &source, &status, &p.Scopes, &expires); err != nil {
			return nil, fmt.Errorf("projectionreader: scan projection row: %w", err)
		}
		p.Organization = org.String()
		if source != nil {
			p.SourceID = *source
		}
		p.Status = pushlifecycle.Status(status)
		if expires != nil && *expires != "" {
			t, err := time.Parse(time.RFC3339Nano, *expires)
			if err != nil {
				return nil, fmt.Errorf("projectionreader: expiry of %s: %w", p.ID, err)
			}
			t = t.UTC()
			p.ExpiresAt = &t
		}
		out = append(out, p)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("projectionreader: read projection: %w", err)
	}
	return out, nil
}
