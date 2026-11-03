package acrreconcile

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/full-chaos/dev-health-ops/internal/auth/pushlifecycle"
	"github.com/full-chaos/dev-health-ops/internal/storage/postgres/authschema"
)

// DefaultPageSize is the keyset page of ReadOrg.
const DefaultPageSize = 500

// Querier is the one capability the reader holds: a read.
type Querier interface {
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
}

// Reader reads the acr rows of the credential projection. It issues SELECT
// only and holds no capability to write.
type Reader struct {
	db       Querier
	pageSQL  string
	orgsSQL  string
	pageSize int
}

// NewReader returns a reader over db for the auth schema.
func NewReader(db Querier, schema authschema.ValidatedIdentifier, pageSize int) (*Reader, error) {
	if db == nil {
		return nil, errors.New("acrreconcile: nil querier")
	}
	if pageSize < 1 {
		return nil, fmt.Errorf("acrreconcile: page size %d is below 1", pageSize)
	}
	return &Reader{db: db, pageSQL: PageSQL(schema), orgsSQL: orgsSQL(schema), pageSize: pageSize}, nil
}

// PageSQL is the one statement of ReadPage.
func PageSQL(schema authschema.ValidatedIdentifier) string {
	return `SELECT external_reference, revision, status, expires_at FROM ` + authschema.Quote(schema) + `.credential_projection
		WHERE domain_owner = 'acr' AND organization_id = $1 AND external_reference > $2
		ORDER BY external_reference LIMIT $3`
}

func orgsSQL(schema authschema.ValidatedIdentifier) string {
	return `SELECT DISTINCT organization_id FROM ` + authschema.Quote(schema) + `.credential_projection
		WHERE domain_owner = 'acr' ORDER BY organization_id`
}

// Orgs lists the organizations that hold acr projection rows, ascending.
func (r *Reader) Orgs(ctx context.Context) ([]uuid.UUID, error) {
	rows, err := r.db.Query(ctx, r.orgsSQL)
	if err != nil {
		return nil, fmt.Errorf("acrreconcile: list organizations: %w", err)
	}
	defer rows.Close()
	var out []uuid.UUID
	for rows.Next() {
		var id uuid.UUID
		if err := rows.Scan(&id); err != nil {
			return nil, fmt.Errorf("acrreconcile: scan organization: %w", err)
		}
		out = append(out, id)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("acrreconcile: list organizations: %w", err)
	}
	return out, nil
}

// ReadPage reads one page of the acr rows of one organization, after the
// reference after (empty for the first page).
func (r *Reader) ReadPage(ctx context.Context, org uuid.UUID, after string) ([]Projected, error) {
	rows, err := r.db.Query(ctx, r.pageSQL, org, after, r.pageSize)
	if err != nil {
		return nil, fmt.Errorf("acrreconcile: read projection page: %w", err)
	}
	defer rows.Close()
	var out []Projected
	for rows.Next() {
		var (
			p       Projected
			status  string
			expires *string
		)
		if err := rows.Scan(&p.ID, &p.Revision, &status, &expires); err != nil {
			return nil, fmt.Errorf("acrreconcile: scan projection row: %w", err)
		}
		p.Status = pushlifecycle.Status(status)
		if expires != nil && *expires != "" {
			t, err := time.Parse(time.RFC3339Nano, *expires)
			if err != nil {
				return nil, fmt.Errorf("acrreconcile: projection expiry of %s: %w", p.ID, err)
			}
			p.ExpiresAt = &t
		}
		out = append(out, p)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("acrreconcile: read projection page: %w", err)
	}
	return out, nil
}

// ReadOrg reads every acr row of one organization, in reference order.
func (r *Reader) ReadOrg(ctx context.Context, org uuid.UUID) ([]Projected, error) {
	var all []Projected
	after := ""
	for {
		page, err := r.ReadPage(ctx, org, after)
		if err != nil {
			return nil, err
		}
		all = append(all, page...)
		if len(page) < r.pageSize {
			return all, nil
		}
		after = page[len(page)-1].ID
	}
}
