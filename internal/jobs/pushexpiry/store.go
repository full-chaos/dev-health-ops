package pushexpiry

import (
	"context"

	"github.com/jackc/pgx/v5"

	"github.com/full-chaos/dev-health-ops/internal/storage/postgres/authschema"
)

// Beginner opens a transaction and reads (a *pgxpool.Pool).
type Beginner interface {
	Begin(ctx context.Context) (pgx.Tx, error)
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
}

// pageSQL is a plain read of the five columns the sweep needs: no lock clause,
// no write, no name, hash or prefix.
const pageSQL = `SELECT id, org_id, source_id, scopes, expires_at FROM external_ingest_tokens
WHERE revoked_at IS NULL AND expires_at <= now() AND (expires_at, id) > ($1, $2)
ORDER BY expires_at, id LIMIT $3`

type PgStore struct {
	DB     Beginner
	Schema authschema.ValidatedIdentifier
}

func (store PgStore) Page(ctx context.Context, after Cursor, limit int) ([]Token, error) {
	return nil, nil
}

func (store PgStore) Send(ctx context.Context, token Token) error { return nil }
