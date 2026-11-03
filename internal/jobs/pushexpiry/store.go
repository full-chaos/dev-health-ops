package pushexpiry

import (
	"context"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/full-chaos/dev-health-ops/internal/audit"
	"github.com/full-chaos/dev-health-ops/internal/storage/postgres/authschema"
)

// Beginner opens a transaction and reads (a *pgxpool.Pool).
type Beginner interface {
	Begin(ctx context.Context) (pgx.Tx, error)
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
}

// pageSQL is a plain read of the five columns the sweep needs, bounded below by
// the window ($4): no lock clause,
// no write, no name, hash or prefix.
const pageSQL = `SELECT id, org_id, source_id, scopes, expires_at FROM external_ingest_tokens
WHERE revoked_at IS NULL AND expires_at <= now() AND expires_at > $4 AND (expires_at, id) > ($1, $2)
ORDER BY expires_at, id LIMIT $3`

type PgStore struct {
	DB     Beginner
	Schema authschema.ValidatedIdentifier
}

func (store PgStore) Page(ctx context.Context, after Cursor, limit int, notBefore time.Time) ([]Token, error) {
	if store.DB == nil {
		return nil, errors.New("push expiry store has no database")
	}
	rows, err := store.DB.Query(ctx, pageSQL, after.ExpiresAt, after.ID, limit, notBefore)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var page []Token
	for rows.Next() {
		var token Token
		if err := rows.Scan(&token.ID, &token.OrgID, &token.SourceID, &token.Scopes, &token.ExpiresAt); err != nil {
			return nil, err
		}
		page = append(page, token)
	}
	return page, rows.Err()
}

func (store PgStore) Send(ctx context.Context, token Token) error {
	if store.DB == nil {
		return errors.New("push expiry store has no database")
	}
	event, err := ExpiredEvent(token)
	if err != nil {
		return err
	}
	tx, err := store.DB.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	if err := audit.AppendPushLifecycle(ctx, tx, store.Schema, []audit.PushLifecycleEvent{event}); err != nil {
		return err
	}
	return tx.Commit(ctx)
}
