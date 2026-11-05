package customerpush

import (
	"context"
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/full-chaos/dev-health-ops/internal/auth/pushreconcile"
	"github.com/full-chaos/dev-health-ops/internal/auth/pushreconcile/projectionreader"
	"github.com/full-chaos/dev-health-ops/internal/storage/postgres/authschema"
)

// The domain read of the reconcile names six columns: no hash, prefix, name or
// last-use column. Keyset by id (uuid order), and a by-ids read.
const (
	reconcileTokenColumns = `id, org_id, source_id, scopes, expires_at, revoked_at`
	reconcileTokenPageSQL = `SELECT ` + reconcileTokenColumns + ` FROM external_ingest_tokens WHERE id > $1 ORDER BY id LIMIT $2`
	reconcileTokenIDsSQL  = `SELECT ` + reconcileTokenColumns + ` FROM external_ingest_tokens WHERE id = ANY($1) ORDER BY id`
)

// TxBeginner starts the one transaction of a page pair.
type TxBeginner interface {
	BeginTx(ctx context.Context, opts pgx.TxOptions) (pgx.Tx, error)
}

// PagePair is one page of one side and the matching rows of the other side,
// read in one snapshot. Next is the keyset position after the page (a token id
// or a projection reference); Last is true when the page was the final one.
type PagePair struct {
	Tokens    []pushreconcile.Token
	Projected []pushreconcile.Projected
	Next      string
	Last      bool
}

func scanReconcileToken(rows pgx.Rows) (tokenRow, error) {
	var t tokenRow
	err := rows.Scan(&t.ID, &t.OrgID, &t.SourceID, &t.Scopes, &t.ExpiresAt, &t.RevokedAt)
	return t, err
}

func readTokens(ctx context.Context, q projectionreader.Querier, sql string, args ...any) ([]pushreconcile.Token, []uuid.UUID, error) {
	rows, err := q.Query(ctx, sql, args...)
	if err != nil {
		return nil, nil, fmt.Errorf("customerpush: read tokens for the reconcile: %w", err)
	}
	defer rows.Close()
	var (
		tokens []pushreconcile.Token
		ids    []uuid.UUID
	)
	for rows.Next() {
		t, err := scanReconcileToken(rows)
		if err != nil {
			return nil, nil, fmt.Errorf("customerpush: scan token for the reconcile: %w", err)
		}
		tokens = append(tokens, ExpectedState(t))
		ids = append(ids, t.ID)
	}
	if err := rows.Err(); err != nil {
		return nil, nil, fmt.Errorf("customerpush: read tokens for the reconcile: %w", err)
	}
	return tokens, ids, nil
}

func checkBatch(batch int) error {
	if batch < 1 || batch > projectionreader.MaxPageSize {
		return fmt.Errorf("customerpush: reconcile batch %d is outside 1..%d", batch, projectionreader.MaxPageSize)
	}
	return nil
}

// snapshot runs fn in one REPEATABLE READ, READ ONLY transaction: both reads of a
// page pair see one snapshot of both schemas. The transaction takes no lock and ends with the page.
func snapshot(ctx context.Context, db TxBeginner, fn func(pgx.Tx) error) error {
	tx, err := db.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
	if err != nil {
		return fmt.Errorf("customerpush: begin reconcile snapshot: %w", err)
	}
	if err := fn(tx); err != nil {
		_ = tx.Rollback(ctx)
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("customerpush: end reconcile snapshot: %w", err)
	}
	return nil
}

// ReadDomainPage reads one page of tokens after the token id after, and the
// projection rows of those tokens, in one snapshot.
func ReadDomainPage(ctx context.Context, db TxBeginner, schema authschema.ValidatedIdentifier, after uuid.UUID, batch int) (PagePair, error) {
	if err := checkBatch(batch); err != nil {
		return PagePair{}, err
	}
	var pair PagePair
	err := snapshot(ctx, db, func(tx pgx.Tx) error {
		tokens, ids, err := readTokens(ctx, tx, reconcileTokenPageSQL, after, batch)
		if err != nil {
			return err
		}
		pair.Tokens = tokens
		pair.Last = len(tokens) < batch
		if len(ids) == 0 {
			return nil
		}
		pair.Next = ids[len(ids)-1].String()
		refs := make([]string, len(ids))
		for i, id := range ids {
			refs[i] = id.String()
		}
		reader, err := projectionreader.NewReader(tx, schema, batch)
		if err != nil {
			return err
		}
		pair.Projected, err = reader.ReadByIDs(ctx, refs)
		return err
	})
	if err != nil {
		return PagePair{}, err
	}
	return pair, nil
}

// ReadProjectionPage reads one page of projection rows after the reference
// after, and the tokens of those references, in one snapshot. A reference that
// is not a token id has no token.
func ReadProjectionPage(ctx context.Context, db TxBeginner, schema authschema.ValidatedIdentifier, after string, batch int) (PagePair, error) {
	if err := checkBatch(batch); err != nil {
		return PagePair{}, err
	}
	var pair PagePair
	err := snapshot(ctx, db, func(tx pgx.Tx) error {
		reader, err := projectionreader.NewReader(tx, schema, batch)
		if err != nil {
			return err
		}
		rows, err := reader.ReadPage(ctx, after)
		if err != nil {
			return err
		}
		pair.Projected = rows
		pair.Last = len(rows) < batch
		if len(rows) == 0 {
			return nil
		}
		pair.Next = rows[len(rows)-1].ID
		var ids []uuid.UUID
		for _, r := range rows {
			if id, err := uuid.Parse(r.ID); err == nil {
				ids = append(ids, id)
			}
		}
		if len(ids) == 0 {
			return nil
		}
		pair.Tokens, _, err = readTokens(ctx, tx, reconcileTokenIDsSQL, ids)
		return err
	})
	if err != nil {
		return PagePair{}, err
	}
	return pair, nil
}
