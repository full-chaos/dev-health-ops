package session

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/audit"
	"github.com/full-chaos/dev-health-ops/internal/auth/requestid"
	"github.com/full-chaos/dev-health-ops/internal/storage/postgres/authschema"
)

// UnpinnedBatch is how many sessions one RevokeUnpinned transaction ends.
const UnpinnedBatch = 500

// revokedUnpinned is the revoked_reason of a session that had no
// legacy_token_version pin.
const revokedUnpinned = "pin_missing"

// RevokeUnpinned ends every session that has no legacy_token_version pin and
// is not yet revoked, with its refresh credentials, in batches of
// UnpinnedBatch rows. A batch is one audit.Commit transaction that locks its
// sessions in id order. It returns how many sessions it revoked; a second run
// returns 0. It runs as the migration role (auth-migrate sessions).
func RevokeUnpinned(ctx context.Context, pool *pgxpool.Pool, schema authschema.ValidatedIdentifier) (int, error) {
	q := authschema.Quote(schema)
	total := 0
	for {
		n := 0
		err := audit.Commit(ctx, pool, schema, audit.Mutation{
			Apply: func(ctx context.Context, tx audit.TxOps) error {
				rows, err := tx.Query(ctx, `
					SELECT id::text FROM `+q+`.sessions
					WHERE legacy_token_version IS NULL AND revoked_at IS NULL
					ORDER BY id LIMIT $1 FOR UPDATE`, UnpinnedBatch)
				if err != nil {
					return err
				}
				var ids []string
				for rows.Next() {
					var id string
					if err := rows.Scan(&id); err != nil {
						rows.Close()
						return err
					}
					ids = append(ids, id)
				}
				rows.Close()
				if err := rows.Err(); err != nil {
					return err
				}
				n = len(ids)
				if n == 0 {
					return nil
				}
				if _, err := tx.Exec(ctx, `
					UPDATE `+q+`.refresh_credentials
					SET revoked_at = clock_timestamp(), revoked_reason = $2, updated_at = clock_timestamp()
					WHERE session_id = ANY($1::uuid[]) AND revoked_at IS NULL`, ids, revokedUnpinned); err != nil {
					return err
				}
				_, err = tx.Exec(ctx, `
					UPDATE `+q+`.sessions
					SET revoked_at = clock_timestamp(), revoked_reason = $2, updated_at = clock_timestamp()
					WHERE id = ANY($1::uuid[]) AND revoked_at IS NULL`, ids, revokedUnpinned)
				return err
			},
			Audit: audit.AuditEvent{
				EventType: "auth.session.revoked", Outcome: audit.OutcomeAllowed,
				Attributes: map[string]any{"reason_code": "session_revoked", "target_kind": "session"},
				RequestID:  requestid.Audit(ctx),
			},
			OffStream: audit.OffStreamSessionLifecycle,
		})
		if errors.Is(err, audit.ErrNoStateChange) {
			return total, nil
		}
		if err != nil {
			return total, fmt.Errorf("%w: revoking unpinned sessions: %w", ErrStore, err)
		}
		total += n
	}
}

// CountUnpinned reports how many sessions have no pin and are not revoked.
func CountUnpinned(ctx context.Context, pool *pgxpool.Pool, schema authschema.ValidatedIdentifier) (int, error) {
	var n int
	err := pool.QueryRow(ctx, `SELECT count(*) FROM `+authschema.Quote(schema)+`.sessions
		WHERE legacy_token_version IS NULL AND revoked_at IS NULL`).Scan(&n)
	if err != nil {
		return 0, fmt.Errorf("%w: counting unpinned sessions: %w", ErrStore, err)
	}
	return n, nil
}
