package providerrevocation

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
)

// Beginner opens a transaction (a *pgxpool.Pool).
type Beginner interface {
	Begin(ctx context.Context) (pgx.Tx, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
}

// claimSQL selects one due row and holds it: the claim rule every sender
// shares (admin.retryPagerDutyRevocationRows uses the same FOR UPDATE SKIP
// LOCKED). Due means: below MaxAttempts; never refused, or past its backoff
// (1 min * 2^(attempts-1), capped at 6 h, from its last attempt); and not the
// setup row of a callback still in flight. The setup gate is keyed on
// created_at, which a restore-fence requeue keeps (attempts 0, updated_at now):
// a requeued row is due at once unless it is a setup row younger than 15 min.
var claimSQL = fmt.Sprintf(`SELECT id, token_encrypted, token_key_version FROM provider_oauth_revocations
WHERE provider = 'pagerduty' AND status = 'pending' AND attempts < %d
  AND (attempts = 0 OR updated_at <= now() - LEAST(interval '1 minute' * power(2, attempts - 1), interval '6 hours'))
  AND NOT (purpose = 'setup' AND attempts = 0 AND created_at > now() - interval '%d seconds')
ORDER BY updated_at, id
LIMIT 1
FOR UPDATE SKIP LOCKED`, MaxAttempts, int64(SetupStaleAfter.Seconds()))

var totalsSQL = fmt.Sprintf(`SELECT count(*), count(*) FILTER (WHERE attempts >= %d)
FROM provider_oauth_revocations WHERE provider = 'pagerduty' AND status = 'pending'`, MaxAttempts)

// exhaustedSQL takes no lock: it only names rows.
var exhaustedSQL = fmt.Sprintf(`SELECT id, COALESCE(last_error, '') FROM provider_oauth_revocations
WHERE provider = 'pagerduty' AND status = 'pending' AND attempts >= %d
ORDER BY id
LIMIT $1`, MaxAttempts)

const (
	sentSQL   = `DELETE FROM provider_oauth_revocations WHERE id = $1`
	failedSQL = `UPDATE provider_oauth_revocations SET attempts = attempts + 1, last_error = $2, updated_at = now() WHERE id = $1 RETURNING attempts`
)

// PgStore is Store over the domain pool.
type PgStore struct{ DB Beginner }

func (store PgStore) Next(ctx context.Context) (Claim, bool, error) {
	if store.DB == nil {
		return nil, false, errors.New("provider revocation store has no database")
	}
	tx, err := store.DB.Begin(ctx)
	if err != nil {
		return nil, false, err
	}
	var row Row
	if err := tx.QueryRow(ctx, claimSQL).Scan(&row.ID, &row.Ciphertext, &row.KeyVersion); err != nil {
		_ = tx.Rollback(context.WithoutCancel(ctx))
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, false, nil
		}
		return nil, false, err
	}
	return &pgClaim{tx: tx, row: row}, true, nil
}

func (store PgStore) Totals(ctx context.Context) (pending, exhausted int64, err error) {
	if store.DB == nil {
		return 0, 0, errors.New("provider revocation store has no database")
	}
	err = store.DB.QueryRow(ctx, totalsSQL).Scan(&pending, &exhausted)
	return pending, exhausted, err
}

func (store PgStore) Exhausted(ctx context.Context, limit int) ([]ExhaustedRow, error) {
	if store.DB == nil {
		return nil, errors.New("provider revocation store has no database")
	}
	rows, err := store.DB.Query(ctx, exhaustedSQL, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []ExhaustedRow
	for rows.Next() {
		var row ExhaustedRow
		var lastError string
		if err := rows.Scan(&row.ID, &lastError); err != nil {
			return nil, err
		}
		row.Reason = reasonClass(lastError)
		out = append(out, row)
	}
	return out, rows.Err()
}

// reasonClass maps a stored last_error to its class: one of the reason codes,
// or "other". The text itself is never logged.
func reasonClass(lastError string) string {
	switch lastError {
	case ReasonRemoteRevokeFailed, ReasonTokenDecryptFailed:
		return lastError
	case "":
		return "none"
	default:
		return "other"
	}
}

type pgClaim struct {
	tx  pgx.Tx
	row Row
}

func (claim *pgClaim) Row() Row { return claim.row }

func (claim *pgClaim) Sent(ctx context.Context) error {
	if _, err := claim.tx.Exec(ctx, sentSQL, claim.row.ID); err != nil {
		return err
	}
	return claim.tx.Commit(ctx)
}

func (claim *pgClaim) Failed(ctx context.Context, reason string) (int, error) {
	var attempts int
	if err := claim.tx.QueryRow(ctx, failedSQL, claim.row.ID, reason).Scan(&attempts); err != nil {
		return 0, err
	}
	return attempts, claim.tx.Commit(ctx)
}

func (claim *pgClaim) Release(ctx context.Context) {
	_ = claim.tx.Rollback(context.WithoutCancel(ctx))
}
