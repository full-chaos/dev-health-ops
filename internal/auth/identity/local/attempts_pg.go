package local

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/storage/postgres/authschema"
)

// Lockout policy (go-api services/login_attempts.py).
const (
	lockoutFailureThreshold = 5
	lockoutDuration         = 15 * time.Minute
	identifierBytes         = 32
)

// PGAttempts is Attempts over auth.login_attempts. The table is unguarded
// lifecycle state; migration 0010's triggers refuse any change that would
// shorten, clear or retarget a live lock, so this code cannot do them even
// by mistake. Time is the database clock.
type PGAttempts struct {
	pool *pgxpool.Pool
	q    string
}

// NewPGAttempts builds the PostgreSQL lockout store.
func NewPGAttempts(pool *pgxpool.Pool, schema authschema.ValidatedIdentifier) *PGAttempts {
	return &PGAttempts{pool: pool, q: authschema.Quote(schema)}
}

func checkIdentifier(id []byte) error {
	if len(id) != identifierBytes {
		return fmt.Errorf("local: the lockout identifier must be %d bytes", identifierBytes)
	}
	return nil
}

// Check reports a live lock and clears an expired one.
func (a *PGAttempts) Check(ctx context.Context, id []byte) (bool, time.Duration, error) {
	if err := checkIdentifier(id); err != nil {
		return false, 0, err
	}
	var remainingSeconds *float64
	err := a.pool.QueryRow(ctx, `
		SELECT EXTRACT(EPOCH FROM (locked_until - clock_timestamp()))
		FROM `+a.q+`.login_attempts WHERE identifier_hash = $1`, id).Scan(&remainingSeconds)
	if errors.Is(err, pgx.ErrNoRows) || (err == nil && remainingSeconds == nil) {
		return false, 0, nil
	}
	if err != nil {
		return false, 0, fmt.Errorf("local: reading the lockout: %w", err)
	}
	if *remainingSeconds > 0 {
		// int(total_seconds): whole seconds, rounded down.
		return true, time.Duration(int64(*remainingSeconds)) * time.Second, nil
	}
	if _, err := a.pool.Exec(ctx, `
		UPDATE `+a.q+`.login_attempts
		SET attempt_count = 0, first_attempt_at = NULL, locked_until = NULL, updated_at = clock_timestamp()
		WHERE identifier_hash = $1 AND locked_until <= clock_timestamp()`, id); err != nil {
		return false, 0, fmt.Errorf("local: clearing an expired lock: %w", err)
	}
	return false, 0, nil
}

// RecordFailure counts one failure under a row lock, so concurrent failures
// serialise and exactly one of them sets the lock.
func (a *PGAttempts) RecordFailure(ctx context.Context, id []byte) error {
	if err := checkIdentifier(id); err != nil {
		return err
	}
	return pgx.BeginFunc(ctx, a.pool, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, `INSERT INTO `+a.q+`.login_attempts (identifier_hash, attempt_count) VALUES ($1, 0) ON CONFLICT (identifier_hash) DO NOTHING`, id); err != nil {
			return err
		}
		var (
			count  int
			first  *time.Time
			locked *time.Time
			now    time.Time
		)
		if err := tx.QueryRow(ctx, `
			SELECT attempt_count, first_attempt_at, locked_until, clock_timestamp()
			FROM `+a.q+`.login_attempts WHERE identifier_hash = $1 FOR UPDATE`, id).Scan(&count, &first, &locked, &now); err != nil {
			return err
		}
		switch {
		case locked != nil && locked.After(now):
			// Only updated_at moves while a lock holds.
		default:
			if locked != nil {
				count, first, locked = 0, nil, nil
			}
			if count == 0 {
				first = &now
			}
			count++
			if count >= lockoutFailureThreshold {
				until := now.Add(lockoutDuration)
				locked = &until
			}
		}
		_, err := tx.Exec(ctx, `
			UPDATE `+a.q+`.login_attempts
			SET attempt_count = $2, first_attempt_at = $3, locked_until = $4, updated_at = $5
			WHERE identifier_hash = $1`, id, count, first, locked, now)
		return err
	})
}

// Clear deletes the row after a successful login. A live lock refuses the
// delete (migration 0010), which is an error here: login is refused while a
// lock holds, so a success never reaches Clear with one.
func (a *PGAttempts) Clear(ctx context.Context, id []byte) error {
	if err := checkIdentifier(id); err != nil {
		return err
	}
	_, err := a.pool.Exec(ctx, `DELETE FROM `+a.q+`.login_attempts WHERE identifier_hash = $1`, id)
	return err
}
