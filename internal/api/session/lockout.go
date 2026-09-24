package session

import (
	"context"
	"time"

	"github.com/jackc/pgx/v5"
)

// Lockout policy (services/login_attempts.py).
const (
	lockoutFailureThreshold = 5
	lockoutDuration         = 15 * time.Minute
)

// checkLockout is check_lockout, and for a held lock also
// get_lockout_remaining_seconds (int(total_seconds)), which login.py calls
// right after it on the same row. An expired lock is cleared (and flushed)
// on the way. attemptByEmail returns no row with an error.
func (h handlers) checkLockout(ctx context.Context, tx pgx.Tx, email string) (locked bool, remainingSeconds int64, err error) {
	now := h.Now()
	attempt, err := attemptByEmail(ctx, tx, email)
	if attempt == nil || attempt.LockedUntil == nil {
		return false, 0, err
	}
	if remaining := attempt.LockedUntil.Sub(now); remaining > 0 {
		return true, int64(remaining / time.Second), nil
	}
	attempt.AttemptCount, attempt.FirstAttemptAt, attempt.LockedUntil = 0, nil, nil
	return false, 0, updateAttempt(ctx, tx, *attempt, now)
}

// recordFailedAttempt is record_failed_attempt.
func (h handlers) recordFailedAttempt(ctx context.Context, tx pgx.Tx, email string) error {
	now := h.Now()
	attempt, err := attemptByEmail(ctx, tx, email)
	if err != nil {
		return err
	}
	if attempt == nil {
		_, err := tx.Exec(ctx, `INSERT INTO login_attempts
	(id, email, attempt_count, first_attempt_at, locked_until, created_at, updated_at)
VALUES ($1, $2, 1, $3, NULL, $3, $3)`, h.NewUUID(), email, now.UTC())
		return err
	}
	if attempt.LockedUntil != nil && attempt.LockedUntil.After(now) {
		// Only updated_at moves while a lock holds.
		return updateAttempt(ctx, tx, *attempt, now)
	}
	if attempt.LockedUntil != nil {
		attempt.AttemptCount, attempt.FirstAttemptAt, attempt.LockedUntil = 0, nil, nil
	}
	if attempt.AttemptCount == 0 {
		first := now
		attempt.FirstAttemptAt = &first
	}
	attempt.AttemptCount++
	if attempt.AttemptCount >= lockoutFailureThreshold {
		if attempt.FirstAttemptAt == nil {
			first := now
			attempt.FirstAttemptAt = &first
		}
		until := now.Add(lockoutDuration)
		attempt.LockedUntil = &until
	}
	return updateAttempt(ctx, tx, *attempt, now)
}

// clearAttempts is clear_attempts.
func clearAttempts(ctx context.Context, tx pgx.Tx, email string) error {
	attempt, err := attemptByEmail(ctx, tx, email)
	if attempt == nil {
		return err
	}
	_, err = tx.Exec(ctx, `DELETE FROM login_attempts WHERE id = $1::uuid`, attempt.ID)
	return err
}
