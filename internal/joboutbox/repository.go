package joboutbox

import (
	"context"
	"errors"
	"sort"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

const rowColumns = `
	outbox.id::text, outbox.dedupe_key, outbox.job_kind, outbox.contract_version,
	outbox.args, outbox.payload_hash, outbox.queue, outbox.priority,
	outbox.max_attempts, outbox.scheduled_at, outbox.status,
	COALESCE(outbox.claim_token::text, ''), outbox.claimed_at, outbox.claim_expires_at,
	outbox.attempt_count, outbox.first_attempt_at, outbox.last_attempt_at,
	outbox.next_attempt_at, outbox.last_error_code, outbox.last_error_detail,
	outbox.last_error_at, outbox.river_job_id, outbox.delivered_at,
	outbox.created_at, outbox.updated_at`

// Repository owns claim leases and the atomic insert/mark transaction.
type Repository struct {
	pool   *pgxpool.Pool
	faults repositoryFaults
}

func NewRepository(pool *pgxpool.Pool) (*Repository, error) {
	if pool == nil {
		return nil, ErrInvalidConfiguration
	}
	return &Repository{pool: pool}, nil
}

// ClaimDue atomically claims pending work and reclaims expired leases.
func (repository *Repository) ClaimDue(
	ctx context.Context,
	now time.Time,
	limit int,
	leaseDuration time.Duration,
) ([]Claim, error) {
	return repository.claimDueExcept(ctx, now, limit, leaseDuration, nil)
}

// claimDueExcept atomically claims pending work except immutable known kinds
// whose checked-in route is not executable. Unknown kinds remain eligible so
// they cannot accumulate without bounded contract-failure evidence.
func (repository *Repository) claimDueExcept(
	ctx context.Context,
	now time.Time,
	limit int,
	leaseDuration time.Duration,
	deferredKinds []string,
) ([]Claim, error) {
	if repository == nil || repository.pool == nil || now.IsZero() || limit < 1 || limit > 100 ||
		leaseDuration < time.Second || leaseDuration > 15*time.Minute || len(deferredKinds) > maxRelayPolicyKinds {
		return nil, ErrInvalidConfiguration
	}
	if deferredKinds == nil {
		// pgx encodes a nil slice as SQL NULL; ALL(NULL) is unknown and would
		// suppress every candidate instead of applying an empty exclusion.
		deferredKinds = []string{}
	}
	tx, err := repository.pool.Begin(ctx)
	if err != nil {
		return nil, ErrUnavailable
	}
	defer func() { _ = tx.Rollback(ctx) }()
	rows, err := tx.Query(ctx, `
		WITH candidates AS (
			SELECT id
			FROM public.worker_job_outbox AS candidate
			WHERE (
				(candidate.status = 'pending' AND candidate.scheduled_at <= $1 AND candidate.next_attempt_at <= $1)
				OR (candidate.status = 'claimed' AND candidate.claim_expires_at <= $1)
			)
			AND candidate.job_kind <> ALL($4::text[])
			AND (
				candidate.prerequisite_completion_key IS NULL
				OR EXISTS (
					SELECT 1
					FROM public.worker_job_completion_fences AS completion
					WHERE completion.completion_key = candidate.prerequisite_completion_key
				)
			)
			ORDER BY candidate.next_attempt_at, candidate.created_at, candidate.id
			FOR UPDATE SKIP LOCKED
			LIMIT $2
		)
		UPDATE public.worker_job_outbox AS outbox
		SET status = 'claimed',
			claim_token = gen_random_uuid(),
			claimed_at = $1,
			claim_expires_at = $3,
			attempt_count = outbox.attempt_count + 1,
			first_attempt_at = COALESCE(outbox.first_attempt_at, $1),
			last_attempt_at = $1,
			updated_at = $1
		FROM candidates
		WHERE outbox.id = candidates.id
		RETURNING `+rowColumns, now.UTC(), limit, now.UTC().Add(leaseDuration), deferredKinds)
	if err != nil {
		return nil, ErrUnavailable
	}
	defer rows.Close()
	claims := make([]Claim, 0, limit)
	for rows.Next() {
		row, scanErr := scanRow(rows)
		if scanErr != nil {
			return nil, ErrUnavailable
		}
		claims = append(claims, Claim{Row: row})
	}
	if rows.Err() != nil {
		return nil, ErrUnavailable
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, ErrUnavailable
	}
	sort.Slice(claims, func(left, right int) bool {
		if claims[left].NextAttemptAt.Equal(claims[right].NextAttemptAt) {
			return claims[left].ID < claims[right].ID
		}
		return claims[left].NextAttemptAt.Before(claims[right].NextAttemptAt)
	})
	return claims, nil
}

// Dispatch executes the supported River insert and delivered mark in one
// queue-control transaction. Any failure before commit persists neither.
func (repository *Repository) Dispatch(
	ctx context.Context,
	claim Claim,
	now time.Time,
	insert insertFunc,
) (int64, error) {
	if repository == nil || repository.pool == nil || insert == nil || now.IsZero() ||
		!uuidPattern.MatchString(claim.ID) || !uuidPattern.MatchString(claim.ClaimToken) {
		return 0, ErrInvalidConfiguration
	}
	tx, err := repository.pool.Begin(ctx)
	if err != nil {
		return 0, ErrUnavailable
	}
	defer func() { _ = tx.Rollback(ctx) }()
	row, err := scanRow(tx.QueryRow(ctx, `
		SELECT `+rowColumns+`
		FROM public.worker_job_outbox AS outbox
		WHERE outbox.id = $1 AND outbox.status = 'claimed' AND outbox.claim_token = $2
			AND outbox.claim_expires_at > statement_timestamp()
		FOR UPDATE`, claim.ID, claim.ClaimToken))
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, ErrLeaseLost
	}
	if err != nil {
		return 0, ErrUnavailable
	}
	if err := callFault(repository.faults.beforeInsert); err != nil {
		return 0, err
	}
	riverJobID, err := insert(ctx, tx, row)
	if err != nil {
		return 0, err
	}
	if err := callFault(repository.faults.afterInsert); err != nil {
		return 0, err
	}
	if err := callFault(repository.faults.beforeMark); err != nil {
		return 0, err
	}
	command, err := tx.Exec(ctx, `
		UPDATE public.worker_job_outbox
		SET status = 'delivered', river_job_id = $1, delivered_at = statement_timestamp(),
			claim_token = NULL, claimed_at = NULL, claim_expires_at = NULL,
			updated_at = statement_timestamp()
		WHERE id = $2 AND status = 'claimed' AND claim_token = $3
			AND claim_expires_at > statement_timestamp()`, riverJobID, claim.ID, claim.ClaimToken)
	if err != nil {
		return 0, ErrUnavailable
	}
	if command.RowsAffected() != 1 {
		return 0, ErrLeaseLost
	}
	if err := callFault(repository.faults.afterMark); err != nil {
		return 0, err
	}
	if err := tx.Commit(ctx); err != nil {
		return 0, ErrUnavailable
	}
	if err := callFault(repository.faults.afterCommit); err != nil {
		return riverJobID, err
	}
	return riverJobID, nil
}

func (repository *Repository) recordFailure(
	ctx context.Context,
	claim Claim,
	now time.Time,
	kind failureKind,
	maxRelayAttempts int,
	nextAttemptAt time.Time,
) error {
	if repository == nil || repository.pool == nil || now.IsZero() || maxRelayAttempts < 1 || maxRelayAttempts > 100 ||
		!uuidPattern.MatchString(claim.ID) || !uuidPattern.MatchString(claim.ClaimToken) {
		return ErrInvalidConfiguration
	}
	code, detail, terminal := failureEvidence(kind)
	command, err := repository.pool.Exec(ctx, `
		UPDATE public.worker_job_outbox
		SET status = CASE WHEN $3 OR attempt_count >= $4 THEN 'dead' ELSE 'pending' END,
			claim_token = NULL, claimed_at = NULL, claim_expires_at = NULL,
			next_attempt_at = CASE WHEN $3 OR attempt_count >= $4 THEN next_attempt_at ELSE $5 END,
			last_error_code = $6, last_error_detail = $7, last_error_at = statement_timestamp(),
			updated_at = statement_timestamp()
		WHERE id = $1 AND status = 'claimed' AND claim_token = $2
			AND claim_expires_at > statement_timestamp()`,
		claim.ID, claim.ClaimToken, terminal, maxRelayAttempts,
		nextAttemptAt.UTC(), code, detail)
	if err != nil {
		return ErrUnavailable
	}
	if command.RowsAffected() != 1 {
		return ErrLeaseLost
	}
	return nil
}

// releaseClaim returns work claimed from a stale route snapshot to pending.
// A subsequent reconciliation step refreshes durable routes and excludes it.
func (repository *Repository) releaseClaim(
	ctx context.Context,
	claim Claim,
	now time.Time,
) error {
	if repository == nil || repository.pool == nil || now.IsZero() ||
		!uuidPattern.MatchString(claim.ID) || !uuidPattern.MatchString(claim.ClaimToken) {
		return ErrInvalidConfiguration
	}
	command, err := repository.pool.Exec(ctx, `
		UPDATE public.worker_job_outbox
		SET status = 'pending', claim_token = NULL, claimed_at = NULL,
			claim_expires_at = NULL, next_attempt_at = $3, updated_at = $3
		WHERE id = $1 AND status = 'claimed' AND claim_token = $2
			AND claim_expires_at > statement_timestamp()`,
		claim.ID, claim.ClaimToken, now.UTC())
	if err != nil {
		return ErrUnavailable
	}
	if command.RowsAffected() != 1 {
		return ErrLeaseLost
	}
	return nil
}

// DeleteTerminalBefore performs bounded retention without exposing args.
func (repository *Repository) DeleteTerminalBefore(
	ctx context.Context,
	before time.Time,
	limit int,
) (int64, error) {
	if repository == nil || repository.pool == nil || before.IsZero() || limit < 1 || limit > 1000 {
		return 0, ErrInvalidConfiguration
	}
	tx, err := repository.pool.Begin(ctx)
	if err != nil {
		return 0, ErrUnavailable
	}
	defer func() { _ = tx.Rollback(ctx) }()
	command, err := tx.Exec(ctx, `
		WITH expired AS (
			SELECT id FROM public.worker_job_outbox
			WHERE (status = 'delivered' AND delivered_at < $1)
				OR (status = 'dead' AND updated_at < $1)
			ORDER BY COALESCE(delivered_at, updated_at), id
			FOR UPDATE SKIP LOCKED
			LIMIT $2
		)
		DELETE FROM public.worker_job_outbox AS outbox
		USING expired WHERE outbox.id = expired.id`, before.UTC(), limit)
	if err != nil {
		return 0, ErrUnavailable
	}
	deleted := command.RowsAffected()
	if _, err := tx.Exec(ctx, `
		WITH expired AS (
			SELECT completion.completion_key
			FROM public.worker_job_completion_fences AS completion
			WHERE completion.completed_at < $1
			  AND NOT EXISTS (
			      SELECT 1
			      FROM public.worker_job_outbox AS outbox
			      WHERE outbox.prerequisite_completion_key = completion.completion_key
			  )
			ORDER BY completion.completed_at, completion.completion_key
			FOR UPDATE SKIP LOCKED
			LIMIT $2
		)
		DELETE FROM public.worker_job_completion_fences AS completion
		USING expired
		WHERE completion.completion_key = expired.completion_key`, before.UTC(), limit); err != nil {
		return 0, ErrUnavailable
	}
	if err := tx.Commit(ctx); err != nil {
		return 0, ErrUnavailable
	}
	return deleted, nil
}

type scanner interface {
	Scan(dest ...any) error
}

func scanRow(source scanner) (Row, error) {
	var row Row
	err := source.Scan(
		&row.ID, &row.DedupeKey, &row.JobKind, &row.ContractVersion,
		&row.Args, &row.PayloadHash, &row.Queue, &row.Priority,
		&row.MaxAttempts, &row.ScheduledAt, &row.Status, &row.ClaimToken,
		&row.ClaimedAt, &row.ClaimExpiresAt, &row.AttemptCount,
		&row.FirstAttemptAt, &row.LastAttemptAt, &row.NextAttemptAt,
		&row.LastErrorCode, &row.LastErrorDetail, &row.LastErrorAt,
		&row.RiverJobID, &row.DeliveredAt, &row.CreatedAt, &row.UpdatedAt,
	)
	return row, err
}

func callFault(fault func() error) error {
	if fault == nil {
		return nil
	}
	if err := fault(); err != nil {
		return errInjectedCrash
	}
	return nil
}
