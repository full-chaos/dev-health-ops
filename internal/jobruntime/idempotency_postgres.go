package jobruntime

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// PostgresIdempotency persists the execution state for a logical job, rather
// than relying on River state to answer whether a domain side effect ran.
//
// The lease makes a process death recoverable: a later River attempt can take
// over only after the previous claimant's bounded lease expires. A concurrent
// duplicate sees ClaimAlreadyComplete; River uniqueness remains the primary
// mechanism that prevents such a duplicate from becoming the only retry path.
type PostgresIdempotency struct {
	pool          *pgxpool.Pool
	leaseDuration time.Duration
	now           func() time.Time
}

const defaultIdempotencyLease = 10 * time.Minute

var errIdempotencyUnavailable = errors.New("job idempotency store is unavailable")

// NewPostgresIdempotency constructs the domain-state idempotency adapter. It
// uses the domain pool deliberately; the queue-control role must never be
// granted access to execution state or external-effect evidence.
func NewPostgresIdempotency(pool *pgxpool.Pool) (*PostgresIdempotency, error) {
	if pool == nil {
		return nil, errIdempotencyUnavailable
	}
	return &PostgresIdempotency{
		pool:          pool,
		leaseDuration: defaultIdempotencyLease,
		now:           time.Now,
	}, nil
}

func (store *PostgresIdempotency) Supports(policy string) bool {
	switch policy {
	case "unique_schedule_occurrence", "maintenance_run_checkpoint",
		"billing_notification", "webhook_delivery", "daily_metrics_run",
		"daily_metrics_partition", "daily_metrics_finalize":
		return true
	default:
		return false
	}
}

func (store *PostgresIdempotency) Begin(ctx context.Context, request ClaimRequest) (IdempotencyClaim, error) {
	if store == nil || store.pool == nil || store.now == nil ||
		store.leaseDuration < time.Second || store.leaseDuration > time.Hour ||
		!store.Supports(request.Policy) || !validClaimRequest(request) {
		return nil, errIdempotencyUnavailable
	}

	now := store.now().UTC()
	if now.IsZero() {
		return nil, errIdempotencyUnavailable
	}
	token := uuid.New()
	tx, err := store.pool.Begin(ctx)
	if err != nil {
		return nil, errIdempotencyUnavailable
	}
	defer func() { _ = tx.Rollback(ctx) }()

	var runID uuid.UUID
	err = tx.QueryRow(ctx, `
		INSERT INTO public.worker_job_runs (
			id, job_kind, idempotency_key, org_id, domain_type, domain_id,
			status, claim_token, lease_expires_at, attempt_count, started_at, created_at, updated_at
		) VALUES ($1, $2, $3, NULLIF($4, '')::uuid, $5, $6::uuid,
			'running', $7, $8, 1, $9, $9, $9)
		ON CONFLICT (job_kind, idempotency_key) DO NOTHING
		RETURNING id`,
		uuid.New(), request.Kind, request.IdempotencyKey, organizationID(request.OrganizationID),
		request.Domain.Type, request.Domain.ID, token, now.Add(store.leaseDuration), now,
	).Scan(&runID)
	if err == nil {
		if err := tx.Commit(ctx); err != nil {
			return nil, errIdempotencyUnavailable
		}
		return &postgresClaim{store: store, id: runID, token: token, state: ClaimProceed}, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return nil, errIdempotencyUnavailable
	}

	var status string
	var leaseExpiresAt *time.Time
	if err := tx.QueryRow(ctx, `
		SELECT id, status, lease_expires_at
		FROM public.worker_job_runs
		WHERE job_kind = $1 AND idempotency_key = $2
		FOR UPDATE`, request.Kind, request.IdempotencyKey,
	).Scan(&runID, &status, &leaseExpiresAt); err != nil {
		return nil, errIdempotencyUnavailable
	}

	switch status {
	case "succeeded":
		if err := tx.Commit(ctx); err != nil {
			return nil, errIdempotencyUnavailable
		}
		return &postgresClaim{store: store, id: runID, state: ClaimAlreadyComplete}, nil
	case "terminal":
		if err := tx.Commit(ctx); err != nil {
			return nil, errIdempotencyUnavailable
		}
		return &postgresClaim{store: store, id: runID, state: ClaimTerminal}, nil
	case "retryable", "running":
		if status == "running" && leaseExpiresAt != nil && leaseExpiresAt.After(now) {
			if err := tx.Commit(ctx); err != nil {
				return nil, errIdempotencyUnavailable
			}
			return &postgresClaim{store: store, id: runID, state: ClaimAlreadyComplete}, nil
		}
		command, err := tx.Exec(ctx, `
			UPDATE public.worker_job_runs
			SET status = 'running', claim_token = $1, lease_expires_at = $2,
				attempt_count = attempt_count + 1, started_at = $3, finished_at = NULL,
				result = NULL, error_category = NULL, updated_at = $3
			WHERE id = $4`, token, now.Add(store.leaseDuration), now, runID)
		if err != nil || command.RowsAffected() != 1 {
			return nil, errIdempotencyUnavailable
		}
		if err := tx.Commit(ctx); err != nil {
			return nil, errIdempotencyUnavailable
		}
		return &postgresClaim{store: store, id: runID, token: token, state: ClaimProceed}, nil
	default:
		return nil, errIdempotencyUnavailable
	}
}

type postgresClaim struct {
	store *PostgresIdempotency
	id    uuid.UUID
	token uuid.UUID
	state ClaimState
}

func (claim *postgresClaim) State() ClaimState {
	if claim == nil {
		return ClaimTerminal
	}
	return claim.state
}

func (claim *postgresClaim) Finish(ctx context.Context, completion Completion) error {
	if claim == nil || claim.store == nil || claim.store.pool == nil ||
		claim.state != ClaimProceed || claim.id == uuid.Nil || claim.token == uuid.Nil ||
		claim.store.now == nil {
		return errIdempotencyUnavailable
	}
	status, err := runStatus(completion)
	if err != nil {
		return errIdempotencyUnavailable
	}
	now := claim.store.now().UTC()
	command, err := claim.store.pool.Exec(ctx, `
		UPDATE public.worker_job_runs
		SET status = $1, claim_token = NULL, lease_expires_at = NULL,
			finished_at = $2, result = $3, error_category = $4, updated_at = $2
		WHERE id = $5 AND status = 'running' AND claim_token = $6`,
		status, now, completion.Result, completion.Category, claim.id, claim.token,
	)
	if err != nil || command.RowsAffected() != 1 {
		return errIdempotencyUnavailable
	}
	return nil
}

func validClaimRequest(request ClaimRequest) bool {
	return request.Kind != "" && len(request.Kind) <= 96 && request.IdempotencyKey != "" &&
		len(request.IdempotencyKey) <= 256 && request.Domain.Type != "" &&
		len(request.Domain.Type) <= 64 && request.Domain.ID != "" &&
		len(request.Domain.ID) <= 36 && request.JobID > 0 && request.Attempt > 0
}

func organizationID(value *string) string {
	if value == nil {
		return ""
	}
	return *value
}

func runStatus(completion Completion) (string, error) {
	switch completion.Result {
	case ResultSuccess, ResultDuplicate:
		return "succeeded", nil
	case ResultRetry:
		return "retryable", nil
	case ResultDiscard, ResultCancel:
		return "terminal", nil
	default:
		return "", fmt.Errorf("unsupported completion result")
	}
}
