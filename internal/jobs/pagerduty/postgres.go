package pagerduty

import (
	"context"
	"errors"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

const receiptKind = "webhook.pagerduty.process"

// PostgresReceiptStore makes the stream ACK boundary survive process death.
// It uses worker_job_runs, whose unique key is independent of a Redis entry
// ID and therefore stable when Valkey reclaims an unacknowledged delivery.
type PostgresReceiptStore struct {
	pool  *pgxpool.Pool
	lease time.Duration
	now   func() time.Time
}

func NewPostgresReceiptStore(pool *pgxpool.Pool) (*PostgresReceiptStore, error) {
	if pool == nil {
		return nil, errUnavailable
	}
	return &PostgresReceiptStore{pool: pool, lease: 10 * time.Minute, now: time.Now}, nil
}

func (store *PostgresReceiptStore) Begin(ctx context.Context, receiptID string) (ReceiptClaim, error) {
	if store == nil || store.pool == nil || store.now == nil || receiptID == "" {
		return ReceiptClaim{}, errUnavailable
	}
	now := store.now().UTC()
	token := uuid.New()
	domainID := uuid.NewSHA1(uuid.NameSpaceURL, []byte(receiptID))
	tx, err := store.pool.Begin(ctx)
	if err != nil {
		return ReceiptClaim{}, errUnavailable
	}
	defer func() { _ = tx.Rollback(ctx) }()
	var id uuid.UUID
	err = tx.QueryRow(ctx, `INSERT INTO public.worker_job_runs (id, job_kind, idempotency_key, domain_type, domain_id, status, claim_token, lease_expires_at, attempt_count, started_at, created_at, updated_at) VALUES ($1,$2,$3,'webhook_delivery',$4,'running',$5,$6,1,$7,$7,$7) ON CONFLICT (job_kind,idempotency_key) DO NOTHING RETURNING id`, uuid.New(), receiptKind, receiptID, domainID, token, now.Add(store.lease), now).Scan(&id)
	if err == nil {
		if tx.Commit(ctx) != nil {
			return ReceiptClaim{}, errUnavailable
		}
		return ReceiptClaim{ReceiptID: receiptID, Token: token.String(), Proceed: true}, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return ReceiptClaim{}, errUnavailable
	}
	var status string
	var expires *time.Time
	if err := tx.QueryRow(ctx, `SELECT id,status,lease_expires_at FROM public.worker_job_runs WHERE job_kind=$1 AND idempotency_key=$2 FOR UPDATE`, receiptKind, receiptID).Scan(&id, &status, &expires); err != nil {
		return ReceiptClaim{}, errUnavailable
	}
	if status == "succeeded" || (status == "running" && expires != nil && expires.After(now)) {
		if tx.Commit(ctx) != nil {
			return ReceiptClaim{}, errUnavailable
		}
		return ReceiptClaim{ReceiptID: receiptID}, nil
	}
	command, err := tx.Exec(ctx, `UPDATE public.worker_job_runs SET status='running',claim_token=$1,lease_expires_at=$2,attempt_count=attempt_count+1,started_at=$3,finished_at=NULL,result=NULL,error_category=NULL,updated_at=$3 WHERE id=$4`, token, now.Add(store.lease), now, id)
	if err != nil || command.RowsAffected() != 1 || tx.Commit(ctx) != nil {
		return ReceiptClaim{}, errUnavailable
	}
	return ReceiptClaim{ReceiptID: receiptID, Token: token.String(), Proceed: true}, nil
}

func (store *PostgresReceiptStore) Complete(ctx context.Context, claim ReceiptClaim) error {
	if store == nil || store.pool == nil || store.now == nil || claim.ReceiptID == "" || claim.Token == "" {
		return errUnavailable
	}
	now := store.now().UTC()
	command, err := store.pool.Exec(ctx, `UPDATE public.worker_job_runs SET status='succeeded',claim_token=NULL,lease_expires_at=NULL,finished_at=$1,result='success',error_category='none',updated_at=$1 WHERE job_kind=$2 AND idempotency_key=$3 AND status='running' AND claim_token=$4::uuid`, now, receiptKind, claim.ReceiptID, claim.Token)
	if err != nil || command.RowsAffected() != 1 {
		return errUnavailable
	}
	return nil
}
