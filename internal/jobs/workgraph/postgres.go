package workgraph

import (
	"context"
	"encoding/json"
	"errors"
	"regexp"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/joboutbox"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

const defaultLease = 10 * time.Minute

var uuidPattern = regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$`)

func validUUID(value string) bool { return uuidPattern.MatchString(value) }

// PostgresStore keeps request and ledger transitions in one transaction. The
// ledger has one row per request; an expired reclaim replaces its fencing token
// atomically rather than leaving a stale token beside a new request owner.
type PostgresStore struct {
	pool  *pgxpool.Pool
	lease time.Duration
	now   func() time.Time
}

func NewPostgresStore(pool *pgxpool.Pool) (*PostgresStore, error) {
	if pool == nil {
		return nil, ErrUnavailable
	}
	return &PostgresStore{pool: pool, lease: defaultLease, now: time.Now}, nil
}

func (store *PostgresStore) Claim(ctx context.Context, requestID string, kind Kind) (*Claim, error) {
	if !store.valid() || !validUUID(requestID) || !kind.Valid() {
		return nil, ErrUnavailable
	}
	tx, err := store.pool.Begin(ctx)
	if err != nil {
		return nil, ErrUnavailable
	}
	defer rollback(ctx, tx)
	now, token := store.now().UTC(), uuid.NewString()
	var request Request
	var scope []byte
	err = tx.QueryRow(ctx, `
UPDATE public.work_graph_execution_requests
SET state = 'running', claim_token = $1::uuid, lease_expires_at = $2,
    attempt_count = attempt_count + 1, updated_at = $3
WHERE id = $4::uuid AND kind = $5
  AND (state = 'pending' OR (state = 'running' AND lease_expires_at <= $3))
RETURNING id::text, org_id::text, kind, scope::text, COALESCE(model_ref, ''),
          COALESCE(prompt_ref, ''), llm_concurrency, spend_limit_microunits,
          correlation_id, idempotency_key`, token, now.Add(store.lease), now, requestID, string(kind)).Scan(
		&request.ID, &request.OrganizationID, &request.Kind, &scope, &request.ModelRef,
		&request.PromptRef, &request.LLMConcurrency, &request.SpendLimitMicrounits,
		&request.CorrelationID, &request.IdempotencyKey,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		var state string
		var leaseExpiresAt *time.Time
		rowErr := tx.QueryRow(ctx, `SELECT state, lease_expires_at FROM public.work_graph_execution_requests WHERE id = $1::uuid AND kind = $2`, requestID, string(kind)).Scan(&state, &leaseExpiresAt)
		if rowErr == nil && (state == "succeeded" || (state == "running" && leaseExpiresAt != nil && leaseExpiresAt.After(now))) {
			return nil, nil
		}
		return nil, ErrInvalidState
	}
	if err != nil {
		return nil, ErrUnavailable
	}
	request.Scope = append([]byte(nil), scope...)
	if !validRequest(request) {
		return nil, ErrInvalidState
	}
	command, err := tx.Exec(ctx, `
INSERT INTO public.work_graph_execution_ledger (
    request_id, claim_token, state, attempt_count, last_attempt_at
) VALUES ($1::uuid, $2::uuid, 'executing', 1, $3)
ON CONFLICT (request_id) DO UPDATE SET
    claim_token = EXCLUDED.claim_token,
    state = 'executing',
    attempt_count = public.work_graph_execution_ledger.attempt_count + 1,
    output_evidence = NULL,
    failure_detail = NULL,
    completed_at = NULL,
    last_attempt_at = EXCLUDED.last_attempt_at
	WHERE public.work_graph_execution_ledger.state IN ('executing', 'repaired')`, requestID, token, now)
	if err != nil {
		return nil, ErrUnavailable
	}
	if command.RowsAffected() != 1 {
		return nil, ErrInvalidState
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, ErrUnavailable
	}
	return &Claim{Request: request, Token: token, LeaseDuration: store.lease}, nil
}

func (store *PostgresStore) Renew(ctx context.Context, claim Claim) error {
	if !store.validClaim(claim) {
		return ErrUnavailable
	}
	now := store.now().UTC()
	command, err := store.pool.Exec(ctx, `
UPDATE public.work_graph_execution_requests
SET lease_expires_at = $1, updated_at = $2
WHERE id = $3::uuid AND kind = $4 AND state = 'running'
  AND claim_token = $5::uuid AND lease_expires_at > $2`, now.Add(store.lease), now,
		claim.Request.ID, string(claim.Request.Kind), claim.Token)
	if err != nil {
		return ErrUnavailable
	}
	if command.RowsAffected() != 1 {
		return ErrLeaseLost
	}
	return nil
}

func (store *PostgresStore) Complete(ctx context.Context, claim Claim, evidence []byte) error {
	if !store.validClaim(claim) || !validEvidence(evidence) {
		return ErrInvalidState
	}
	return store.transition(ctx, claim, "succeeded", evidence, "")
}

func (store *PostgresStore) Fail(ctx context.Context, claim Claim, detail string) error {
	return store.transition(ctx, claim, "failed", nil, detail)
}

func (store *PostgresStore) Ambiguous(ctx context.Context, claim Claim, detail string) error {
	return store.transition(ctx, claim, "ambiguous", nil, detail)
}

func (store *PostgresStore) transition(ctx context.Context, claim Claim, state string, evidence []byte, detail string) error {
	if !store.validClaim(claim) || (state != "succeeded" && state != "failed" && state != "ambiguous") ||
		(state == "succeeded" && !validEvidence(evidence)) ||
		(state != "succeeded" && (len(detail) == 0 || len(detail) > 1024)) {
		return ErrInvalidState
	}
	tx, err := store.pool.Begin(ctx)
	if err != nil {
		return ErrUnavailable
	}
	defer rollback(ctx, tx)
	now := store.now().UTC()
	command, err := tx.Exec(ctx, `
UPDATE public.work_graph_execution_requests
SET state = $1, claim_token = NULL, lease_expires_at = NULL, updated_at = $2
WHERE id = $3::uuid AND kind = $4 AND state = 'running'
  AND claim_token = $5::uuid AND lease_expires_at > $2`, state, now, claim.Request.ID,
		string(claim.Request.Kind), claim.Token)
	if err != nil {
		return ErrUnavailable
	}
	if command.RowsAffected() != 1 {
		return ErrLeaseLost
	}
	command, err = tx.Exec(ctx, `
UPDATE public.work_graph_execution_ledger
SET state = $1, output_evidence = CASE WHEN $1 = 'succeeded' THEN $2::jsonb ELSE NULL END,
    failure_detail = CASE WHEN $1 = 'succeeded' THEN NULL ELSE $3 END,
    completed_at = CASE WHEN $1 = 'succeeded' THEN $4::timestamptz ELSE NULL END
WHERE request_id = $5::uuid AND state = 'executing' AND claim_token = $6::uuid`,
		state, nullableJSON(evidence), detail, now, claim.Request.ID, claim.Token)
	if err != nil {
		return ErrUnavailable
	}
	if command.RowsAffected() != 1 {
		return ErrLeaseLost
	}
	if state == "succeeded" {
		completionKey, keyErr := joboutbox.CompletionKey(
			"work_graph_execution_request", claim.Request.ID,
		)
		if keyErr != nil {
			return ErrInvalidState
		}
		if err := joboutbox.MarkCompletionTx(ctx, tx, completionKey); err != nil {
			return ErrUnavailable
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return ErrUnavailable
	}
	return nil
}

func (store *PostgresStore) valid() bool {
	return store != nil && store.pool != nil && store.now != nil && store.lease > 0
}
func (store *PostgresStore) validClaim(claim Claim) bool {
	return store.valid() && validRequest(claim.Request) && validUUID(claim.Token)
}
func validEvidence(value []byte) bool {
	return len(value) > 1 && len(value) <= 4096 && json.Valid(value)
}
func nullableJSON(value []byte) any {
	if len(value) == 0 {
		return nil
	}
	return string(value)
}
func rollback(ctx context.Context, tx pgx.Tx) {
	rollbackCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()
	_ = tx.Rollback(rollbackCtx)
}
