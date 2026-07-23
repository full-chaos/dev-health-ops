package joboutbox

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/jobcontract"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

var producerNamespace = uuid.MustParse("5c945f3d-1ab2-5cba-9b3d-85562f024edc")

// Producer inserts immutable job envelopes into the generic outbox. Callers
// supply the transaction so a domain transition and its child handoff cannot
// commit independently.
type Producer struct {
	pool     *pgxpool.Pool
	registry PolicyRegistry
	now      func() time.Time
}

func NewProducer(pool *pgxpool.Pool, registry PolicyRegistry) (*Producer, error) {
	if pool == nil || registry == nil {
		return nil, ErrInvalidConfiguration
	}
	return &Producer{pool: pool, registry: registry, now: time.Now}, nil
}

func (producer *Producer) Publish(
	ctx context.Context,
	tx pgx.Tx,
	kind string,
	envelope jobcontract.Envelope,
) error {
	if producer == nil || producer.pool == nil || producer.registry == nil || producer.now == nil || tx == nil {
		return ErrInvalidConfiguration
	}
	descriptor, ok := producer.registry.Descriptor(kind)
	if !ok {
		return ErrContractRejected
	}
	if !descriptor.Executable() {
		return ErrPolicyRejected
	}
	if envelope.ContractVersion != descriptor.CurrentVersion {
		return ErrContractRejected
	}
	encoded, err := jobcontract.MarshalCanonical(envelope)
	if err != nil {
		return ErrContractRejected
	}
	decoded, err := jobcontract.Decode(kind, encoded)
	if err != nil || decoded.IdempotencyKey != envelope.IdempotencyKey {
		return ErrContractRejected
	}
	hash := sha256.Sum256(encoded)
	payloadHash := "sha256:" + hex.EncodeToString(hash[:])
	now := producer.now().UTC()
	id := uuid.NewSHA1(producerNamespace, []byte(envelope.IdempotencyKey))
	command, err := tx.Exec(ctx, `
INSERT INTO public.worker_job_outbox (
    id, dedupe_key, job_kind, contract_version, args, payload_hash,
    queue, priority, max_attempts, scheduled_at, status, attempt_count,
    next_attempt_at, created_at, updated_at
) VALUES (
    $1, $2, $3, $4, $5::json, $6, $7, $8, $9, $10,
    'pending', 0, $10, $10, $10
)
ON CONFLICT (dedupe_key) DO NOTHING`,
		id, envelope.IdempotencyKey, kind, envelope.ContractVersion, string(encoded),
		payloadHash, descriptor.Queue, descriptor.Priority, descriptor.MaxAttempts, now,
	)
	if err != nil {
		return ErrUnavailable
	}
	if command.RowsAffected() == 1 {
		return nil
	}
	var existingKind, existingHash string
	var existingVersion int
	err = tx.QueryRow(ctx, `
SELECT job_kind, contract_version, payload_hash
FROM public.worker_job_outbox
WHERE dedupe_key = $1`, envelope.IdempotencyKey).
		Scan(&existingKind, &existingVersion, &existingHash)
	if errors.Is(err, pgx.ErrNoRows) || err != nil {
		return ErrUnavailable
	}
	if existingKind != kind || existingVersion != envelope.ContractVersion || existingHash != payloadHash {
		return ErrContractRejected
	}
	return nil
}

func (producer *Producer) PublishStandalone(
	ctx context.Context,
	kind string,
	envelope jobcontract.Envelope,
) error {
	if producer == nil || producer.pool == nil {
		return ErrInvalidConfiguration
	}
	tx, err := producer.pool.Begin(ctx)
	if err != nil {
		return ErrUnavailable
	}
	defer func() {
		rollbackCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		defer cancel()
		_ = tx.Rollback(rollbackCtx)
	}()
	if err := producer.Publish(ctx, tx, kind, envelope); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return ErrUnavailable
	}
	return nil
}
