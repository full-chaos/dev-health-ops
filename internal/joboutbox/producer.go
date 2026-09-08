package joboutbox

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/jobcontract"
	"github.com/full-chaos/dev-health-ops/internal/jobruntime"
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

// NewTransactionProducer constructs the package-owned transaction-only seam.
// It cannot publish standalone because it deliberately owns no connection
// pool; callers must supply the transaction that also owns their domain state.
func NewTransactionProducer(registry PolicyRegistry) (*Producer, error) {
	if registry == nil {
		return nil, ErrInvalidConfiguration
	}
	return &Producer{registry: registry, now: time.Now}, nil
}

func (producer *Producer) Publish(
	ctx context.Context,
	tx pgx.Tx,
	kind string,
	envelope jobcontract.Envelope,
) error {
	return producer.publish(ctx, tx, kind, envelope, false, "")
}

// PublishAfter persists an executable handoff that becomes relay-eligible only
// after the named durable completion fence commits.
func (producer *Producer) PublishAfter(
	ctx context.Context,
	tx pgx.Tx,
	kind string,
	envelope jobcontract.Envelope,
	prerequisiteCompletionKey string,
) error {
	return producer.publish(ctx, tx, kind, envelope, false, prerequisiteCompletionKey)
}

// PublishDeferred persists a reviewed Celery-routed handoff without making it
// executable. The relay excludes these rows until the checked-in route is
// promoted, preserving the domain transaction while Celery remains primary.
func (producer *Producer) PublishDeferred(
	ctx context.Context,
	tx pgx.Tx,
	kind string,
	envelope jobcontract.Envelope,
) error {
	return producer.publish(ctx, tx, kind, envelope, true, "")
}

// PublishDeferredAfter is the checked-in-Celery equivalent of PublishAfter.
// The row remains ineligible both while its route is deferred and while its
// durable predecessor is incomplete.
func (producer *Producer) PublishDeferredAfter(
	ctx context.Context,
	tx pgx.Tx,
	kind string,
	envelope jobcontract.Envelope,
	prerequisiteCompletionKey string,
) error {
	return producer.publish(ctx, tx, kind, envelope, true, prerequisiteCompletionKey)
}

func (producer *Producer) publish(
	ctx context.Context,
	tx pgx.Tx,
	kind string,
	envelope jobcontract.Envelope,
	deferred bool,
	prerequisiteCompletionKey string,
) error {
	if producer == nil || producer.registry == nil || producer.now == nil || tx == nil {
		return ErrInvalidConfiguration
	}
	if prerequisiteCompletionKey != "" && !validCompletionKey(prerequisiteCompletionKey) {
		return fmt.Errorf("%w: prerequisite_completion_key_invalid", ErrContractRejected)
	}
	descriptor, ok := producer.registry.Descriptor(kind)
	if !ok {
		return fmt.Errorf("%w: kind_not_registered", ErrContractRejected)
	}
	if !descriptorAllowsPublish(descriptor, deferred) {
		return fmt.Errorf("%w: publish_not_permitted_for_route", ErrPolicyRejected)
	}
	if envelope.ContractVersion != descriptor.CurrentVersion {
		return fmt.Errorf("%w: contract_version_mismatch", ErrContractRejected)
	}
	encoded, err := jobcontract.MarshalCanonical(envelope)
	if err != nil {
		// jobcontract validation messages name the offending FIELD and its rule
		// ("organization_id must be a lowercase UUID"), never the value, so
		// they carry no tenant data or credential material and are safe to
		// propagate. Collapsing them to a bare sentinel is what made a
		// non-conformant organization row cost four rebuild-and-read cycles to
		// find (CHAOS-3903).
		return fmt.Errorf("%w: envelope_marshal_rejected: %w", ErrContractRejected, err)
	}
	decoded, err := jobcontract.Decode(kind, encoded)
	if err != nil {
		return fmt.Errorf("%w: envelope_decode_rejected: %w", ErrContractRejected, err)
	}
	if decoded.IdempotencyKey != envelope.IdempotencyKey {
		return fmt.Errorf("%w: idempotency_key_not_round_trippable", ErrContractRejected)
	}
	hash := sha256.Sum256(encoded)
	payloadHash := "sha256:" + hex.EncodeToString(hash[:])
	now := producer.now().UTC()
	id := uuid.NewSHA1(producerNamespace, []byte(envelope.IdempotencyKey))
	command, err := tx.Exec(ctx, `
INSERT INTO public.worker_job_outbox (
    id, dedupe_key, job_kind, contract_version, args, payload_hash,
    queue, priority, max_attempts, scheduled_at, status, attempt_count,
    next_attempt_at, prerequisite_completion_key, created_at, updated_at
) VALUES (
    $1, $2, $3, $4, $5::json, $6, $7, $8, $9, $10,
    'pending', 0, $10, NULLIF($11, ''), $10, $10
)
ON CONFLICT (dedupe_key) DO NOTHING`,
		id, envelope.IdempotencyKey, kind, envelope.ContractVersion, string(encoded),
		payloadHash, descriptor.Queue, descriptor.Priority, descriptor.MaxAttempts, now,
		prerequisiteCompletionKey,
	)
	if err != nil {
		return ErrUnavailable
	}
	if command.RowsAffected() == 1 {
		return nil
	}
	var existingKind, existingHash, existingPrerequisite, existingStatus string
	var existingVersion int
	// `status` is read alongside the agreement fields, in the same round trip.
	// Before this, the conflict branch compared kind, contract_version,
	// payload_hash and prerequisite_completion_key, found them identical, and
	// returned nil -- WITHOUT ever looking at status. A 'delivered' row is
	// terminal for the relay (repository.go's claim SQL takes only 'pending'
	// and expired 'claimed'), so every such publish was a no-op reported as a
	// success. See ErrDeliveryAlreadyTerminal.
	//
	// `river_job_id` is deliberately NOT read here, though it would make the
	// error more useful. This SELECT is on the hot publish path of EVERY job
	// kind, so every column it names becomes a column every fixture and every
	// deployment must already have -- and twenty in-tree integration fixtures
	// build worker_job_outbox without it, which turned an idempotent
	// republish into a 42501/42703 for unrelated packages. The status alone
	// answers the question this error exists to ask, and the dead job is one
	// query away from the dedupe key the caller already holds.
	err = tx.QueryRow(ctx, `
SELECT job_kind, contract_version, payload_hash,
       COALESCE(prerequisite_completion_key, ''), status
FROM public.worker_job_outbox
WHERE dedupe_key = $1`, envelope.IdempotencyKey).
		Scan(&existingKind, &existingVersion, &existingHash, &existingPrerequisite,
			&existingStatus)
	if errors.Is(err, pgx.ErrNoRows) || err != nil {
		return ErrUnavailable
	}
	if existingKind != kind || existingVersion != envelope.ContractVersion ||
		existingHash != payloadHash || existingPrerequisite != prerequisiteCompletionKey {
		return fmt.Errorf("%w: dedupe_key_conflicts_with_existing_row", ErrContractRejected)
	}
	// Checked AFTER the agreement comparison, deliberately: a row that
	// DISAGREES is a contract fault and must keep reporting itself as one,
	// whatever its status happens to be.
	if terminalOutboxStatus(existingStatus) {
		// The status is the fact a caller needs, and it is one of four fixed
		// literals -- no tenant data, no credential material. The dedupe key is
		// deliberately NOT included: it embeds the domain id, and this error
		// text reaches operator-facing logs.
		return fmt.Errorf("%w: status=%s", ErrDeliveryAlreadyTerminal, existingStatus)
	}
	return nil
}

// terminalOutboxStatus names the two statuses from which the relay will never
// mint another delivery. 'pending' and 'claimed' are both still reachable by
// Repository's claim SQL, so a publish that lands on one of those genuinely
// has a delivery coming and reports plain success.
//
// Listed positively rather than as an exclusion: a status added to
// ck_worker_job_outbox_status later is then treated as NON-terminal by default,
// which is the direction that fails quiet rather than the direction that turns
// a healthy publish into a warning nobody can act on.
func terminalOutboxStatus(status string) bool {
	return status == "delivered" || status == "dead"
}

// IsPublished reports whether a Publish* call left the envelope durably in the
// outbox, whether or not THIS call was the one that put a delivery in front of
// it.
//
// It exists so the distinction ErrDeliveryAlreadyTerminal draws is a decision
// every publisher makes EXPLICITLY, rather than one they inherit. Most
// publishers here have no logger and no repair path of their own -- the
// daily-metrics, work-graph, remaining-metrics and fixed-schedule publishers
// are DB-only seams by design -- and for them "the row is already delivered"
// has always been a successful outcome and still is; without this helper they
// would each have to grow an errors.Is branch that swallows the sentinel, and
// a swallow is exactly what makes a new signal invisible again.
//
// The one caller that does NOT use this is
// internal/syncdispatchruntime's provider-unit publish, which acts on the
// sentinel: joboutbox.StrandRepair, not another republish, owns a unit whose
// delivery is already terminal.
//
// Note what this does NOT collapse: a contract or policy rejection, or an
// unavailable database, all still report false. Only the terminal-delivery
// observation is treated as published.
func IsPublished(err error) bool {
	return err == nil || errors.Is(err, ErrDeliveryAlreadyTerminal)
}

func descriptorAllowsPublish(descriptor jobruntime.Descriptor, deferred bool) bool {
	if !deferred {
		return descriptor.Executable()
	}
	return descriptor.MigrationState == "go_implemented" &&
		descriptor.Route == "celery" && descriptor.RollbackRoute == "celery"
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
	// The sentinel is carried out to the caller, but it must not skip the
	// commit: Publish's own writes are already part of this transaction on
	// every other path, and an agreeing terminal row means there was simply
	// nothing to insert -- rolling back here would additionally discard
	// anything the caller put in the same transaction.
	publishErr := producer.Publish(ctx, tx, kind, envelope)
	if !IsPublished(publishErr) {
		return publishErr
	}
	if err := tx.Commit(ctx); err != nil {
		return ErrUnavailable
	}
	return publishErr
}
