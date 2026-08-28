package daily

import (
	"context"
	"errors"
	"fmt"

	"github.com/full-chaos/dev-health-ops/internal/jobcontract"
	"github.com/full-chaos/dev-health-ops/internal/joboutbox"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type PostgresPublisher struct {
	producer *joboutbox.Producer
	registry joboutbox.PolicyRegistry
}

func NewPostgresPublisher(
	pool *pgxpool.Pool,
	registry joboutbox.PolicyRegistry,
) (*PostgresPublisher, error) {
	producer, err := joboutbox.NewProducer(pool, registry)
	if err != nil {
		// Keep the producer construction cause reachable, same as the publish
		// paths below -- collapsing it to an undifferentiated "unavailable"
		// is the CHAOS-3903/3905 defect.
		return nil, fmt.Errorf("%w: %w", ErrUnavailable, err)
	}
	return &PostgresPublisher{producer: producer, registry: registry}, nil
}

func (publisher *PostgresPublisher) PublishDispatchTx(
	ctx context.Context,
	tx pgx.Tx,
	run Run,
	prerequisiteCompletionKey string,
) error {
	if publisher == nil || publisher.producer == nil || publisher.registry == nil || tx == nil {
		return ErrUnavailable
	}
	descriptor, ok := publisher.registry.Descriptor(jobcontract.KindDailyMetricsDispatch)
	if !ok {
		return ErrUnavailable
	}
	organizationID := run.OrganizationID
	envelope := jobcontract.Envelope{
		ContractVersion: jobcontract.ContractVersionV1,
		OrganizationID:  &organizationID,
		CorrelationID:   "daily:" + run.ID,
		IdempotencyKey:  "metrics.daily_dispatch:" + run.ID,
		Domain:          jobcontract.DomainLink{Type: "daily_metrics_run", ID: run.ID},
		Payload:         jobcontract.DailyMetricsDispatchPayload{RunID: run.ID},
	}
	var err error
	if descriptor.Executable() {
		if prerequisiteCompletionKey == "" {
			err = publisher.producer.Publish(ctx, tx, jobcontract.KindDailyMetricsDispatch, envelope)
		} else {
			err = publisher.producer.PublishAfter(
				ctx, tx, jobcontract.KindDailyMetricsDispatch, envelope, prerequisiteCompletionKey,
			)
		}
	} else {
		if prerequisiteCompletionKey == "" {
			err = publisher.producer.PublishDeferred(ctx, tx, jobcontract.KindDailyMetricsDispatch, envelope)
		} else {
			err = publisher.producer.PublishDeferredAfter(
				ctx, tx, jobcontract.KindDailyMetricsDispatch, envelope, prerequisiteCompletionKey,
			)
		}
	}
	if err != nil {
		if errors.Is(err, joboutbox.ErrContractRejected) || errors.Is(err, joboutbox.ErrPolicyRejected) {
			// Keep BOTH sentinels reachable, matching the remaining-metrics
			// publisher: callers classify on ErrInvalidState, and the outbox
			// reason underneath names which rule rejected the envelope
			// (CHAOS-3903).
			return fmt.Errorf("%w: %w", ErrInvalidState, err)
		}
		// Any other producer error (e.g. a Postgres write failure) keeps its
		// cause too -- dropping it here was the same defect CHAOS-3903 fixed
		// on the contract/policy branch above, just left on this one
		// (CHAOS-3905).
		return fmt.Errorf("%w: %w", ErrUnavailable, err)
	}
	return nil
}

// PublishRedriveDispatchTx enqueues a NEW metrics.daily_dispatch job for a
// run an operator explicitly named for redrive (CHAOS-4358). It is
// deliberately NOT PublishDispatchTx with a different prerequisite: that
// function's envelope always uses the fixed idempotency key
// "metrics.daily_dispatch:"+run.ID, and the outbox's dedupe table
// (worker_job_outbox, unique on dedupe_key) remembers that key FOREVER --
// "ON CONFLICT (dedupe_key) DO NOTHING" means a second publish under the
// identical key silently no-ops even after the original River job was
// discarded and cleaned up, which is exactly how a stranded run's dispatch
// job disappears for good. nonce makes this publish's dedupe_key distinct
// from the original (and from any other redrive of the same run), so the
// outbox treats it as a genuinely new, executable handoff instead of
// replaying the permanent dedupe record of the first one.
//
// This only ever calls the executable Publish path (never
// PublishDeferred/PublishDeferredAfter): Celery's daily-metrics route
// retired in CHAOS-4026, so the deferred branch PublishDispatchTx keeps for
// the pre-cutover window has no live caller for a REDRIVE, which by
// definition only makes sense against a run already on the Go path.
func (publisher *PostgresPublisher) PublishRedriveDispatchTx(
	ctx context.Context,
	tx pgx.Tx,
	run Run,
	nonce string,
) error {
	if publisher == nil || publisher.producer == nil || publisher.registry == nil || tx == nil || nonce == "" {
		return ErrUnavailable
	}
	descriptor, ok := publisher.registry.Descriptor(jobcontract.KindDailyMetricsDispatch)
	if !ok {
		return ErrUnavailable
	}
	organizationID := run.OrganizationID
	envelope := jobcontract.Envelope{
		ContractVersion: jobcontract.ContractVersionV1,
		OrganizationID:  &organizationID,
		CorrelationID:   "daily:" + run.ID,
		IdempotencyKey:  "metrics.daily_dispatch:redrive:" + run.ID + ":" + nonce,
		Domain:          jobcontract.DomainLink{Type: "daily_metrics_run", ID: run.ID},
		Payload:         jobcontract.DailyMetricsDispatchPayload{RunID: run.ID},
	}
	if !descriptor.Executable() {
		return fmt.Errorf("%w: daily dispatch route is not executable", ErrInvalidState)
	}
	if err := publisher.producer.Publish(ctx, tx, jobcontract.KindDailyMetricsDispatch, envelope); err != nil {
		if errors.Is(err, joboutbox.ErrContractRejected) || errors.Is(err, joboutbox.ErrPolicyRejected) {
			return fmt.Errorf("%w: %w", ErrInvalidState, err)
		}
		return fmt.Errorf("%w: %w", ErrUnavailable, err)
	}
	return nil
}

func (publisher *PostgresPublisher) PublishPartition(
	ctx context.Context,
	run Run,
	partition Partition,
) error {
	if publisher == nil || publisher.producer == nil || partition.RunID != run.ID {
		return ErrInvalidState
	}
	key := "metrics.daily_partition:" + partition.ID
	envelope := jobcontract.Envelope{
		ContractVersion: jobcontract.ContractVersionV1,
		OrganizationID:  &run.OrganizationID,
		CorrelationID:   "daily:" + run.ID,
		IdempotencyKey:  key,
		Domain: jobcontract.DomainLink{
			Type: "daily_metrics_partition",
			ID:   partition.ID,
		},
		Payload: jobcontract.DailyMetricsPartitionPayload{PartitionID: partition.ID},
	}
	if err := publisher.producer.PublishStandalone(
		ctx, jobcontract.KindDailyMetricsPartition, envelope,
	); err != nil {
		return fmt.Errorf("%w: %w", ErrUnavailable, err)
	}
	return nil
}

// PublishRedrivePartitionTx enqueues a NEW metrics.daily_partition job for a
// partition an operator explicitly named for redrive (CHAOS-4358). This is
// the load-bearing half of the redrive: re-enqueuing metrics.daily_dispatch
// alone (PublishRedriveDispatchTx) is NOT sufficient, because
// Dispatcher.Work's own per-partition publish still calls the ordinary
// PublishPartition, whose dedupe_key is "metrics.daily_partition:"+
// partition.ID -- permanent and keyed on the immutable partition id, so a
// FRESH dispatch run reaching an ALREADY-dispatched-and-failed partition
// still silently no-ops at the outbox layer (proven against the real local
// stack: re-publishing dispatch alone only unblocked partitions that had
// never been published before; every previously-dispatched failed partition
// stayed stuck). nonce must be unique per invocation, exactly like
// PublishRedriveDispatchTx's.
func (publisher *PostgresPublisher) PublishRedrivePartitionTx(
	ctx context.Context,
	tx pgx.Tx,
	run Run,
	partition Partition,
	nonce string,
) error {
	if publisher == nil || publisher.producer == nil || tx == nil ||
		partition.RunID != run.ID || nonce == "" {
		return ErrInvalidState
	}
	envelope := jobcontract.Envelope{
		ContractVersion: jobcontract.ContractVersionV1,
		OrganizationID:  &run.OrganizationID,
		CorrelationID:   "daily:" + run.ID,
		IdempotencyKey:  "metrics.daily_partition:redrive:" + partition.ID + ":" + nonce,
		Domain: jobcontract.DomainLink{
			Type: "daily_metrics_partition",
			ID:   partition.ID,
		},
		Payload: jobcontract.DailyMetricsPartitionPayload{PartitionID: partition.ID},
	}
	if err := publisher.producer.Publish(ctx, tx, jobcontract.KindDailyMetricsPartition, envelope); err != nil {
		if errors.Is(err, joboutbox.ErrContractRejected) || errors.Is(err, joboutbox.ErrPolicyRejected) {
			return fmt.Errorf("%w: %w", ErrInvalidState, err)
		}
		return fmt.Errorf("%w: %w", ErrUnavailable, err)
	}
	return nil
}

func (publisher *PostgresPublisher) PublishFinalizeTx(
	ctx context.Context,
	tx pgx.Tx,
	run Run,
) error {
	if publisher == nil || publisher.producer == nil {
		return ErrUnavailable
	}
	key := "metrics.daily_finalize:" + run.ID
	envelope := jobcontract.Envelope{
		ContractVersion: jobcontract.ContractVersionV1,
		OrganizationID:  &run.OrganizationID,
		CorrelationID:   "daily:" + run.ID,
		IdempotencyKey:  key,
		Domain: jobcontract.DomainLink{
			Type: "daily_metrics_run",
			ID:   run.ID,
		},
		Payload: jobcontract.DailyMetricsFinalizePayload{RunID: run.ID},
	}
	if err := publisher.producer.Publish(
		ctx, tx, jobcontract.KindDailyMetricsFinalize, envelope,
	); err != nil {
		return fmt.Errorf("%w: %w", ErrUnavailable, err)
	}
	return nil
}

// PublishRedriveFinalizeTx enqueues a NEW metrics.daily_finalize job for a
// run an operator/sweep explicitly named for redrive (CHAOS-4389). Mirrors
// PublishRedriveDispatchTx/PublishRedrivePartitionTx (CHAOS-4358): the
// ordinary PublishFinalizeTx (called exactly once, from CompletePartition,
// the instant a run's last partition transitions to 'succeeded') always uses
// the fixed idempotency key "metrics.daily_finalize:"+run.ID, and the
// outbox's dedupe table remembers that key FOREVER ("ON CONFLICT
// (dedupe_key) DO NOTHING") -- so once River discards that one job (the
// compatibility bridge's Finalize call failing repeatedly, e.g. the
// CHAOS-4361 memory-bound class, or any other retryable error exhausting
// River's attempt budget before CompleteFinalize ever runs), nothing else
// ever re-enqueues it and the run is stranded status='running' forever with
// 100% of its partitions succeeded -- the CHAOS-4389 gap. nonce must be
// unique per invocation (a fresh UUID is the expected caller pattern),
// matching the sibling redrive publishers' contract.
func (publisher *PostgresPublisher) PublishRedriveFinalizeTx(
	ctx context.Context,
	tx pgx.Tx,
	run Run,
	nonce string,
) error {
	if publisher == nil || publisher.producer == nil || tx == nil || nonce == "" {
		return ErrUnavailable
	}
	descriptor, ok := publisher.registry.Descriptor(jobcontract.KindDailyMetricsFinalize)
	if !ok {
		return ErrUnavailable
	}
	organizationID := run.OrganizationID
	envelope := jobcontract.Envelope{
		ContractVersion: jobcontract.ContractVersionV1,
		OrganizationID:  &organizationID,
		CorrelationID:   "daily:" + run.ID,
		IdempotencyKey:  "metrics.daily_finalize:redrive:" + run.ID + ":" + nonce,
		Domain: jobcontract.DomainLink{
			Type: "daily_metrics_run",
			ID:   run.ID,
		},
		Payload: jobcontract.DailyMetricsFinalizePayload{RunID: run.ID},
	}
	if !descriptor.Executable() {
		return fmt.Errorf("%w: daily finalize route is not executable", ErrInvalidState)
	}
	if err := publisher.producer.Publish(ctx, tx, jobcontract.KindDailyMetricsFinalize, envelope); err != nil {
		if errors.Is(err, joboutbox.ErrContractRejected) || errors.Is(err, joboutbox.ErrPolicyRejected) {
			return fmt.Errorf("%w: %w", ErrInvalidState, err)
		}
		return fmt.Errorf("%w: %w", ErrUnavailable, err)
	}
	return nil
}

var _ Publisher = (*PostgresPublisher)(nil)
var _ RunPublisher = (*PostgresPublisher)(nil)
