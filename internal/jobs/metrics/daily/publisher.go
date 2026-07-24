package daily

import (
	"context"
	"errors"

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
		return nil, ErrUnavailable
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
			return ErrInvalidState
		}
		return ErrUnavailable
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
		return ErrUnavailable
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
		return ErrUnavailable
	}
	return nil
}

var _ Publisher = (*PostgresPublisher)(nil)
var _ RunPublisher = (*PostgresPublisher)(nil)
