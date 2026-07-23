package daily

import (
	"context"

	"github.com/full-chaos/dev-health-ops/internal/jobcontract"
	"github.com/full-chaos/dev-health-ops/internal/joboutbox"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type PostgresPublisher struct {
	producer *joboutbox.Producer
}

func NewPostgresPublisher(
	pool *pgxpool.Pool,
	registry joboutbox.PolicyRegistry,
) (*PostgresPublisher, error) {
	producer, err := joboutbox.NewProducer(pool, registry)
	if err != nil {
		return nil, ErrUnavailable
	}
	return &PostgresPublisher{producer: producer}, nil
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
