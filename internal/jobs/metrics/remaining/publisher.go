package remaining

import (
	"context"
	"errors"

	"github.com/full-chaos/dev-health-ops/internal/jobcontract"
	"github.com/full-chaos/dev-health-ops/internal/joboutbox"
	"github.com/full-chaos/dev-health-ops/internal/jobruntime"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

var familyJobKinds = map[string]string{
	"capacity":            jobcontract.KindRemainingCapacity,
	"complexity":          jobcontract.KindRemainingComplexity,
	"dora":                jobcontract.KindRemainingDORA,
	"extra_metrics":       jobcontract.KindRemainingExtraMetrics,
	"membership_backfill": jobcontract.KindRemainingMembership,
	"recommendations":     jobcontract.KindRemainingRecommendations,
	"release_impact":      jobcontract.KindRemainingReleaseImpact,
	"team_metrics":        jobcontract.KindRemainingTeamMetrics,
}

func JobKindForFamily(family string) (string, bool) {
	kind, ok := familyJobKinds[family]
	return kind, ok
}

type PostgresPublisher struct {
	producer *joboutbox.Producer
	registry *jobruntime.Registry
}

func NewPostgresPublisher(
	pool *pgxpool.Pool,
	registry *jobruntime.Registry,
) (*PostgresPublisher, error) {
	producer, err := joboutbox.NewProducer(pool, registry)
	if err != nil || registry == nil {
		return nil, ErrUnavailable
	}
	return &PostgresPublisher{producer: producer, registry: registry}, nil
}

// PublishPartitionTx joins the remaining-metrics domain transaction. Celery
// routes are persisted as deferred outbox rows; independently promoted routes
// become executable without changing the producer or envelope shape.
func (publisher *PostgresPublisher) PublishPartitionTx(
	ctx context.Context,
	tx pgx.Tx,
	run Run,
	partition Partition,
) error {
	if publisher == nil || publisher.producer == nil || publisher.registry == nil ||
		tx == nil || run.ID == "" || run.OrganizationID == "" ||
		partition.ID == "" || partition.RunID != run.ID {
		return ErrUnavailable
	}
	kind, ok := JobKindForFamily(run.Family)
	if !ok {
		return ErrInvalidState
	}
	descriptor, ok := publisher.registry.Descriptor(kind)
	if !ok {
		return ErrUnavailable
	}
	organizationID := run.OrganizationID
	envelope := jobcontract.Envelope{
		ContractVersion: jobcontract.ContractVersionV1,
		OrganizationID:  &organizationID,
		CorrelationID:   "remaining:" + run.ID,
		IdempotencyKey:  "remaining:partition:" + partition.ID,
		Domain: jobcontract.DomainLink{
			Type: "remaining_metric_partition",
			ID:   partition.ID,
		},
		Payload: jobcontract.NewRemainingMetricsPartitionPayload(kind, partition.ID),
	}
	var err error
	if descriptor.Executable() {
		err = publisher.producer.Publish(ctx, tx, kind, envelope)
	} else {
		err = publisher.producer.PublishDeferred(ctx, tx, kind, envelope)
	}
	if err != nil {
		if errors.Is(err, joboutbox.ErrContractRejected) ||
			errors.Is(err, joboutbox.ErrPolicyRejected) {
			return ErrInvalidState
		}
		return ErrUnavailable
	}
	return nil
}
