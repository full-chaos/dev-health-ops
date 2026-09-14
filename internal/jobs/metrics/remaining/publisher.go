package remaining

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/full-chaos/dev-health-ops/internal/jobcontract"
	"github.com/full-chaos/dev-health-ops/internal/joboutbox"
	"github.com/full-chaos/dev-health-ops/internal/jobruntime"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// familyJobKinds is derived from families.json (the single source for the
// family->kind mapping, CHAOS-5007) exactly once, instead of being a
// separately hand-maintained map: a hardcoded copy is what let
// "work_item_attribution" go missing here while every other artifact
// (families.json, registry.json, migration-state.json, the Python mirror)
// still had it, turning one silently-dropped map entry into a full "daily"
// family composition crash (CHAOS-4993, CHAOS-3092 PR-B). Deriving it
// removes the class of bug rather than just testing for it.
var (
	familyJobKindsOnce sync.Once
	familyJobKinds     map[string]string
)

func loadFamilyJobKinds() map[string]string {
	familyJobKindsOnce.Do(func() {
		inventory, err := Load()
		if err != nil {
			// families.json is //go:embed'ed and Validate()-checked by Load()
			// itself; a failure here means the embedded artifact is corrupt,
			// which is a build-time defect, not a runtime condition any
			// caller of JobKindForFamily can meaningfully recover from.
			panic(fmt.Sprintf("remaining: families.json failed to load: %v", err))
		}
		kinds := make(map[string]string, len(inventory.Families))
		for _, family := range inventory.Families {
			kinds[family.Name] = family.RouteKey
		}
		familyJobKinds = kinds
	})
	return familyJobKinds
}

func JobKindForFamily(family string) (string, bool) {
	kind, ok := loadFamilyJobKinds()[family]
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
	if err != nil {
		// Keep the producer construction cause reachable, same as the publish
		// path below -- a nil pool/registry error from joboutbox otherwise
		// vanishes into an undifferentiated "unavailable" (CHAOS-3903/3905).
		return nil, fmt.Errorf("%w: %w", ErrUnavailable, err)
	}
	if registry == nil {
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
	prerequisiteCompletionKey string,
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
		if prerequisiteCompletionKey == "" {
			err = publisher.producer.Publish(ctx, tx, kind, envelope)
		} else {
			err = publisher.producer.PublishAfter(
				ctx, tx, kind, envelope, prerequisiteCompletionKey,
			)
		}
	} else {
		if prerequisiteCompletionKey == "" {
			err = publisher.producer.PublishDeferred(ctx, tx, kind, envelope)
		} else {
			err = publisher.producer.PublishDeferredAfter(
				ctx, tx, kind, envelope, prerequisiteCompletionKey,
			)
		}
	}
	// joboutbox.IsPublished, not `err != nil`: an already-terminal outbox row
	// means the envelope is durably staged and there was nothing to insert,
	// which for this publisher has always been -- and remains -- a success.
	// This publisher owns no repair path and holds no logger; the caller that
	// ACTS on ErrDeliveryAlreadyTerminal is the provider-unit dispatch path.
	if !joboutbox.IsPublished(err) {
		if errors.Is(err, joboutbox.ErrContractRejected) ||
			errors.Is(err, joboutbox.ErrPolicyRejected) {
			// Keep BOTH sentinels reachable. Callers switch on ErrInvalidState
			// to classify the failure as permanent for this input, and the
			// outbox reason underneath is what names which rule rejected the
			// envelope -- replacing it left "remaining metrics durable state is
			// invalid" as the only evidence of a field-level validation failure
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

// PublishRedrivePartitionTx enqueues a NEW job for a partition an operator
// (or `metrics remaining redrive`) explicitly named for redrive -- the
// remaining-metrics analog of daily's PublishRedrivePartitionTx, and for the
// identical reason: PublishPartitionTx's idempotency key is
// "remaining:partition:"+partition.ID, permanent and immutable, so once
// River discards the one job ever published under that key nothing else
// re-enqueues it -- the partition can sit 'failed' forever even though its
// parent run is still status='running' and ClaimPartition would happily
// reclaim it if a job ever named it again. nonce must be unique per redrive
// invocation (a fresh UUID is the expected caller pattern), so a repeated
// redrive of the same still-failed partition is never silently deduped
// against an earlier attempt.
//
// Branches on descriptor.Executable() and carries both routes, exactly like
// PublishPartitionTx, even though only the executable route is ever reached
// in practice here: a partition can only have reached 'failed' by first
// being claimed and computed through the native (River-only) executor path,
// which requires an already-executable route, so a redrive of it can never
// legitimately need the deferred one. The kind is still resolved from
// run.Family at run time, not a compile-time constant, so it cannot be
// checked per kind the way a fixed-kind call site can -- carrying both
// routes here is what keeps this call site checkable at all (see
// TestEveryOutboxPublishSiteAgreesWithTheCheckedInRoute).
func (publisher *PostgresPublisher) PublishRedrivePartitionTx(
	ctx context.Context,
	tx pgx.Tx,
	run Run,
	partition Partition,
	nonce string,
) error {
	if publisher == nil || publisher.producer == nil || publisher.registry == nil ||
		tx == nil || run.ID == "" || run.OrganizationID == "" ||
		partition.ID == "" || partition.RunID != run.ID || nonce == "" {
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
		IdempotencyKey:  "remaining:partition:redrive:" + partition.ID + ":" + nonce,
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
	if !joboutbox.IsPublished(err) {
		if errors.Is(err, joboutbox.ErrContractRejected) || errors.Is(err, joboutbox.ErrPolicyRejected) {
			return fmt.Errorf("%w: %w", ErrInvalidState, err)
		}
		return fmt.Errorf("%w: %w", ErrUnavailable, err)
	}
	return nil
}
