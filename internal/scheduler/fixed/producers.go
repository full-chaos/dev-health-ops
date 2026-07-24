package fixed

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/jobcontract"
	"github.com/full-chaos/dev-health-ops/internal/jobs/metrics/remaining"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

// occurrenceDomainNamespace derives the synthetic domain identity for job
// kinds whose domain link has no backing table. `schedule_occurrence` and
// `maintenance_run` are declared in the job contract but no table exists for
// either: the runtime's domain guard explicitly exempts both. Deriving a
// version-5 UUID from the occurrence key keeps the contract's UUID shape while
// preserving the property that matters, which is that one due time always maps
// to one domain identity.
var occurrenceDomainNamespace = uuid.MustParse("6f2f2ba4-2c2f-5f8a-9a1e-9a2c7b6d4e11")

// OccurrenceDomainID is the deterministic synthetic domain identity for one
// occurrence.
func OccurrenceDomainID(occurrence Occurrence) string {
	return uuid.NewSHA1(occurrenceDomainNamespace, []byte(occurrence.Key)).String()
}

// HeartbeatProducer emits the single daily phone-home heartbeat.
type HeartbeatProducer struct{}

// NewHeartbeatProducer constructs the heartbeat producer.
func NewHeartbeatProducer() Producer { return HeartbeatProducer{} }

func (HeartbeatProducer) ID() string { return ProducerHeartbeat }

// Produce builds exactly one heartbeat envelope. The heartbeat has no domain
// row and no fan-out: one occurrence is one telemetry event, and the
// idempotency key is the canonical due time so a replayed occurrence is a
// bounded no-op rather than a second reported day.
func (HeartbeatProducer) Produce(
	_ context.Context,
	_ pgx.Tx,
	schedule Schedule,
	occurrence Occurrence,
) (Outcome, error) {
	scheduledFor := occurrence.ScheduledFor.UTC().Format(time.RFC3339)
	envelope := jobcontract.Envelope{
		ContractVersion: jobcontract.ContractVersionV1,
		CorrelationID:   "fixed-schedule:" + schedule.ID + ":" + scheduledFor,
		IdempotencyKey:  "heartbeat:" + scheduledFor,
		Domain: jobcontract.DomainLink{
			Type: "schedule_occurrence",
			ID:   OccurrenceDomainID(occurrence),
		},
		Payload: jobcontract.HeartbeatPayload{ScheduledFor: scheduledFor},
	}
	return Outcome{Requests: []JobRequest{{Kind: jobcontract.KindHeartbeat, Envelope: envelope}}}, nil
}

// RetentionSpec binds one schedule to one bounded deletion scope.
type RetentionSpec struct {
	// Policy is the checked-in retention policy value.
	Policy string
	// Retention is how much history the store keeps. The delete cutoff is the
	// occurrence's canonical due time minus this duration, so the cutoff is a
	// pure function of the occurrence and never of the wall clock at execution.
	Retention time.Duration
	// BatchSize bounds one deletion pass.
	BatchSize int
}

// RetentionProducer emits bounded retention requests for the retention
// schedules. Each schedule maps to exactly one policy: the two prune entries
// stay distinct operations so one can be paused or rolled back alone.
type RetentionProducer struct {
	byScheduleID map[string]RetentionSpec
}

// NewRetentionProducer constructs the retention producer with the checked-in
// policy bindings.
func NewRetentionProducer() Producer {
	return &RetentionProducer{byScheduleID: map[string]RetentionSpec{
		"prune_rate_limit_observations": {
			Policy: jobcontract.RetentionRateLimitObservation,
			// SYNC_RATE_LIMIT_OBSERVATION_RETENTION_DAYS default, CHAOS-2758.
			Retention: 14 * 24 * time.Hour,
			BatchSize: 500,
		},
		"prune_external_ingest_batches": {
			Policy: jobcontract.RetentionExternalIngestBatch,
			// EXTERNAL_INGEST_STATUS_RETENTION_DAYS default, CHAOS-2694.
			Retention: 90 * 24 * time.Hour,
			BatchSize: 500,
		},
	}}
}

func (*RetentionProducer) ID() string { return ProducerRetentionCleanup }

// Produce builds one bounded retention request.
func (producer *RetentionProducer) Produce(
	_ context.Context,
	_ pgx.Tx,
	schedule Schedule,
	occurrence Occurrence,
) (Outcome, error) {
	spec, ok := producer.byScheduleID[schedule.ID]
	if !ok {
		return Outcome{}, fmt.Errorf(
			"%w: retention producer has no policy for schedule %s",
			ErrProducerUnavailable, schedule.ID,
		)
	}
	deleteBefore := occurrence.ScheduledFor.UTC().Add(-spec.Retention).Format(time.RFC3339)
	envelope := jobcontract.Envelope{
		ContractVersion: jobcontract.ContractVersionV1,
		CorrelationID:   "fixed-schedule:" + schedule.ID + ":" + occurrence.ScheduledFor.UTC().Format(time.RFC3339),
		IdempotencyKey:  "retention:" + spec.Policy + ":" + occurrence.ScheduledFor.UTC().Format("2006-01-02"),
		Domain: jobcontract.DomainLink{
			Type: "maintenance_run",
			ID:   OccurrenceDomainID(occurrence),
		},
		Payload: jobcontract.RetentionCleanupPayload{
			BatchSize:       spec.BatchSize,
			DeleteBefore:    deleteBefore,
			RetentionPolicy: spec.Policy,
		},
	}
	return Outcome{Requests: []JobRequest{{Kind: jobcontract.KindRetentionCleanup, Envelope: envelope}}}, nil
}

// remainingScopeBuilder produces the family scope for one organization on one
// due date. Scope shapes are validated by internal/jobs/metrics/remaining, so
// an incorrect field here fails the transaction rather than persisting a run
// no handler can execute.
type remainingScopeBuilder func(day string) (json.RawMessage, error)

// remainingFamilyBinding maps one schedule to one remaining-metrics family.
type remainingFamilyBinding struct {
	Family string
	Scope  remainingScopeBuilder
}

// RemainingMetricsStore is the durable remaining-metrics run writer. It is an
// interface so the producer can be tested without PostgreSQL; production wires
// *remaining.PostgresStore.
type RemainingMetricsStore interface {
	StartRunTx(context.Context, pgx.Tx, remaining.StartRunRequest, remaining.PartitionPublisher) (remaining.Run, error)
}

// OrganizationLister enumerates the organizations a fan-out schedule covers.
type OrganizationLister interface {
	ActiveOrganizationIDs(context.Context, pgx.Tx) ([]string, error)
}

// RemainingMetricsFanoutProducer materializes one remaining-metrics run per
// active organization and lets the domain store publish each partition.
//
// The legacy Beat entries fanned out per organization because the compute is
// organization scoped: a single blank-organization run matches zero rows for a
// UUID-scoped tenant, which is exactly the CHAOS-2849 failure this preserves.
type RemainingMetricsFanoutProducer struct {
	store        RemainingMetricsStore
	publisher    remaining.PartitionPublisher
	lister       OrganizationLister
	byScheduleID map[string]remainingFamilyBinding
}

// NewRemainingMetricsFanoutProducer constructs the fan-out producer with the
// checked-in family bindings.
func NewRemainingMetricsFanoutProducer(
	store RemainingMetricsStore,
	publisher remaining.PartitionPublisher,
	lister OrganizationLister,
) (Producer, error) {
	if store == nil || publisher == nil || lister == nil {
		return nil, ErrProducerUnavailable
	}
	return &RemainingMetricsFanoutProducer{
		store:     store,
		publisher: publisher,
		lister:    lister,
		byScheduleID: map[string]remainingFamilyBinding{
			"complexity_daily_fanout": {
				Family: "complexity",
				Scope: func(day string) (json.RawMessage, error) {
					return json.Marshal(map[string]any{
						"version": 1, "day": day, "backfill_days": 1,
					})
				},
			},
			"release_impact_daily_fanout": {
				Family: "release_impact",
				Scope: func(day string) (json.RawMessage, error) {
					return json.Marshal(map[string]any{
						"version": 1, "day": day, "backfill_days": 1,
						"recomputation_window_days": 7,
					})
				},
			},
			"recommendations_daily_fanout": {
				Family: "recommendations",
				Scope: func(string) (json.RawMessage, error) {
					return json.Marshal(map[string]any{"version": 1, "window": 14})
				},
			},
			"membership_backfill_daily_fanout": {
				Family: "membership_backfill",
				// An empty repository list is the no-LLM whole-organization
				// backfill the legacy safety net ran.
				Scope: func(string) (json.RawMessage, error) {
					return json.Marshal(map[string]any{"version": 1, "repo_ids": []string{}})
				},
			},
			"capacity_forecast_weekly_fanout": {
				Family: "capacity",
				Scope: func(string) (json.RawMessage, error) {
					return json.Marshal(map[string]any{
						"version": 1, "all_teams": true,
						"history_days": 90, "simulations": 10000,
					})
				},
			},
		},
	}, nil
}

func (*RemainingMetricsFanoutProducer) ID() string { return ProducerRemainingMetricsFanout }

// Produce starts one deterministic run per active organization. Run identity
// is derived from organization, family, generation, and scope key, and the
// generation is the occurrence's canonical due time, so replaying the same
// occurrence verifies the existing run instead of creating a second one.
func (producer *RemainingMetricsFanoutProducer) Produce(
	ctx context.Context,
	tx pgx.Tx,
	schedule Schedule,
	occurrence Occurrence,
) (Outcome, error) {
	binding, ok := producer.byScheduleID[schedule.ID]
	if !ok {
		return Outcome{}, fmt.Errorf(
			"%w: remaining-metrics producer has no family for schedule %s",
			ErrProducerUnavailable, schedule.ID,
		)
	}
	organizationIDs, err := producer.lister.ActiveOrganizationIDs(ctx, tx)
	if err != nil {
		return Outcome{}, err
	}
	if len(organizationIDs) == 0 {
		return Outcome{SkipReason: "no_active_organizations"}, nil
	}
	dueTime := occurrence.ScheduledFor.UTC()
	day := dueTime.Format("2006-01-02")
	scope, err := binding.Scope(day)
	if err != nil {
		return Outcome{}, fmt.Errorf("build %s scope: %w", binding.Family, err)
	}
	generation := "fixed-schedule:" + schedule.ID + ":" + dueTime.Format(time.RFC3339)

	handoffs := 0
	for _, organizationID := range organizationIDs {
		if err := ctx.Err(); err != nil {
			return Outcome{}, err
		}
		if _, err := producer.store.StartRunTx(ctx, tx, remaining.StartRunRequest{
			OrganizationID: organizationID,
			Family:         binding.Family,
			Generation:     generation,
			ScopeKey:       day,
			Scopes:         []json.RawMessage{scope},
		}, producer.publisher); err != nil {
			return Outcome{}, fmt.Errorf(
				"start %s run for organization: %w", binding.Family, err,
			)
		}
		handoffs++
	}
	return Outcome{Handoffs: handoffs}, nil
}
