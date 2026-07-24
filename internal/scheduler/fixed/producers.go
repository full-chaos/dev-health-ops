package fixed

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/jobcontract"
	"github.com/full-chaos/dev-health-ops/internal/joboutbox"
	"github.com/full-chaos/dev-health-ops/internal/jobs/metrics/remaining"
	"github.com/full-chaos/dev-health-ops/internal/jobs/workgraph"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

// SkipNoActiveOrganizations marks a fan-out occurrence that found no active
// organization to schedule.
const SkipNoActiveOrganizations = "no_active_organizations"

// Retention policy names.
//
// These are agreed cross-lane values, not this package's to define: the
// canonical list lives in internal/jobcontract on the runtime-truth lane, where
// internal/jobcontract/registry.go pins it element-by-element against the
// system.retention_cleanup schema enum so the constant list and the contract
// cannot drift apart. They are declared here only so the producer compiles
// before that lane merges, and this block is deleted in favour of the
// jobcontract constants at merge.
//
// The names are plural because the policy IS the table scope. Each retention
// store holds its table as a compile-time literal, never a constructor argument
// or payload field, so no operator input and no future policy can widen the
// target. A reviewer reading the policy name knows exactly which table can lose
// rows: `external_ingest_batches` is the table, and
// `rate_limit_observations` is the tail of `provider_rate_limit_observations`.
const (
	retentionRateLimitObservations = "rate_limit_observations"
	retentionExternalIngestBatches = "external_ingest_batches"
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
	// DefaultDays is the checked horizon when no operator override applies.
	DefaultDays int
	// BatchSize bounds one deletion pass.
	BatchSize int
	// RetentionDaysEnv is the operator override the legacy Python task honored.
	// It is read per occurrence rather than cached at construction: the
	// scheduler is long-lived, and a horizon changed by an operator must take
	// effect on the next occurrence rather than at the next process restart.
	// The legacy tasks re-read it on every run, so caching would also be a
	// silent behavioral change.
	//
	// The horizon is still resolved here, at the producer, so the cutoff that
	// travels in delete_before stays authoritative and the handler never reads
	// the environment. A worker whose environment differs from the scheduler's
	// would otherwise delete a different range than the one scheduled.
	RetentionDaysEnv string
}

// retentionDays resolves the operator-configurable horizon with the same
// semantics as the Python tasks being replaced, because a cutover must not
// silently change how long data is kept.
//
// Both legacy readers behave identically once normalized: an unset or empty
// value takes the default, an unparseable value takes the default, and a parsed
// value is clamped with max(0, days). The clamp is preserved deliberately even
// though it means a negative override collapses to zero, which deletes
// everything older than the occurrence rather than erroring. That is the
// current production behavior; changing it here would be an undeclared
// behavioral change during a cutover, so it is preserved and flagged in the
// lane handoff instead.
func retentionDays(name string, fallbackDays int) time.Duration {
	days := fallbackDays
	if raw, present := os.LookupEnv(name); present && strings.TrimSpace(raw) != "" {
		parsed, err := strconv.Atoi(strings.TrimSpace(raw))
		if err == nil {
			days = parsed
		}
	}
	if days < 0 {
		days = 0
	}
	return time.Duration(days) * 24 * time.Hour
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
	const (
		rateLimitEnv      = "SYNC_RATE_LIMIT_OBSERVATION_RETENTION_DAYS"
		externalIngestEnv = "EXTERNAL_INGEST_STATUS_RETENTION_DAYS"
	)
	return &RetentionProducer{byScheduleID: map[string]RetentionSpec{
		"prune_rate_limit_observations": {
			Policy:      retentionRateLimitObservations,
			DefaultDays: 14, // CHAOS-2758
			// The handler drains one occurrence chunk-by-chunk until the work is
			// gone, so this bounds a single pass rather than the total deletion.
			// No catch-up occurrence is needed to clear a first-run backlog.
			BatchSize:        500,
			RetentionDaysEnv: rateLimitEnv,
		},
		"prune_external_ingest_batches": {
			Policy:           retentionExternalIngestBatches,
			DefaultDays:      90, // CHAOS-2694
			BatchSize:        500,
			RetentionDaysEnv: externalIngestEnv,
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
	// The handler reads no environment and requires a UTC RFC3339 cutoff with a
	// trailing Z: a non-UTC offset is a permanent failure there. Deriving it
	// from the occurrence's canonical due time also makes the cutoff immutable
	// across retries, so an interrupted drain resumes against the same range.
	retention := retentionDays(spec.RetentionDaysEnv, spec.DefaultDays)
	deleteBefore := occurrence.ScheduledFor.UTC().Add(-retention).Format(time.RFC3339)
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
	// RequiresSeed marks a family whose durable run identity includes an
	// immutable generation seed. The remaining-metrics store requires a seed
	// for exactly the capacity family and rejects one for every other family,
	// so this is a per-family fact rather than a producer preference.
	RequiresSeed bool
	// RequiresGraphBuild marks a family whose legacy dispatcher chained a
	// work-graph build before the projection.
	RequiresGraphBuild bool
}

// generationSeedNamespace derives the immutable capacity seed.
var generationSeedNamespace = uuid.MustParse("b1c2d3e4-5f60-5a7b-8c9d-0e1f2a3b4c5d")

// deterministicGenerationSeed derives the immutable seed for one organization's
// occurrence. It must be a pure function of the occurrence and the
// organization: the store verifies the seed on every replay, so a random seed
// would make a retried occurrence conflict with the run it already created.
func deterministicGenerationSeed(occurrence Occurrence, organizationID string) int64 {
	digest := uuid.NewSHA1(generationSeedNamespace, []byte(occurrence.Key+"|"+organizationID))
	value := int64(binary.BigEndian.Uint64(digest[:8]) & 0x7fffffffffffffff)
	return value
}

// WorkGraphRequestWriter persists a work-graph execution request inside the
// caller's transaction. It is an interface so the producer stays testable.
type WorkGraphRequestWriter interface {
	WriteTx(context.Context, pgx.Tx, workgraph.Request) error
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
	graphWriter  WorkGraphRequestWriter
	byScheduleID map[string]remainingFamilyBinding
}

// NewRemainingMetricsFanoutProducer constructs the fan-out producer with the
// checked-in family bindings.
func NewRemainingMetricsFanoutProducer(
	store RemainingMetricsStore,
	publisher remaining.PartitionPublisher,
	lister OrganizationLister,
	graphWriter WorkGraphRequestWriter,
) (Producer, error) {
	if store == nil || publisher == nil || lister == nil || graphWriter == nil {
		return nil, ErrProducerUnavailable
	}
	return &RemainingMetricsFanoutProducer{
		store:       store,
		publisher:   publisher,
		lister:      lister,
		graphWriter: graphWriter,
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
				// The legacy dispatcher chained run_work_graph_build before the
				// projection precisely because this is the safety net for an
				// idle organization: projecting onto stale or absent edges
				// produces zero membership and still reports success, which
				// makes the backstop look healthy while repairing nothing.
				RequiresGraphBuild: true,
			},
			"capacity_forecast_weekly_fanout": {
				Family: "capacity",
				Scope: func(string) (json.RawMessage, error) {
					return json.Marshal(map[string]any{
						"version": 1, "all_teams": true,
						"history_days": 90, "simulations": 10000,
					})
				},
				// The capacity forecast is a Monte Carlo simulation, so its run
				// identity carries an immutable seed. The store requires one for
				// this family and rejects one for every other.
				RequiresSeed: true,
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
		// The legacy dispatcher fell back to a literal "default" organization
		// when the table was empty, which single-tenant installations relied
		// on. That sentinel cannot be reproduced here: remaining_metric_runs
		// types org_id as uuid, so "default" is unrepresentable, and inventing
		// a synthetic UUID would create runs whose scope matches no existing
		// data and report green while computing nothing. The occurrence is
		// therefore recorded as a bounded skip with a distinct reason so the
		// condition is visible in the ledger and in fixed_scheduler telemetry
		// rather than looking like healthy work.
		return Outcome{SkipReason: SkipNoActiveOrganizations}, nil
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
		request := remaining.StartRunRequest{
			OrganizationID: organizationID,
			Family:         binding.Family,
			Generation:     generation,
			ScopeKey:       day,
			Scopes:         []json.RawMessage{scope},
		}
		if binding.RequiresSeed {
			seed := deterministicGenerationSeed(occurrence, organizationID)
			request.GenerationSeed = &seed
		}
		if binding.RequiresGraphBuild {
			completionKey, err := producer.startGraphBuild(
				ctx, tx, occurrence, generation, organizationID,
			)
			if err != nil {
				return Outcome{}, err
			}
			// The projection stays ineligible until the build's durable
			// completion fence commits, which is this contract's equivalent of
			// the legacy immutable Celery chain.
			request.PrerequisiteCompletionKey = completionKey
			handoffs++
		}
		if _, err := producer.store.StartRunTx(ctx, tx, request, producer.publisher); err != nil {
			return Outcome{}, fmt.Errorf(
				"start %s run for organization: %w", binding.Family, err,
			)
		}
		handoffs++
	}
	return Outcome{Handoffs: handoffs}, nil
}

// startGraphBuild persists the work-graph build that must precede a membership
// projection and returns its durable completion key.
func (producer *RemainingMetricsFanoutProducer) startGraphBuild(
	ctx context.Context,
	tx pgx.Tx,
	occurrence Occurrence,
	generation string,
	organizationID string,
) (string, error) {
	scope, err := json.Marshal(map[string]any{})
	if err != nil {
		return "", fmt.Errorf("build work-graph scope: %w", err)
	}
	requestID := uuid.NewSHA1(
		occurrenceDomainNamespace,
		[]byte("workgraph.build|"+occurrence.Key+"|"+organizationID),
	).String()
	if err := producer.graphWriter.WriteTx(ctx, tx, workgraph.Request{
		ID:             requestID,
		OrganizationID: organizationID,
		Kind:           workgraph.KindBuild,
		Scope:          scope,
		LLMConcurrency: 1,
		CorrelationID:  generation,
		IdempotencyKey: generation + ":" + jobcontract.KindWorkGraphBuild + ":" + organizationID,
	}); err != nil {
		return "", fmt.Errorf("start work-graph build for organization: %w", err)
	}
	completionKey, err := joboutbox.CompletionKey("work_graph_execution_request", requestID)
	if err != nil {
		return "", fmt.Errorf("derive work-graph completion key: %w", err)
	}
	return completionKey, nil
}
