package fixed

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/jobcontract"
	"github.com/full-chaos/dev-health-ops/internal/joboutbox"
	"github.com/full-chaos/dev-health-ops/internal/jobs/metrics/daily"
	"github.com/full-chaos/dev-health-ops/internal/jobs/metrics/remaining"
	"github.com/full-chaos/dev-health-ops/internal/jobs/workgraph"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

// SkipNoActiveOrganizations marks a fan-out occurrence that found no active
// organization to schedule.
const SkipNoActiveOrganizations = "no_active_organizations"

// DegradedRejectedOrganizations marks a fan-out occurrence that produced work
// for some organizations while permanently rejecting others -- most often an
// organization whose id the job contract refuses (jobcontract.uuidPattern
// enforces RFC 4122, which a hand-written id like
// 11111111-2222-3333-4444-555555555555 does not satisfy even though
// uuid.Parse accepts it). It is deliberately a DEGRADED condition rather than
// a window failure: the rejected tenant is unactionable until its row is
// corrected, and failing the shared window instead starves every other tenant
// forever (CHAOS-3903).
const DegradedRejectedOrganizations = "organizations_rejected"

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

// SyncCoverageRefreshProducer emits one bounded global sweep. The worker
// resolves the selected configurations and all retained facts from the domain
// database, so the scheduled envelope stays small and contains no tenant data.
type SyncCoverageRefreshProducer struct{ limit int }

// NewSyncCoverageRefreshProducer constructs the checked 100-row safety-net
// sweep used by the former Celery task.
func NewSyncCoverageRefreshProducer() Producer {
	return SyncCoverageRefreshProducer{limit: 100}
}

func (SyncCoverageRefreshProducer) ID() string { return ProducerSyncCoverageRefresh }

func (producer SyncCoverageRefreshProducer) Produce(
	_ context.Context,
	_ pgx.Tx,
	schedule Schedule,
	occurrence Occurrence,
) (Outcome, error) {
	scheduledFor := occurrence.ScheduledFor.UTC().Format(time.RFC3339)
	envelope := jobcontract.Envelope{
		ContractVersion: jobcontract.ContractVersionV1,
		CorrelationID:   "fixed-schedule:" + schedule.ID + ":" + scheduledFor,
		IdempotencyKey:  "sync-coverage-refresh:" + scheduledFor,
		Domain: jobcontract.DomainLink{
			Type: "schedule_occurrence",
			ID:   OccurrenceDomainID(occurrence),
		},
		Payload: jobcontract.SyncCoverageRefreshPayload{
			ScheduledFor: scheduledFor,
			Limit:        producer.limit,
		},
	}
	return Outcome{Requests: []JobRequest{{Kind: jobcontract.KindSyncCoverageRefresh, Envelope: envelope}}}, nil
}

// RetentionSpec binds one schedule to one bounded deletion scope.
type RetentionSpec struct {
	// Policy is the checked-in retention policy value.
	Policy string
	// ContractVersion preserves each published policy domain. New policies use
	// a new version rather than widening an older required-field enum in place.
	ContractVersion int
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

// ErrRetentionConfiguration identifies an operator-supplied retention horizon
// that cannot be honored. It is deliberately an error rather than a bounded
// skip: a misconfigured horizon never repairs itself, so reporting it as
// "nothing to do" would let retention silently stop while the schedule looked
// healthy.
var ErrRetentionConfiguration = errors.New("retention horizon configuration is invalid")

// retentionDays resolves the operator-configurable horizon.
//
// Unset, empty, and unparseable values take the checked default, matching both
// legacy Python readers exactly. A NEGATIVE value does not.
//
// The legacy readers clamp with max(0, days), which turns a one-character typo
// into "delete every terminal row", with no backstop anywhere: the resulting
// payload has a valid batch size, a valid RFC3339 UTC cutoff, and violates no
// contract. Reviewed jointly with the retention handler owner and ruled: a
// negative value is not an expression of intent, so it fails the occurrence
// loudly instead of being interpreted as the most destructive reading
// available. The clamp branch was also unreachable in any healthy installation
// — a negative override deletes everything nightly from the moment it is set —
// so refusing it changes nothing observable for anyone whose data is intact.
//
// Zero remains legal. "Retain nothing" is a coherent posture an operator can
// mean, and unlike a negative it is exactly what was typed.
func retentionDays(name string, fallbackDays int) (time.Duration, error) {
	days := fallbackDays
	if raw, present := os.LookupEnv(name); present && strings.TrimSpace(raw) != "" {
		trimmed := strings.TrimSpace(raw)
		parsed, err := strconv.Atoi(trimmed)
		switch {
		case err != nil:
			// Unparseable text carries no intent at all, so the checked default
			// is the safe reading and matches the legacy behavior.
		case parsed < 0:
			return 0, fmt.Errorf(
				"%w: %s=%q is negative; a negative horizon would delete every row "+
					"older than the occurrence itself",
				ErrRetentionConfiguration, name, trimmed,
			)
		default:
			days = parsed
		}
	}
	return time.Duration(days) * 24 * time.Hour, nil
}

// RetentionProducer emits bounded retention requests for the retention
// schedules. Each schedule maps to exactly one policy: the two prune entries
// stay distinct operations so one can be paused or rolled back alone.
type RetentionProducer struct {
	byScheduleID          map[string]RetentionSpec
	askDevAdmission       AskDevRetentionAdmission
	activeProducerVersion int
}

// NewRetentionProducer constructs the retention producer with the checked-in
// policy bindings at the currently active v2 route. Ask Dev v3 stays dark
// unless the composition root supplies both lifecycle admission and a route
// whose producer version has completed consumer rollout.
func NewRetentionProducer() Producer {
	producer, err := NewRetentionProducerForRoute(
		disabledAskDevRetentionAdmission{}, jobcontract.ContractVersionV2,
	)
	if err != nil {
		panic(err)
	}
	return producer
}

// NewRetentionProducerForRoute binds the producer to the active migration
// route and the canonical Ask Dev lifecycle admission reader.
func NewRetentionProducerForRoute(
	askDevAdmission AskDevRetentionAdmission,
	activeProducerVersion int,
) (Producer, error) {
	if askDevAdmission == nil || activeProducerVersion < jobcontract.ContractVersionV1 ||
		activeProducerVersion > jobcontract.ContractVersionV3 {
		return nil, ErrProducerUnavailable
	}
	const (
		rateLimitEnv      = "SYNC_RATE_LIMIT_OBSERVATION_RETENTION_DAYS"
		externalIngestEnv = "EXTERNAL_INGEST_STATUS_RETENTION_DAYS"
	)
	return &RetentionProducer{
		askDevAdmission:       askDevAdmission,
		activeProducerVersion: activeProducerVersion,
		byScheduleID: map[string]RetentionSpec{
			"prune_rate_limit_observations": {
				Policy:          jobcontract.RetentionRateLimitObservations,
				ContractVersion: jobcontract.ContractVersionV2,
				DefaultDays:     14, // CHAOS-2758
				// The handler drains one occurrence chunk-by-chunk until the work is
				// gone, so this bounds a single pass rather than the total deletion.
				// No catch-up occurrence is needed to clear a first-run backlog.
				BatchSize:        500,
				RetentionDaysEnv: rateLimitEnv,
			},
			"prune_external_ingest_batches": {
				Policy:           jobcontract.RetentionExternalIngestBatches,
				ContractVersion:  jobcontract.ContractVersionV2,
				DefaultDays:      90, // CHAOS-2694
				BatchSize:        500,
				RetentionDaysEnv: externalIngestEnv,
			},
			"prune_ask_dev_conversations": {
				Policy:          jobcontract.RetentionAskDevConversations,
				ContractVersion: jobcontract.ContractVersionV3,
				// Conversation.expires_at already contains the exact 0/30-day
				// product policy. A second horizon here would silently double it.
				DefaultDays: 0,
				BatchSize:   500,
			},
		},
	}, nil
}

func (*RetentionProducer) ID() string { return ProducerRetentionCleanup }

// Produce builds one bounded retention request.
func (producer *RetentionProducer) Produce(
	ctx context.Context,
	tx pgx.Tx,
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
	if spec.ContractVersion > producer.activeProducerVersion {
		return Outcome{SkipReason: "consumer_version_incompatible"}, nil
	}
	if spec.Policy == jobcontract.RetentionAskDevConversations {
		state, err := producer.askDevAdmission.State(ctx, tx)
		if err != nil {
			return Outcome{}, fmt.Errorf("schedule %s: %w", schedule.ID, err)
		}
		if !state.Eligible() {
			return Outcome{SkipReason: "ask_dev_inactive_without_state"}, nil
		}
	}
	// The handler reads no environment and requires a UTC RFC3339 cutoff with a
	// trailing Z: a non-UTC offset is a permanent failure there. Deriving it
	// from the occurrence's canonical due time also makes the cutoff immutable
	// across retries, so an interrupted drain resumes against the same range.
	retention, err := retentionDays(spec.RetentionDaysEnv, spec.DefaultDays)
	if err != nil {
		return Outcome{}, fmt.Errorf("schedule %s: %w", schedule.ID, err)
	}
	deleteBefore := occurrence.ScheduledFor.UTC().Add(-retention).Format(time.RFC3339)
	envelope := jobcontract.Envelope{
		ContractVersion: spec.ContractVersion,
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

// DailyMetricsFanoutStore owns the durable scheduler-side half of the nightly
// daily-metrics handoff. The repository snapshot is intentionally absent from
// this interface: only the heavy worker may read ClickHouse and materialize it.
type DailyMetricsFanoutStore interface {
	StartScheduledFanoutRunTx(context.Context, pgx.Tx, daily.ScheduledFanoutRequest, daily.RunPublisher) (daily.Run, error)
}

// DailyMetricsFanoutProducer creates one durable run and dispatch handoff per
// active organization. It does not create a partition; the heavy worker first
// discovers the organization repository IDs from ClickHouse and atomically
// materializes non-empty partitions under that run.
type DailyMetricsFanoutProducer struct {
	store     DailyMetricsFanoutStore
	publisher daily.RunPublisher
	lister    OrganizationLister
}

func NewDailyMetricsFanoutProducer(
	store DailyMetricsFanoutStore,
	publisher daily.RunPublisher,
	lister OrganizationLister,
) (Producer, error) {
	if store == nil || publisher == nil || lister == nil {
		return nil, ErrProducerUnavailable
	}
	return &DailyMetricsFanoutProducer{store: store, publisher: publisher, lister: lister}, nil
}

func (*DailyMetricsFanoutProducer) ID() string { return ProducerDailyMetricsFanout }

func (producer *DailyMetricsFanoutProducer) Produce(
	ctx context.Context,
	tx pgx.Tx,
	schedule Schedule,
	occurrence Occurrence,
) (Outcome, error) {
	if producer == nil || schedule.ID != "daily_metrics_fanout" ||
		schedule.TargetKind != jobcontract.KindDailyMetricsDispatch {
		return Outcome{}, ErrProducerUnavailable
	}
	organizationIDs, err := producer.lister.ActiveOrganizationIDs(ctx, tx)
	if err != nil {
		return Outcome{}, err
	}
	if len(organizationIDs) == 0 {
		return Outcome{SkipReason: SkipNoActiveOrganizations}, nil
	}
	due := occurrence.ScheduledFor.UTC()
	generation := "fixed-schedule:" + schedule.ID + ":" + due.Format(time.RFC3339)
	handoffs, rejected := 0, 0
	for _, organizationID := range organizationIDs {
		if err := ctx.Err(); err != nil {
			return Outcome{}, err
		}
		// Same savepoint isolation as the remaining-metrics fan-out: one
		// organization the contract permanently rejects must not discard the
		// runs already materialized for the others (CHAOS-3903).
		if err := producer.startOrganization(
			ctx, tx, organizationID, due, generation,
		); err != nil {
			if !permanentForOrganization(err) {
				return Outcome{}, err
			}
			rejected++
			continue
		}
		handoffs++
	}
	outcome := Outcome{Handoffs: handoffs}
	if rejected > 0 {
		outcome.Degraded = DegradedRejectedOrganizations
	}
	if handoffs == 0 && rejected > 0 {
		outcome.SkipReason = DegradedRejectedOrganizations
	}
	return outcome, nil
}

func (producer *DailyMetricsFanoutProducer) startOrganization(
	ctx context.Context,
	tx pgx.Tx,
	organizationID string,
	due time.Time,
	generation string,
) error {
	nested, err := tx.Begin(ctx)
	if err != nil {
		return fmt.Errorf("open savepoint for organization: %w", err)
	}
	committed := false
	defer func() {
		if committed {
			return
		}
		rollbackCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		defer cancel()
		_ = nested.Rollback(rollbackCtx)
	}()
	if _, err := producer.store.StartScheduledFanoutRunTx(ctx, nested, daily.ScheduledFanoutRequest{
		OrganizationID: organizationID,
		TargetDay:      due,
		Generation:     generation,
	}, producer.publisher); err != nil {
		return fmt.Errorf("start daily metrics run for organization: %w", err)
	}
	if err := nested.Commit(ctx); err != nil {
		return fmt.Errorf("release savepoint for organization: %w", err)
	}
	committed = true
	return nil
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

	handoffs, rejected := 0, 0
	for _, organizationID := range organizationIDs {
		if err := ctx.Err(); err != nil {
			return Outcome{}, err
		}
		produced, err := producer.startOrganization(
			ctx, tx, occurrence, binding, organizationID, generation, day, scope,
		)
		if err != nil {
			if !permanentForOrganization(err) {
				return Outcome{}, err
			}
			// One tenant whose data this schedule can never act on must not
			// discard the work already materialized for every other tenant.
			// Returning here rolled the engine's transaction back on every
			// tick, so a single organization row the job contract rejects held
			// the whole fixed scheduler unready indefinitely (CHAOS-3903).
			rejected++
			continue
		}
		handoffs += produced
	}
	outcome := Outcome{Handoffs: handoffs}
	if rejected > 0 {
		// Degraded, not SkipReason: SkipReason only reaches telemetry when the
		// occurrence produced nothing at all, and the whole point here is that
		// the other tenants' work DID happen.
		outcome.Degraded = DegradedRejectedOrganizations
	}
	if handoffs == 0 && rejected > 0 {
		outcome.SkipReason = DegradedRejectedOrganizations
	}
	return outcome, nil
}

// permanentForOrganization reports whether an error will fail identically on
// every retry for the SAME organization, so retrying it costs a window and
// changes nothing.
//
// A contract or policy rejection is a statement about the envelope's shape:
// the same organization will produce the same envelope and be refused again.
// A durable-state rejection is a statement about rows that already exist and
// disagree with what this schedule would write. Neither heals with time.
//
// Everything else -- a dropped connection, a pool timeout, a lock wait -- is
// transient and MUST still fail the window, because skipping a tenant on a
// transient fault would silently drop its work for that occurrence and the
// ledger would record the window as healthy.
func permanentForOrganization(err error) bool {
	return errors.Is(err, joboutbox.ErrContractRejected) ||
		errors.Is(err, joboutbox.ErrPolicyRejected) ||
		errors.Is(err, remaining.ErrInvalidState) ||
		errors.Is(err, daily.ErrInvalidState) ||
		errors.Is(err, workgraph.ErrInvalidState)
}

// startOrganization produces one organization's work inside its OWN savepoint.
//
// The savepoint is what makes the isolation real rather than incidental. The
// engine runs an entire occurrence in one transaction, so a per-organization
// failure that has already issued a statement aborts that transaction, and
// every later organization would fail with "current transaction is aborted"
// regardless of its own data. pgx opens a savepoint for a Begin on an existing
// Tx, so rolling the nested transaction back discards exactly this
// organization's writes and leaves the outer transaction usable.
func (producer *RemainingMetricsFanoutProducer) startOrganization(
	ctx context.Context,
	tx pgx.Tx,
	occurrence Occurrence,
	binding remainingFamilyBinding,
	organizationID string,
	generation string,
	day string,
	scope json.RawMessage,
) (int, error) {
	nested, err := tx.Begin(ctx)
	if err != nil {
		return 0, fmt.Errorf("open savepoint for organization: %w", err)
	}
	committed := false
	defer func() {
		if committed {
			return
		}
		rollbackCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		defer cancel()
		_ = nested.Rollback(rollbackCtx)
	}()

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
	handoffs := 0
	if binding.RequiresGraphBuild {
		completionKey, err := producer.startGraphBuild(
			ctx, nested, occurrence, generation, organizationID,
		)
		if err != nil {
			return 0, err
		}
		// The projection stays ineligible until the build's durable
		// completion fence commits, which is this contract's equivalent of
		// the legacy immutable Celery chain.
		request.PrerequisiteCompletionKey = completionKey
		handoffs++
	}
	if _, err := producer.store.StartRunTx(ctx, nested, request, producer.publisher); err != nil {
		return 0, fmt.Errorf(
			"start %s run for organization: %w", binding.Family, err,
		)
	}
	handoffs++
	if err := nested.Commit(ctx); err != nil {
		return 0, fmt.Errorf("release savepoint for organization: %w", err)
	}
	committed = true
	return handoffs, nil
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
