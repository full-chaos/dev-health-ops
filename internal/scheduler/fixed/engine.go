package fixed

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/joboutbox"
	"github.com/full-chaos/dev-health-ops/internal/jobruntime"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// ErrEngineUnavailable identifies an engine that cannot run a window.
var ErrEngineUnavailable = errors.New("fixed schedule engine is unavailable")

// Publisher writes one job handoff inside the engine transaction. It exists so
// the engine can be tested without a live outbox and so the deferred/executable
// decision stays in one place.
type Publisher interface {
	Publish(ctx context.Context, tx pgx.Tx, request JobRequest, executable bool) error
}

// OutboxPublisher routes handoffs through the checked-in generic worker
// outbox. Deferred publication is chosen from the registry route, never from a
// producer, so a schedule cannot promote its own kind past the migration
// contract.
type OutboxPublisher struct {
	producer *joboutbox.Producer
}

// NewOutboxPublisher constructs a transaction-only outbox publisher. It owns
// no pool: the engine always supplies the transaction that also carries the
// occurrence claim and the producer's domain rows.
func NewOutboxPublisher(registry *jobruntime.Registry) (*OutboxPublisher, error) {
	if registry == nil {
		return nil, ErrEngineUnavailable
	}
	producer, err := joboutbox.NewTransactionProducer(registry)
	if err != nil {
		return nil, ErrEngineUnavailable
	}
	return &OutboxPublisher{producer: producer}, nil
}

// Publish persists one immutable job envelope.
func (publisher *OutboxPublisher) Publish(
	ctx context.Context,
	tx pgx.Tx,
	request JobRequest,
	executable bool,
) error {
	if publisher == nil || publisher.producer == nil {
		return ErrEngineUnavailable
	}
	switch {
	case executable && request.PrerequisiteCompletionKey != "":
		return publisher.producer.PublishAfter(ctx, tx, request.Kind, request.Envelope, request.PrerequisiteCompletionKey)
	case executable:
		return publisher.producer.Publish(ctx, tx, request.Kind, request.Envelope)
	case request.PrerequisiteCompletionKey != "":
		return publisher.producer.PublishDeferredAfter(ctx, tx, request.Kind, request.Envelope, request.PrerequisiteCompletionKey)
	default:
		return publisher.producer.PublishDeferred(ctx, tx, request.Kind, request.Envelope)
	}
}

// coldStartBaselineReason marks the first recorded occurrence of a schedule.
const coldStartBaselineReason = "cold_start_baseline"

// ScheduleResult is the bounded outcome for one schedule in one window.
type ScheduleResult struct {
	ScheduleID string
	Due        int
	Claimed    int
	Duplicate  int
	Handoffs   int
	Skipped    int
	// ColdStart records that this window wrote the schedule's baseline.
	ColdStart bool
	// StaleSkipped records that the newest due time was past this schedule's
	// staleness horizon and was deliberately not run.
	StaleSkipped bool
	// Degraded names a bounded condition that is not a failure, for example an
	// installation with no active organizations or a report whose durable handoff
	// is spent. It is exported so an operator can tell "nothing to do" from
	// "working", and it may accompany produced work.
	Degraded string
	// DegradedLoaded reports that Degraded came from the shared occurrence
	// ledger. It is true even when the durable verdict is clear.
	DegradedLoaded bool
	// Evaluated reports that a producer returned a verdict AND the occurrence
	// committed, so Degraded reflects a fresh, durable observation.
	//
	// "Committed verdict" is the precise meaning, and it is narrower than "the
	// producer ran". If publication, ledger completion or the commit itself fails
	// after Produce returned, this stays FALSE even though the producer did look.
	// That is deliberate: the transaction rolled back, so nothing the producer
	// observed is durable, and for the gauge's purpose an uncommitted verdict is
	// indistinguishable from never having looked. Treating it as an observation
	// would let a failed window clear a live reason.
	//
	// It exists because most windows do NOT evaluate most schedules: the loop
	// polls every few seconds while a 300 second schedule is due once per period
	// and a daily one once per day. Without this flag a consumer cannot tell "the
	// producer looked and found nothing wrong" from "the producer did not look",
	// and treating the second as the first silently clears a degraded reason that
	// is still true — which made a permanent fault visible for a single poll and
	// invisible to any realistic scrape interval.
	Evaluated bool
	// MissingFor is how long the newest recorded occurrence has been older
	// than the schedule expects. It drives missing-occurrence alerting.
	MissingFor time.Duration
	// Err is the first failure for this schedule. One failing schedule never
	// aborts the others: a broken retention producer must not stop the daily
	// safety nets from being scheduled.
	Err error
}

// WindowResult is the bounded outcome of one engine window.
type WindowResult struct {
	ObservedAt time.Time
	Schedules  []ScheduleResult
}

// Failed reports whether any schedule failed in this window.
func (result WindowResult) Failed() bool {
	for _, schedule := range result.Schedules {
		if schedule.Err != nil {
			return true
		}
	}
	return false
}

// Err joins every schedule failure in the window.
func (result WindowResult) Err() error {
	var failures []error
	for _, schedule := range result.Schedules {
		if schedule.Err != nil {
			failures = append(failures, fmt.Errorf("schedule %s: %w", schedule.ScheduleID, schedule.Err))
		}
	}
	return errors.Join(failures...)
}

type transactionBeginner interface {
	Begin(context.Context) (pgx.Tx, error)
}

// Engine runs one bounded window across every declared schedule.
//
// Each occurrence gets its own transaction. That is deliberate: a single
// transaction spanning all schedules would let one slow producer hold the
// occurrence ledger and the domain tables it writes for the whole window, and
// a failure in the last schedule would roll back work the earlier schedules
// had already correctly materialized.
type Engine struct {
	schedules []Schedule
	producers *ProducerSet
	ledger    Ledger
	publisher Publisher
	registry  *jobruntime.Registry
	beginner  transactionBeginner
}

// EngineConfig is the constructed dependency set for one process.
type EngineConfig struct {
	Schedules []Schedule
	Producers *ProducerSet
	Ledger    Ledger
	Publisher Publisher
	Registry  *jobruntime.Registry
	Pool      *pgxpool.Pool
}

// NewEngine validates the whole declaration set before returning. A schedule
// whose target kind is absent from the registry, or whose producer is not
// constructed, fails construction rather than being dropped: silently reducing
// coverage is the exact failure mode the cutover plan forbids.
func NewEngine(config EngineConfig) (*Engine, error) {
	if config.Pool == nil {
		return nil, ErrEngineUnavailable
	}
	return newEngine(config, config.Pool)
}

func newEngine(config EngineConfig, beginner transactionBeginner) (*Engine, error) {
	if config.Producers == nil || config.Ledger == nil || config.Publisher == nil ||
		config.Registry == nil || beginner == nil || len(config.Schedules) == 0 {
		return nil, ErrEngineUnavailable
	}
	if err := ValidateInventory(); err != nil {
		return nil, err
	}
	schedules := append([]Schedule(nil), config.Schedules...)
	sort.Slice(schedules, func(first, second int) bool {
		return schedules[first].ID < schedules[second].ID
	})
	for _, schedule := range schedules {
		if err := schedule.Validate(); err != nil {
			return nil, err
		}
		descriptor, ok := config.Registry.Descriptor(schedule.TargetKind)
		if !ok {
			return nil, fmt.Errorf(
				"%w: schedule %s targets unregistered kind %s",
				ErrInvalidSchedule, schedule.ID, schedule.TargetKind,
			)
		}
		if schedule.MaxAttempts > descriptor.MaxAttempts {
			return nil, fmt.Errorf(
				"%w: schedule %s allows %d attempts but %s permits %d",
				ErrInvalidSchedule, schedule.ID, schedule.MaxAttempts,
				descriptor.Kind, descriptor.MaxAttempts,
			)
		}
		if _, ok := config.Producers.Producer(schedule.ProducerID); !ok {
			return nil, fmt.Errorf(
				"%w: schedule %s declares producer %s",
				ErrProducerUnavailable, schedule.ID, schedule.ProducerID,
			)
		}
	}
	return &Engine{
		schedules: schedules,
		producers: config.Producers,
		ledger:    config.Ledger,
		publisher: config.Publisher,
		registry:  config.Registry,
		beginner:  beginner,
	}, nil
}

// Step runs one bounded window. It never returns partial success as failure or
// vice versa: every schedule reports its own outcome, and the caller decides
// what closes readiness.
func (engine *Engine) Step(ctx context.Context, observedAt time.Time) (WindowResult, error) {
	if engine == nil || ctx == nil || observedAt.IsZero() {
		return WindowResult{}, ErrEngineUnavailable
	}
	if err := ctx.Err(); err != nil {
		return WindowResult{}, err
	}
	observedAt = observedAt.UTC().Truncate(time.Second)
	result := WindowResult{ObservedAt: observedAt, Schedules: make([]ScheduleResult, 0, len(engine.schedules))}
	for _, schedule := range engine.schedules {
		if err := ctx.Err(); err != nil {
			return result, err
		}
		result.Schedules = append(result.Schedules, engine.stepSchedule(ctx, schedule, observedAt))
	}
	return result, nil
}

func (engine *Engine) stepSchedule(
	ctx context.Context,
	schedule Schedule,
	observedAt time.Time,
) (outcome ScheduleResult) {
	outcome = ScheduleResult{ScheduleID: schedule.ID}
	defer func() {
		evaluation, present, err := engine.lastEvaluation(ctx, schedule.ID)
		if err != nil {
			outcome.Err = errors.Join(outcome.Err, err)
			return
		}
		outcome.DegradedLoaded = true
		if present {
			outcome.Degraded = evaluation.Degraded
		} else {
			outcome.Degraded = ""
		}
	}()
	lastRecorded, err := engine.lastRecorded(ctx, schedule)
	if err != nil {
		outcome.Err = err
		return outcome
	}
	decision, err := DueOccurrence(schedule, observedAt, lastRecorded)
	if err != nil {
		outcome.Err = err
		return outcome
	}
	if decision.SkippedStale {
		outcome.StaleSkipped = true
	}
	if decision.Occurrence != nil {
		outcome.Due = 1
		// A cold start and a resumed-after-gap skip both record the boundary
		// without producing work, so they take the same baseline path.
		baseline := decision.ColdStart || decision.SkippedStale
		claimed, duplicate, handoffs, skipped, degraded, evaluated, err := engine.runOccurrence(
			ctx, schedule, *decision.Occurrence, baseline,
		)
		outcome.Claimed += claimed
		outcome.Duplicate += duplicate
		outcome.Handoffs += handoffs
		outcome.Skipped += skipped
		outcome.ColdStart = decision.ColdStart
		// Evaluated gates the degraded verdict, so an empty Degraded from a real
		// evaluation CLEARS a stale reason while a window that never ran the
		// producer leaves it alone. The cold-start baseline is excluded because it
		// deliberately produces no work and is not an observation of health.
		if evaluated {
			outcome.Evaluated = true
			if degraded != coldStartBaselineReason {
				outcome.Degraded = degraded
			}
		}
		if err != nil {
			outcome.Err = err
			return outcome
		}
	}
	outcome.MissingFor = engine.missingFor(schedule, observedAt, lastRecorded)
	return outcome
}

// lastRecorded reads the schedule's newest durable due time. It is the anchor
// both for deciding what is owed and for missing-occurrence alerting, so it is
// read once per schedule per window rather than twice.
func (engine *Engine) lastRecorded(
	ctx context.Context,
	schedule Schedule,
) (*Anchor, error) {
	tx, err := engine.beginner.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("begin fixed schedule read: %w", err)
	}
	defer func() {
		rollbackCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		defer cancel()
		_ = tx.Rollback(rollbackCtx)
	}()
	anchor, present, err := engine.ledger.LastOccurrence(ctx, tx, schedule.ID)
	if err != nil {
		return nil, err
	}
	if !present {
		return nil, nil
	}
	return &anchor, nil
}

func (engine *Engine) lastEvaluation(
	ctx context.Context,
	scheduleID string,
) (Evaluation, bool, error) {
	tx, err := engine.beginner.Begin(ctx)
	if err != nil {
		return Evaluation{}, false, fmt.Errorf("begin fixed schedule evaluation read: %w", err)
	}
	defer func() {
		rollbackCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		defer cancel()
		_ = tx.Rollback(rollbackCtx)
	}()
	return engine.ledger.LastEvaluation(ctx, tx, scheduleID)
}

// runOccurrence performs the single transaction that owns one occurrence:
// claim, produce, publish, record. Every write commits together, so a crash at
// any point either leaves the occurrence entirely unmaterialized and eligible
// for the next tick, or fully materialized and durably claimed.
func (engine *Engine) runOccurrence(
	ctx context.Context,
	schedule Schedule,
	occurrence Occurrence,
	baseline bool,
) (claimed, duplicate, handoffs, skipped int, degraded string, evaluated bool, err error) {
	producer, ok := engine.producers.Producer(schedule.ProducerID)
	if !ok {
		return 0, 0, 0, 0, "", false, fmt.Errorf("%w: %s", ErrProducerUnavailable, schedule.ProducerID)
	}
	tx, err := engine.beginner.Begin(ctx)
	if err != nil {
		return 0, 0, 0, 0, "", false, fmt.Errorf("begin fixed schedule transaction: %w", err)
	}
	committed := false
	defer func() {
		if committed {
			return
		}
		rollbackCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		defer cancel()
		_ = tx.Rollback(rollbackCtx)
	}()

	claim, err := engine.ledger.Claim(ctx, tx, occurrence)
	if err != nil {
		return 0, 0, 0, 0, "", false, err
	}
	if claim == ClaimDuplicate {
		// Another replica or an earlier tick owns this due time. Rolling back
		// is correct: the winner's transaction already carries the production
		// work, and repeating it here would double the product effect.
		return 0, 1, 0, 0, "", false, nil
	}

	if baseline {
		// A cold-start baseline records where this schedule begins so the next
		// window has an anchor. It deliberately produces no work: the ledger
		// carries none of Beat's history, so treating the boundary this process
		// started inside as owed would fire a schedule that Beat had already
		// run, or run one a full period early.
		if err := engine.ledger.Complete(
			ctx, tx, occurrence, OccurrenceSkipped, 0, coldStartBaselineReason, "",
		); err != nil {
			return 0, 0, 0, 0, "", false, err
		}
		if err := tx.Commit(ctx); err != nil {
			return 0, 0, 0, 0, "", false, fmt.Errorf("commit fixed schedule baseline %s: %w", occurrence.Key, err)
		}
		committed = true
		return 1, 0, 0, 1, coldStartBaselineReason, false, nil
	}

	result, err := producer.Produce(ctx, tx, schedule, occurrence)
	if err != nil {
		return 0, 0, 0, 0, "", false, fmt.Errorf("produce occurrence %s: %w", occurrence.Key, err)
	}
	published := 0
	for _, request := range result.Requests {
		descriptor, ok := engine.registry.Descriptor(request.Kind)
		if !ok {
			return 0, 0, 0, 0, "", false, fmt.Errorf(
				"%w: producer %s emitted unregistered kind %s",
				ErrInvalidSchedule, schedule.ProducerID, request.Kind,
			)
		}
		if request.Kind != schedule.TargetKind {
			return 0, 0, 0, 0, "", false, fmt.Errorf(
				"%w: schedule %s targets %s but produced %s",
				ErrInvalidSchedule, schedule.ID, schedule.TargetKind, request.Kind,
			)
		}
		if err := engine.publisher.Publish(ctx, tx, request, descriptor.Executable()); err != nil {
			return 0, 0, 0, 0, "", false, fmt.Errorf("publish %s for occurrence %s: %w", request.Kind, occurrence.Key, err)
		}
		published++
	}

	total := published + result.Handoffs
	status, reason := OccurrenceMaterialized, ""
	if total == 0 {
		status = OccurrenceSkipped
		reason = result.SkipReason
		if reason == "" {
			reason = "producer_returned_no_work"
		}
	}
	if err := engine.ledger.Complete(
		ctx, tx, occurrence, status, total, reason, result.Degraded,
	); err != nil {
		return 0, 0, 0, 0, "", false, err
	}
	if err := tx.Commit(ctx); err != nil {
		return 0, 0, 0, 0, "", false, fmt.Errorf("commit fixed schedule occurrence %s: %w", occurrence.Key, err)
	}
	committed = true
	// ONLY a producer-reported condition becomes Degraded. A SkipReason is
	// deliberately NOT promoted, and that distinction is load-bearing rather than
	// tidy:
	//
	// A skip reason describes THIS occurrence — "nothing was due", "no active
	// organizations". A degraded reason describes ONGOING state, and because it now
	// persists until the schedule next evaluates, promoting a skip made it latch for
	// a full evaluation period. On the weekly capacity fan-out that is a raised
	// gauge for a week: organizations added the day after Monday's skipped run leave
	// an operator looking at a fault that resolved days earlier.
	//
	// That regression reached a producer this change did not write, and the reason it
	// slipped through is worth recording: "the zero value preserves prior behaviour"
	// is true of the Degraded FIELD and false of the promotion path that fills it.
	// Heartbeat and retention were unaffected only because their successful outcomes
	// always carry work — luck about those two producers, not a property of the
	// change.
	//
	// The LEDGER is unaffected either way: Complete still receives the original
	// SkipReason, which must stay a bounded value from the occurrence-status
	// vocabulary.
	degraded = result.Degraded
	if status == OccurrenceSkipped {
		return 1, 0, 0, 1, degraded, true, nil
	}
	return 1, 0, total, 0, degraded, true, nil
}

// missingFor reports how far the newest recorded occurrence lags the newest due
// time, using the anchor already read for this window.
//
// It is measured against the anchor observed at the start of the window, so a
// schedule that just claimed its occurrence still reports the lag it had. That
// is deliberate: the value drives alerting on a schedule that stopped
// producing, and reporting zero the instant a catch-up ran would hide exactly
// the outage an operator needs to see.
func (engine *Engine) missingFor(
	schedule Schedule,
	observedAt time.Time,
	lastRecorded *Anchor,
) time.Duration {
	location, err := schedule.Location()
	if err != nil {
		return 0
	}
	expected, ok := schedule.Cadence.Previous(observedAt, location)
	if !ok {
		return 0
	}
	if lastRecorded == nil {
		// No history at all is not yet a missed occurrence: the very first
		// window records a baseline. Alerting on it would make every fresh
		// deployment start unhealthy.
		return 0
	}
	if lastRecorded.ScheduledFor.Before(expected) {
		return observedAt.Sub(expected)
	}
	return 0
}

// Schedules returns the engine's validated schedule table.
func (engine *Engine) Schedules() []Schedule {
	if engine == nil {
		return nil
	}
	return append([]Schedule(nil), engine.schedules...)
}
