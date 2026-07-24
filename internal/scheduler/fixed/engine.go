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

// ScheduleResult is the bounded outcome for one schedule in one window.
type ScheduleResult struct {
	ScheduleID string
	Due        int
	Claimed    int
	Duplicate  int
	Handoffs   int
	Skipped    int
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
) ScheduleResult {
	outcome := ScheduleResult{ScheduleID: schedule.ID}
	occurrences, err := DueOccurrences(schedule, observedAt)
	if err != nil {
		outcome.Err = err
		return outcome
	}
	outcome.Due = len(occurrences)
	for _, occurrence := range occurrences {
		claimed, duplicate, handoffs, skipped, err := engine.runOccurrence(ctx, schedule, occurrence)
		outcome.Claimed += claimed
		outcome.Duplicate += duplicate
		outcome.Handoffs += handoffs
		outcome.Skipped += skipped
		if err != nil {
			outcome.Err = err
			return outcome
		}
	}
	missing, err := engine.missingFor(ctx, schedule, observedAt)
	if err != nil {
		outcome.Err = err
		return outcome
	}
	outcome.MissingFor = missing
	return outcome
}

// runOccurrence performs the single transaction that owns one occurrence:
// claim, produce, publish, record. Every write commits together, so a crash at
// any point either leaves the occurrence entirely unmaterialized and eligible
// for the next tick, or fully materialized and durably claimed.
func (engine *Engine) runOccurrence(
	ctx context.Context,
	schedule Schedule,
	occurrence Occurrence,
) (claimed, duplicate, handoffs, skipped int, err error) {
	producer, ok := engine.producers.Producer(schedule.ProducerID)
	if !ok {
		return 0, 0, 0, 0, fmt.Errorf("%w: %s", ErrProducerUnavailable, schedule.ProducerID)
	}
	tx, err := engine.beginner.Begin(ctx)
	if err != nil {
		return 0, 0, 0, 0, fmt.Errorf("begin fixed schedule transaction: %w", err)
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
		return 0, 0, 0, 0, err
	}
	if claim == ClaimDuplicate {
		// Another replica or an earlier tick owns this due time. Rolling back
		// is correct: the winner's transaction already carries the production
		// work, and repeating it here would double the product effect.
		return 0, 1, 0, 0, nil
	}

	result, err := producer.Produce(ctx, tx, schedule, occurrence)
	if err != nil {
		return 0, 0, 0, 0, fmt.Errorf("produce occurrence %s: %w", occurrence.Key, err)
	}
	published := 0
	for _, request := range result.Requests {
		descriptor, ok := engine.registry.Descriptor(request.Kind)
		if !ok {
			return 0, 0, 0, 0, fmt.Errorf(
				"%w: producer %s emitted unregistered kind %s",
				ErrInvalidSchedule, schedule.ProducerID, request.Kind,
			)
		}
		if request.Kind != schedule.TargetKind {
			return 0, 0, 0, 0, fmt.Errorf(
				"%w: schedule %s targets %s but produced %s",
				ErrInvalidSchedule, schedule.ID, schedule.TargetKind, request.Kind,
			)
		}
		if err := engine.publisher.Publish(ctx, tx, request, descriptor.Executable()); err != nil {
			return 0, 0, 0, 0, fmt.Errorf("publish %s for occurrence %s: %w", request.Kind, occurrence.Key, err)
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
	if err := engine.ledger.Complete(ctx, tx, occurrence, status, total, reason); err != nil {
		return 0, 0, 0, 0, err
	}
	if err := tx.Commit(ctx); err != nil {
		return 0, 0, 0, 0, fmt.Errorf("commit fixed schedule occurrence %s: %w", occurrence.Key, err)
	}
	committed = true
	if status == OccurrenceSkipped {
		return 1, 0, 0, 1, nil
	}
	return 1, 0, total, 0, nil
}

// missingFor reports how far the newest recorded occurrence lags the newest
// due time. A schedule that has never produced an occurrence reports the full
// age of its first expected due time, which is what makes a never-started
// producer alert instead of looking idle.
func (engine *Engine) missingFor(
	ctx context.Context,
	schedule Schedule,
	observedAt time.Time,
) (time.Duration, error) {
	location, err := schedule.Location()
	if err != nil {
		return 0, err
	}
	expected, ok := schedule.Cadence.Previous(observedAt, location)
	if !ok {
		return 0, fmt.Errorf("%w: %s", ErrInvalidCadence, schedule.ID)
	}
	tx, err := engine.beginner.Begin(ctx)
	if err != nil {
		return 0, fmt.Errorf("begin fixed schedule read: %w", err)
	}
	defer func() {
		rollbackCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		defer cancel()
		_ = tx.Rollback(rollbackCtx)
	}()
	last, present, err := engine.ledger.LastScheduledFor(ctx, tx, schedule.ID)
	if err != nil {
		return 0, err
	}
	if !present || last.Before(expected) {
		return observedAt.Sub(expected), nil
	}
	return 0, nil
}

// Schedules returns the engine's validated schedule table.
func (engine *Engine) Schedules() []Schedule {
	if engine == nil {
		return nil
	}
	return append([]Schedule(nil), engine.schedules...)
}
