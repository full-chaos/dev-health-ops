package fixed

import (
	"context"
	"errors"
	"fmt"
	"sort"

	"github.com/full-chaos/dev-health-ops/internal/jobcontract"
	"github.com/jackc/pgx/v5"
)

var (
	// ErrProducerUnavailable identifies a schedule whose declared producer is
	// not constructed in this binary. Readiness stays closed rather than the
	// schedule silently never firing.
	ErrProducerUnavailable = errors.New("fixed schedule producer is unavailable")
	// ErrProducerNotImplemented identifies a declared schedule whose native Go
	// producer does not exist yet. It is deliberately a hard error: a schedule
	// that quietly produced nothing would look identical to a healthy one.
	ErrProducerNotImplemented = errors.New("fixed schedule producer is not implemented")
)

// JobRequest is one job handoff the engine writes through the generic worker
// outbox. The engine chooses deferred or executable publication from the
// checked-in route, so a producer cannot promote its own kind.
type JobRequest struct {
	Kind     string
	Envelope jobcontract.Envelope
	// PrerequisiteCompletionKey defers relay eligibility until a durable
	// completion fence commits. Empty means immediately eligible.
	PrerequisiteCompletionKey string
}

// Outcome is the bounded result of one producer invocation.
type Outcome struct {
	// Requests are envelopes the engine publishes.
	Requests []JobRequest
	// Handoffs counts durable handoffs the producer already persisted inside
	// the same transaction, for example through a domain store that owns its
	// own publisher. It exists so telemetry cannot report an occurrence as
	// zero-work when it did materialize a graph.
	Handoffs int
	// SkipReason records a bounded, non-error reason no work was produced, for
	// example an installation with no active organizations. It is recorded on
	// the occurrence so an operator can tell "nothing to do" from "broken".
	SkipReason string
	// Degraded records a bounded, non-fatal condition that coexists WITH
	// produced work. SkipReason cannot express it: that field only reaches
	// telemetry when the occurrence produced nothing at all.
	//
	// It exists because a per-row fault must not become a per-schedule failure.
	// A producer that sweeps many tenants can meet data it cannot act on for one
	// of them — a report run whose durable handoff is spent, say — and returning
	// an error there rolls the engine's transaction back, discarding the work it
	// had already correctly materialized for every OTHER tenant, on every tick,
	// forever. That is strictly less available than the Celery task being
	// replaced, which dispatches each row independently.
	//
	// Setting it is therefore the honest middle: the occurrence commits, the
	// unaffected tenants get their work, and the condition is exported as
	// fixed_scheduler_schedule_degraded, which deliberately does NOT close
	// readiness. Use it only for a condition that is genuinely scoped to some
	// rows and genuinely non-fatal for the rest. A fault that invalidates the
	// whole sweep is still an error.
	Degraded string
}

// Producer materializes the authoritative domain rows for one occurrence and
// declares the job handoffs it needs. It always runs inside the engine's
// claiming transaction, so a producer must never open its own transaction,
// commit, or perform a remote side effect: a partial graph would otherwise
// survive a rolled-back occurrence claim.
//
// A producer must be idempotent for a repeated occurrence key. The engine's
// claim makes a repeat rare, but a crash between the domain write and the
// commit is always possible.
type Producer interface {
	// ID matches Schedule.ProducerID.
	ID() string
	Produce(ctx context.Context, tx pgx.Tx, schedule Schedule, occurrence Occurrence) (Outcome, error)
}

// ProducerSet is the constructed producer registry for one process.
type ProducerSet struct {
	byID map[string]Producer
}

// NewProducerSet indexes producers by their declared identity.
func NewProducerSet(producers ...Producer) (*ProducerSet, error) {
	set := &ProducerSet{byID: make(map[string]Producer, len(producers))}
	for _, producer := range producers {
		if producer == nil || producer.ID() == "" {
			return nil, fmt.Errorf("%w: unnamed producer", ErrProducerUnavailable)
		}
		if _, duplicate := set.byID[producer.ID()]; duplicate {
			return nil, fmt.Errorf("%w: duplicate producer %s", ErrProducerUnavailable, producer.ID())
		}
		set.byID[producer.ID()] = producer
	}
	return set, nil
}

// Producer resolves a declared producer identity.
func (set *ProducerSet) Producer(id string) (Producer, bool) {
	if set == nil {
		return nil, false
	}
	producer, ok := set.byID[id]
	return producer, ok
}

// Missing reports the declared producer identities this process does not
// construct, in stable order. A non-empty result closes readiness: an
// unowned schedule is exactly the "unknown schedule ownership" condition
// TRD section 15 requires readiness to fail on.
func (set *ProducerSet) Missing(schedules []Schedule) []string {
	missing := make(map[string]struct{})
	for _, schedule := range schedules {
		if _, ok := set.Producer(schedule.ProducerID); !ok {
			missing[schedule.ProducerID] = struct{}{}
		}
	}
	names := make([]string, 0, len(missing))
	for name := range missing {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

// notImplementedProducer is a declared-but-unbuilt producer. Constructing one
// is a deliberate, visible statement that a schedule is owned on paper and not
// in code: every invocation fails, which closes readiness through the loop and
// keeps the schedule out of any "migrated" evidence.
type notImplementedProducer struct {
	id     string
	reason string
}

// NewNotImplementedProducer declares an unbuilt producer with the reason it is
// still outstanding. The reason is required so an operator reading a failing
// scheduler learns which lane owns the gap.
func NewNotImplementedProducer(id, reason string) Producer {
	return notImplementedProducer{id: id, reason: reason}
}

func (producer notImplementedProducer) ID() string { return producer.id }

// UnbuiltReason reports the recorded reason this producer is not built. It makes
// "declared but unbuilt" a queryable property of a constructed producer set
// rather than a fact only a reader of the construction site can see.
func (producer notImplementedProducer) UnbuiltReason() string { return producer.reason }

// unbuiltProducer is satisfied only by a declared-but-unbuilt producer.
type unbuiltProducer interface{ UnbuiltReason() string }

// Unbuilt names every schedule whose producer is declared but not built, keyed
// by schedule ID and carrying the recorded reason, in stable order.
//
// Missing (above) answers "is any schedule unowned"; this answers the distinct
// and, so far, more dangerous question "is any schedule owned on paper only".
// ScheduleCoverage proves the legacy Beat inventory maps onto this table, but it
// compares names, cadences, zones and policies — never executability — so a
// schedule whose producer fails every invocation passes coverage unchanged. That
// is exactly how three unbuilt producers coexisted with a green coverage test
// while their tickets were closed. Exposing the set makes it assertable.
func (set *ProducerSet) Unbuilt(schedules []Schedule) map[string]string {
	unbuilt := make(map[string]string)
	if set == nil {
		return unbuilt
	}
	for _, schedule := range schedules {
		producer, ok := set.Producer(schedule.ProducerID)
		if !ok {
			continue
		}
		if stub, isStub := producer.(unbuiltProducer); isStub {
			unbuilt[schedule.ID] = stub.UnbuiltReason()
		}
	}
	return unbuilt
}

func (producer notImplementedProducer) Produce(
	context.Context,
	pgx.Tx,
	Schedule,
	Occurrence,
) (Outcome, error) {
	return Outcome{}, fmt.Errorf("%w: %s (%s)", ErrProducerNotImplemented, producer.id, producer.reason)
}
