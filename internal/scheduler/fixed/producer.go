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

func (producer notImplementedProducer) Produce(
	context.Context,
	pgx.Tx,
	Schedule,
	Occurrence,
) (Outcome, error) {
	return Outcome{}, fmt.Errorf("%w: %s (%s)", ErrProducerNotImplemented, producer.id, producer.reason)
}
