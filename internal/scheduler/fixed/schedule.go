package fixed

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"regexp"
	"time"
)

const (
	// OccurrenceIdentityVersion frames every fixed-schedule occurrence key. A
	// change to the derivation inputs requires a new version so an in-flight
	// occurrence cannot be silently re-identified.
	OccurrenceIdentityVersion = "fixed_schedule_occurrence_v1"
)

var (
	// ErrInvalidSchedule identifies a declaration that cannot be operated.
	ErrInvalidSchedule = errors.New("fixed schedule declaration is invalid")

	scheduleIDPattern = regexp.MustCompile(`^[a-z][a-z0-9_]{2,63}$`)
)

// CatchUpPolicy declares what a scheduler does with occurrences whose due time
// passed while no replica was running.
type CatchUpPolicy string

const (
	// CatchUpSkip emits only the newest due occurrence. A safety-net cadence
	// short enough that a missed run is repaired by the next tick uses this.
	CatchUpSkip CatchUpPolicy = "skip"
	// CatchUpBounded emits every occurrence inside the uniqueness window in
	// ascending order. A nightly safety net uses this so an outage that spans
	// its due time still produces the run.
	CatchUpBounded CatchUpPolicy = "bounded_catch_up"
)

// Schedule is the complete checked-in declaration for one fixed maintenance
// schedule, matching TRD section 9.2. Every field is required: a partially
// declared schedule is rejected at construction rather than defaulted, so an
// operator cannot inherit an unreviewed cadence or retry policy.
type Schedule struct {
	// ID is the stable schedule identity. It participates in every occurrence
	// key, so renaming it re-identifies all future occurrences.
	ID string
	// LegacyBeatEntry is the Celery Beat name this schedule replaces. The
	// schedule-coverage test uses it to prove the legacy inventory is fully
	// accounted for.
	LegacyBeatEntry string
	// Cadence and Timezone define the canonical due-time set.
	Cadence  Cadence
	Timezone string
	// CatchUp declares missed-run behavior.
	CatchUp CatchUpPolicy
	// UniquenessWindow is how long one occurrence identity must remain
	// distinguishable. It bounds catch-up enumeration and is the minimum
	// durable retention the occurrence ledger must provide.
	UniquenessWindow time.Duration
	// TargetKind is the registry job kind this schedule produces.
	TargetKind string
	// ProducerID selects the registered argument constructor. The table stays
	// data-only so it can be compared against the legacy inventory without
	// linking executable producer code.
	ProducerID string
	// MaxAttempts is the River attempt budget for produced jobs. Exhausting it
	// is terminal for that occurrence; the next due time is unaffected.
	MaxAttempts int
	// AlertThreshold is how long an expected occurrence may be missing before
	// the runtime reports the schedule as unhealthy.
	AlertThreshold time.Duration
	// Rationale documents why the cadence and policy are what they are.
	Rationale string
}

// Location resolves the declared IANA time zone.
func (schedule Schedule) Location() (*time.Location, error) {
	if schedule.Timezone == "" {
		return nil, fmt.Errorf("%w: timezone is empty", ErrInvalidTimezone)
	}
	location, err := time.LoadLocation(schedule.Timezone)
	if err != nil {
		return nil, fmt.Errorf("%w: %s", ErrInvalidTimezone, schedule.Timezone)
	}
	return location, nil
}

// Validate proves the declaration is internally consistent. It deliberately
// does not consult the job registry: registry agreement is a separate check so
// a declaration table can be unit-tested without loading contracts.
func (schedule Schedule) Validate() error {
	if !scheduleIDPattern.MatchString(schedule.ID) {
		return fmt.Errorf("%w: id %q", ErrInvalidSchedule, schedule.ID)
	}
	if schedule.LegacyBeatEntry == "" {
		return fmt.Errorf("%w: %s has no legacy beat entry", ErrInvalidSchedule, schedule.ID)
	}
	if err := schedule.Cadence.Validate(); err != nil {
		return fmt.Errorf("schedule %s: %w", schedule.ID, err)
	}
	if _, err := schedule.Location(); err != nil {
		return fmt.Errorf("schedule %s: %w", schedule.ID, err)
	}
	switch schedule.CatchUp {
	case CatchUpSkip, CatchUpBounded:
	default:
		return fmt.Errorf("%w: %s has unknown catch-up policy %q", ErrInvalidSchedule, schedule.ID, schedule.CatchUp)
	}
	period := schedule.Cadence.Period()
	if schedule.UniquenessWindow < period {
		return fmt.Errorf(
			"%w: %s uniqueness window %s is shorter than its %s cadence",
			ErrInvalidSchedule, schedule.ID, schedule.UniquenessWindow, period,
		)
	}
	if schedule.UniquenessWindow > 31*24*time.Hour {
		return fmt.Errorf("%w: %s uniqueness window is unbounded", ErrInvalidSchedule, schedule.ID)
	}
	if schedule.TargetKind == "" {
		return fmt.Errorf("%w: %s has no target kind", ErrInvalidSchedule, schedule.ID)
	}
	if schedule.ProducerID == "" {
		return fmt.Errorf("%w: %s has no producer", ErrInvalidSchedule, schedule.ID)
	}
	if schedule.MaxAttempts < 1 || schedule.MaxAttempts > 25 {
		return fmt.Errorf("%w: %s max attempts %d is out of range", ErrInvalidSchedule, schedule.ID, schedule.MaxAttempts)
	}
	if schedule.AlertThreshold <= period {
		return fmt.Errorf(
			"%w: %s alert threshold %s must exceed its %s cadence",
			ErrInvalidSchedule, schedule.ID, schedule.AlertThreshold, period,
		)
	}
	if schedule.AlertThreshold > schedule.UniquenessWindow {
		return fmt.Errorf(
			"%w: %s alert threshold %s exceeds its uniqueness window",
			ErrInvalidSchedule, schedule.ID, schedule.AlertThreshold,
		)
	}
	if schedule.Rationale == "" {
		return fmt.Errorf("%w: %s has no recorded rationale", ErrInvalidSchedule, schedule.ID)
	}
	return nil
}

// Occurrence is one deterministic fixed-schedule due time with its durable
// identity. It carries no business payload: argument construction belongs to
// the registered producer, which runs inside the claiming transaction.
type Occurrence struct {
	Key             string
	IdentityVersion string
	ScheduleID      string
	TargetKind      string
	ScheduledFor    time.Time
	ObservedAt      time.Time
}

// NewOccurrence derives the durable identity for one due time. The digest
// contains the identity version, the schedule identity, and the canonical UTC
// due time and nothing else, so two replicas that observe the same due time
// derive the same key without coordinating.
func NewOccurrence(schedule Schedule, scheduledFor, observedAt time.Time) Occurrence {
	scheduledFor = scheduledFor.UTC().Truncate(time.Second)
	hasher := sha256.New()
	writeDigestField(hasher, "identity_version", OccurrenceIdentityVersion)
	writeDigestField(hasher, "schedule_id", schedule.ID)
	writeDigestField(hasher, "scheduled_for", scheduledFor.Format(time.RFC3339Nano))
	return Occurrence{
		Key:             "sha256:" + hex.EncodeToString(hasher.Sum(nil)),
		IdentityVersion: OccurrenceIdentityVersion,
		ScheduleID:      schedule.ID,
		TargetKind:      schedule.TargetKind,
		ScheduledFor:    scheduledFor,
		ObservedAt:      observedAt.UTC().Truncate(time.Second),
	}
}

// DueOccurrences enumerates the occurrences a scheduler should attempt at
// observedAt under the schedule's catch-up policy. It is a pure function so
// the two-replica and restart behaviors are unit-testable without a database.
//
// An occurrence older than the uniqueness window is never emitted: its
// identity may no longer be distinguishable in the durable ledger, so
// re-emitting it could duplicate a product effect. Such an occurrence is
// reported through Missing, not silently dropped.
func DueOccurrences(schedule Schedule, observedAt time.Time) ([]Occurrence, error) {
	if err := schedule.Validate(); err != nil {
		return nil, err
	}
	location, err := schedule.Location()
	if err != nil {
		return nil, err
	}
	observedAt = observedAt.UTC().Truncate(time.Second)
	horizon := observedAt.Add(-schedule.UniquenessWindow)

	switch schedule.CatchUp {
	case CatchUpSkip:
		scheduledFor, ok := schedule.Cadence.Previous(observedAt, location)
		if !ok || scheduledFor.Before(horizon) {
			return nil, nil
		}
		return []Occurrence{NewOccurrence(schedule, scheduledFor, observedAt)}, nil
	case CatchUpBounded:
		limit := catchUpLimit(schedule)
		dueTimes, _ := schedule.Cadence.Between(horizon, observedAt, location, limit)
		occurrences := make([]Occurrence, 0, len(dueTimes))
		for _, scheduledFor := range dueTimes {
			occurrences = append(occurrences, NewOccurrence(schedule, scheduledFor, observedAt))
		}
		return occurrences, nil
	default:
		return nil, fmt.Errorf("%w: %s", ErrInvalidSchedule, schedule.ID)
	}
}

// catchUpLimit bounds one catch-up enumeration so a long outage cannot emit an
// unbounded burst. The window over the period is the exact number of
// occurrences the uniqueness window can distinguish, plus one for the boundary.
func catchUpLimit(schedule Schedule) int {
	period := schedule.Cadence.Period()
	if period <= 0 {
		return 1
	}
	limit := int(schedule.UniquenessWindow/period) + 1
	if limit > 64 {
		return 64
	}
	return limit
}

func writeDigestField(hasher interface{ Write([]byte) (int, error) }, name, value string) {
	// Length prefixes make the digest unambiguous: no pair of distinct field
	// values can concatenate into the same byte sequence.
	_, _ = fmt.Fprintf(hasher, "%d:%s=%d:%s;", len(name), name, len(value), value)
}
