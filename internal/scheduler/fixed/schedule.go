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

// DueDecision is what a scheduler should do for one schedule at one instant.
type DueDecision struct {
	// Occurrence is the single occurrence to attempt, if any.
	Occurrence *Occurrence
	// ColdStart means this schedule has no durable history. The occurrence is
	// a baseline to record, not work to produce.
	ColdStart bool
	// SkippedStale records that the newest due time was too old to run under
	// this schedule's policy. It is reported rather than silently dropped.
	SkippedStale bool
}

// DueOccurrence decides what to attempt at observedAt given the newest due time
// already recorded in the durable ledger.
//
// At most one occurrence is ever emitted. That is Celery Beat's missed-run
// behavior: after an outage Beat fires one overdue task and resumes, it does
// not replay every boundary it slept through. Emitting the whole missed window
// would turn a scheduler restart into a burst of backdated nightly fan-outs.
//
// lastRecorded is the anchor. Its absence means cold start: a brand-new ledger
// carries none of Beat's history, so the current due time is recorded as a
// baseline and produces no work. Without that, first activation at 01:30 would
// treat both yesterday's and today's 01:00 boundary as owed, and a freshly
// started 300 second schedule would immediately fire the bucket it started
// inside instead of waiting a full period.
//
// The epoch-aligned cadence grid is still what makes an occurrence key
// deterministic across replicas. Anchoring only decides which grid point is
// owed, so replica agreement is preserved.
func DueOccurrence(
	schedule Schedule,
	observedAt time.Time,
	lastRecorded *time.Time,
) (DueDecision, error) {
	if err := schedule.Validate(); err != nil {
		return DueDecision{}, err
	}
	location, err := schedule.Location()
	if err != nil {
		return DueDecision{}, err
	}
	observedAt = observedAt.UTC().Truncate(time.Second)
	scheduledFor, ok := schedule.Cadence.Previous(observedAt, location)
	if !ok {
		return DueDecision{}, fmt.Errorf("%w: %s", ErrInvalidCadence, schedule.ID)
	}

	if lastRecorded == nil {
		occurrence := NewOccurrence(schedule, scheduledFor, observedAt)
		return DueDecision{Occurrence: &occurrence, ColdStart: true}, nil
	}
	anchor := lastRecorded.UTC()
	if !scheduledFor.After(anchor) {
		// The newest boundary is already durably owned.
		return DueDecision{}, nil
	}

	occurrence := NewOccurrence(schedule, scheduledFor, observedAt)
	switch schedule.CatchUp {
	case CatchUpSkip:
		// Telemetry and cumulative retention resume rather than catch up. If
		// more than one boundary was missed the anchor sits behind the boundary
		// before this one, and running now would report a heartbeat for a day
		// the process was absent, or prune under a cutoff the next night
		// supersedes anyway. Re-baselining records where the schedule resumed
		// without claiming work happened.
		priorBoundary, ok := schedule.Cadence.Previous(scheduledFor.Add(-time.Second), location)
		if ok && anchor.Before(priorBoundary) {
			return DueDecision{Occurrence: &occurrence, SkippedStale: true}, nil
		}
	case CatchUpBounded:
		// A safety net must run however late: that is the entire reason it
		// exists. Exactly one occurrence is owed regardless of gap size.
	default:
		return DueDecision{}, fmt.Errorf("%w: %s", ErrInvalidSchedule, schedule.ID)
	}
	return DueDecision{Occurrence: &occurrence}, nil
}

func writeDigestField(hasher interface{ Write([]byte) (int, error) }, name, value string) {
	// Length prefixes make the digest unambiguous: no pair of distinct field
	// values can concatenate into the same byte sequence.
	_, _ = fmt.Fprintf(hasher, "%d:%s=%d:%s;", len(name), name, len(value), value)
}
