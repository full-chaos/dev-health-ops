// Package sync provides a dormant, read-only shadow of the legacy Python
// sync scheduler. It evaluates candidates only; it never claims, writes, or
// starts a scheduler loop.
package sync

import (
	"errors"
	"fmt"
	"time"
)

const (
	// TimingDigestVersion identifies the fixed schedule/marker-only candidate
	// framing for later cross-runtime comparison.
	TimingDigestVersion = "sync_scheduler_timing_digest_v1"
	// EvaluationVersion identifies the Python-compatible timing rules.
	EvaluationVersion = "sync_scheduler_timing_evaluation_v1"
	// CronGrammarVersion identifies the deterministic five-field Croniter
	// subset. Random R expressions and optional sixth/seventh fields are
	// explicitly outside this grammar.
	CronGrammarVersion = "croniter_five_field_deterministic_v1"
	// ScheduleMarkerEvaluationScope makes explicit that organization existence,
	// feature entitlement, and all other dispatch gates are out of scope.
	ScheduleMarkerEvaluationScope = "schedule_and_marker_only"

	activeJobStatus = 0
	staleRunningTTL = 2 * time.Hour
)

type RunningMarkerState string

const (
	RunningNotSet RunningMarkerState = "not_running"
	RunningFresh  RunningMarkerState = "fresh"
	RunningStale  RunningMarkerState = "stale"
)

type Decision string

const (
	DecisionScheduleDue     Decision = "schedule_due"
	DecisionInactive        Decision = "inactive"
	DecisionManual          Decision = "manual"
	DecisionInactiveJob     Decision = "inactive_job"
	DecisionFreshRunning    Decision = "fresh_running"
	DecisionNotDue          Decision = "not_due"
	DecisionNextRunGate     Decision = "next_run_gate"
	DecisionInvalidCron     Decision = "invalid_cron"
	DecisionUnsupportedCron Decision = "unsupported_cron"
)

// ErrUnsupportedCron identifies syntax intentionally routed outside the
// versioned deterministic subset. It does not assert that Croniter would
// otherwise accept the expression.
var ErrUnsupportedCron = errors.New("cron syntax is unsupported for deterministic comparison")

// ErrUnsupportedRandomCron identifies Croniter's random R syntax. Recreating
// Croniter selects a new value, so cross-runtime shadow comparison cannot
// evaluate this syntax deterministically.
var ErrUnsupportedRandomCron = fmt.Errorf("%w: random R expression", ErrUnsupportedCron)

// Candidate is the minimal scheduler state read from the legacy semantic
// tables. It contains no execution handle and is safe to evaluate repeatedly.
type Candidate struct {
	ConfigID     string
	Active       bool
	ScheduleCron string
	ScheduleTZ   string
	LastSyncAt   *time.Time
	CreatedAt    time.Time
	Job          *Job
}

type Job struct {
	ID           string
	ScheduleCron string
	Timezone     string
	Status       int
	IsRunning    bool
	LastRunAt    *time.Time
	UpdatedAt    *time.Time
	NextRunAt    *time.Time
}

// Evaluation records occurrence due-ness separately from schedule/marker
// timing eligibility. TimingEligible never means dispatch eligible: the
// dormant shadow intentionally omits organization and feature-service gates.
type Evaluation struct {
	ConfigID           string
	Base               time.Time
	NextOccurrence     *time.Time
	ObservedAt         time.Time
	Due                bool
	TimingEligible     bool
	Decision           Decision
	RunningMarker      RunningMarkerState
	Timezone           string
	TimezoneFallback   bool
	EligibilityScope   string
	CronGrammarVersion string
}

type EvaluatedCandidate struct {
	Candidate  Candidate
	Evaluation Evaluation
}

// Snapshot is a bounded, deterministically ordered read model. Candidate IDs
// remain in-memory comparison material and are represented only by the digest
// in any later telemetry surface.
type Snapshot struct {
	ObservedAt         time.Time
	Limit              int
	Truncated          bool
	Candidates         []EvaluatedCandidate
	DigestVersion      string
	EvaluationVersion  string
	EligibilityScope   string
	CronGrammarVersion string
	CandidateDigest    string
}
