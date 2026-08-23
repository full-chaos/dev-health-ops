// Package sync provides a dormant shadow of the legacy Python sync scheduler.
// Snapshot remains read-only. The separate transaction kernel can persist a
// coordinator handoff and its schedule marker atomically, but no command starts
// a scheduler loop.
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
	// EvaluationVersion identifies the timing rules. v2 adds the occurrence
	// ledger to the cron base (CHAOS-3936) and is therefore deliberately NOT
	// Python-compatible for a config whose run never completed: Python freezes
	// on last_sync_at there and Go keeps advancing. The version is part of the
	// candidate digest so a cross-runtime comparison cannot silently read a
	// v2 digest as if both runtimes still applied the same rule.
	EvaluationVersion = "sync_scheduler_timing_evaluation_v2"
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
	// DecisionOrgMissing mirrors Python's pre-mint organization guard
	// (workers/sync_scheduler.py:204-205 via workers/org_guard.py:14-36). It is
	// produced by the Coordinator, not by Evaluate: this package's timing kernel
	// deliberately owns no business lookups (see Evaluate's doc comment).
	DecisionOrgMissing Decision = "org_missing"
	// DecisionFeatureDisabled mirrors Python's pre-mint canonical-incident
	// entitlement guard (workers/sync_scheduler.py:207-219). Also a Coordinator
	// decision, for the same reason.
	DecisionFeatureDisabled Decision = "feature_disabled"
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
	// LastOccurrenceAt is the newest scheduled_for already present in this
	// config's occurrence ledger, or nil when the config has never had one
	// minted. It advances when an occurrence is HANDED OFF, so unlike
	// LastSyncAt -- which advances only when a run COMPLETES -- a run that
	// fails, hangs, or is never consumed cannot pin it (CHAOS-3936).
	LastOccurrenceAt *time.Time
	Job              *Job
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
