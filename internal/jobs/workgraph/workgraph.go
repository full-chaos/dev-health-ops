// Package workgraph owns the fenced, dormant River boundary for work-graph
// construction and investment materialization. Queue arguments contain only a
// durable request id; all scope, prompt, model, spend, and evidence metadata
// are persisted before the outbox handoff.
package workgraph

import (
	"context"
	"errors"
	"time"
)

var (
	ErrInvalidState = errors.New("work graph execution state is invalid")
	ErrLeaseLost    = errors.New("work graph execution lease was lost")
	ErrLeaseActive  = errors.New("work graph execution lease is still active")
	ErrUnavailable  = errors.New("work graph execution dependency is unavailable")
)

// LeaseActiveError reports that the request is held by a lease that has not
// expired yet, and carries how long is left on it.
//
// A live lease is not the same answer as "already finished", even though the
// claim reached both by matching no row. Reporting it as finished retires the
// job, and that job is the only thing that would have returned to reclaim the
// lease after it expired -- the retry budget is spent in tens of seconds
// against a lease measured in minutes. Since this request's completion is the
// fence key for every handoff gated on it, a request abandoned that way strands
// its whole chain (CHAOS-3991).
type LeaseActiveError struct {
	RetryAfter time.Duration
}

func (err *LeaseActiveError) Error() string { return ErrLeaseActive.Error() }
func (err *LeaseActiveError) Unwrap() error { return ErrLeaseActive }

// Failure classes of a DETERMINISTIC executor failure: one that will recur on
// every retry with the same request and configuration. They are code labels,
// safe to log and to store on the ledger; the wrapped error text is NOT (it can
// carry provider output) and is never logged by the handler.
const (
	ClassLLMDeterministic   = "llm_deterministic"
	ClassScopeInvalid       = "scope_invalid"
	ClassWindowInvalid      = "window_invalid"
	ClassLLMBatchUnsupport  = "llm_batch_mode_unsupported"
	ClassLLMProviderInvalid = "llm_provider_invalid"
	ClassOrgRequired        = "org_required"
	ClassEvidenceEncode     = "evidence_encode"
	ClassKindMismatch       = "executor_kind_mismatch"
)

// DeterministicError marks an executor failure as deterministic. It is the
// ONLY signal that ends a request terminally (state 'failed'): every other
// executor error is treated as retryable, because a terminal state needs a
// deterministic cause (CHAOS-8782, lead ruling D4911).
type DeterministicError struct {
	Class string
	Err   error
}

func (err *DeterministicError) Error() string {
	return "work graph execution failed deterministically (" + err.Class + "): " + err.Err.Error()
}
func (err *DeterministicError) Unwrap() error { return err.Err }

// Deterministic wraps err as a deterministic failure of the given class.
func Deterministic(class string, err error) error {
	if err == nil {
		return nil
	}
	return &DeterministicError{Class: class, Err: err}
}

type Kind string

const (
	KindBuild       Kind = "workgraph.build"
	KindMaterialize Kind = "investment.materialize"
)

func (kind Kind) Valid() bool {
	switch kind {
	case KindBuild, KindMaterialize:
		return true
	default:
		return false
	}
}

// Request is the immutable, authoritative execution intent. Scope is canonical
// JSON and is never supplied by a River job or compatibility HTTP request.
type Request struct {
	ID                        string
	OrganizationID            string
	Kind                      Kind
	Scope                     []byte
	ModelRef                  string
	PromptRef                 string
	LLMConcurrency            int
	SpendLimitMicrounits      int64
	CorrelationID             string
	IdempotencyKey            string
	PrerequisiteCompletionKey string
	// AttemptCount is the request row's own claim counter, as of the Claim that
	// produced this value (it includes that claim). It survives requeues and
	// strand-repair re-arms, so it is the only clock that spans River jobs.
	AttemptCount int
	// Coalesce asks the writer to supersede this producer's own PENDING
	// requests that name the same work -- same organization, same kind, same
	// scope -- instead of queueing alongside them.
	//
	// It is opt-in per producer rather than a property of the kind, because
	// superseding is only safe for work NOTHING ELSE IS WAITING ON. A request
	// whose completion key fences a later handoff must never be cancelled out
	// from under that fence, so a producer that chains its requests leaves
	// this false. It is not persisted: the coalescing group is derived from
	// correlation_id, which every producer already namespaces to itself.
	Coalesce bool
}

type Claim struct {
	Request       Request
	Token         string
	LeaseDuration time.Duration
}

// Store is the durable state-machine boundary. Every state-changing operation
// is fenced by both the current claim token and a live lease.
type Store interface {
	Claim(context.Context, string, Kind) (*Claim, error)
	Renew(context.Context, Claim) error
	Complete(context.Context, Claim, []byte) error
	Fail(context.Context, Claim, string) error
	Ambiguous(context.Context, Claim, string) error
	// Requeue stands a claimed request back to 'pending' so the next attempt
	// can claim it at once instead of waiting out the lease. It is fenced like
	// every other transition (claim token + live lease) and is only for an
	// outcome that is safe to re-run.
	Requeue(context.Context, Claim) error
}

// NativeExecutor is intentionally narrow. It receives the loaded
// authoritative request and fenced claim; it cannot choose a Python callable,
// change LLM controls, or supply source evidence.
type NativeExecutor interface {
	Execute(context.Context, Claim) ([]byte, error)
}
