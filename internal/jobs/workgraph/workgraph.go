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

type Kind string

const (
	KindBuild       Kind = "workgraph.build"
	KindMaterialize Kind = "investment.materialize"
	KindDispatch    Kind = "investment.dispatch"
	KindChunk       Kind = "investment.chunk"
	KindFinalize    Kind = "investment.finalize"
)

func (kind Kind) Valid() bool {
	switch kind {
	case KindBuild, KindMaterialize, KindDispatch, KindChunk, KindFinalize:
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
}

// CompatibilityExecutor is intentionally narrow. It receives the loaded
// authoritative request and fenced claim; it cannot choose a Python callable,
// change LLM controls, or supply source evidence.
type CompatibilityExecutor interface {
	Execute(context.Context, Claim) ([]byte, error)
}
