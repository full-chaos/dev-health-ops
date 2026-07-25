package jobruntime

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"reflect"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/jobcontract"
	"github.com/riverqueue/river"
	"github.com/riverqueue/river/rivertype"
)

// Handler receives a validated, scoped, deadline-bound job context. It never
// receives encoded_args or unvalidated queue metadata.
type Handler[T ContractArgs] interface {
	Work(context.Context, *Execution[T]) error
}

// HandlerFunc adapts a function to Handler.
type HandlerFunc[T ContractArgs] func(context.Context, *Execution[T]) error

func (function HandlerFunc[T]) Work(ctx context.Context, execution *Execution[T]) error {
	return function(ctx, execution)
}

// Execution is the typed handler context. Domain state remains authoritative;
// River state and these arguments are only execution inputs.
type Execution[T ContractArgs] struct {
	JobID          int64
	Attempt        int
	Args           T
	Envelope       jobcontract.Envelope
	CorrelationID  string
	OrganizationID *string
	Deadline       time.Time
	Definition     Descriptor
	Logger         *slog.Logger
}

// ScopeRequest resolves and verifies tenant/domain ownership before secrets or
// provider clients may be attached to context.
type ScopeRequest struct {
	Kind              string
	OrganizationID    *string
	Domain            jobcontract.DomainLink
	OrganizationScope string
}

type TenantScope interface {
	Supports(string) bool
	Resolve(context.Context, ScopeRequest) (context.Context, error)
}

// BudgetRequest contains only registry policy and stable identifiers. Budget
// implementations may attach provider/cost-class logic without exposing it to
// generic runtime code.
type BudgetRequest struct {
	Kind             string
	OrganizationID   *string
	ConcurrencyScope string
	ConcurrencyLimit int
}

type BudgetLease interface {
	Release()
}

type Budget interface {
	Supports(string, int) bool
	Acquire(context.Context, BudgetRequest) (BudgetLease, error)
}

type ClaimState string

const (
	ClaimProceed         ClaimState = "proceed"
	ClaimAlreadyComplete ClaimState = "already_complete"
	ClaimTerminal        ClaimState = "terminal"
)

type ClaimRequest struct {
	Kind           string
	OrganizationID *string
	IdempotencyKey string
	Domain         jobcontract.DomainLink
	Policy         string
	JobID          int64
	Attempt        int
}

type Completion struct {
	Result   Result
	Category ErrorCategory
}

type IdempotencyClaim interface {
	State() ClaimState
	Finish(context.Context, Completion) error
}

type Idempotency interface {
	Supports(string) bool
	Begin(context.Context, ClaimRequest) (IdempotencyClaim, error)
}

type Dependencies struct {
	Logger      *slog.Logger
	Observer    Observer
	TenantScope TenantScope
	Budget      Budget
	Idempotency Idempotency
}

const claimFinalizeTimeout = 5 * time.Second

// Adapter implements river.Worker while keeping all behavior driven by a
// checked-in HandlerSpec.
type Adapter[T ContractArgs] struct {
	descriptor  Descriptor
	handler     Handler[T]
	logger      *slog.Logger
	observer    Observer
	tenantScope TenantScope
	budget      Budget
	idempotency Idempotency
}

func NewAdapter[T ContractArgs](registry *Registry, spec HandlerSpec, handler Handler[T], dependencies Dependencies) (*Adapter[T], error) {
	if registry == nil {
		return nil, errors.New("runtime registry is required")
	}
	if handler == nil || dependencies.Logger == nil || dependencies.Observer == nil ||
		dependencies.TenantScope == nil || dependencies.Budget == nil || dependencies.Idempotency == nil {
		return nil, errors.New("complete handler middleware dependencies are required")
	}
	if err := registry.ValidateHandler(spec); err != nil {
		return nil, err
	}
	var args T
	value := reflect.ValueOf(args)
	if !value.IsValid() || ((value.Kind() == reflect.Pointer || value.Kind() == reflect.Interface) && value.IsNil()) {
		return nil, errors.New("typed job arguments must be a non-pointer value")
	}
	if args.Kind() != spec.Kind {
		return nil, fmt.Errorf("typed args kind %s does not match handler %s", args.Kind(), spec.Kind)
	}
	if !reflect.DeepEqual(args.SupportedContractVersions(), spec.SupportedVersions) {
		return nil, fmt.Errorf("typed args versions drift from handler %s", spec.Kind)
	}
	if !dependencies.TenantScope.Supports(spec.OrganizationScope) {
		return nil, fmt.Errorf("tenant scope does not support handler %s policy", spec.Kind)
	}
	if !dependencies.Budget.Supports(spec.ConcurrencyScope, spec.ConcurrencyLimit) {
		return nil, fmt.Errorf("budget does not support handler %s concurrency policy", spec.Kind)
	}
	if !dependencies.Idempotency.Supports(spec.Idempotency) {
		return nil, fmt.Errorf("idempotency does not support handler %s policy", spec.Kind)
	}
	descriptor, _ := registry.Descriptor(spec.Kind)
	return &Adapter[T]{
		descriptor:  descriptor,
		handler:     handler,
		logger:      dependencies.Logger,
		observer:    dependencies.Observer,
		tenantScope: dependencies.TenantScope,
		budget:      dependencies.Budget,
		idempotency: dependencies.Idempotency,
	}, nil
}

// Spec is a defensive copy suitable for StartupSpec handler coverage.
func (adapter *Adapter[T]) Spec() HandlerSpec {
	spec := adapter.descriptor
	spec.SupportedVersions = append([]int(nil), spec.SupportedVersions...)
	spec.SensitiveFields = append([]string(nil), spec.SensitiveFields...)
	return spec
}

func (adapter *Adapter[T]) Middleware(*rivertype.JobRow) []rivertype.WorkerMiddleware {
	// The adapter owns the ordered typed pipeline so validation can compare raw
	// encoded_args with River's typed decode before a handler runs.
	return nil
}

func (adapter *Adapter[T]) Timeout(*river.Job[T]) time.Duration {
	return adapter.descriptor.Timeout
}

func (adapter *Adapter[T]) NextRetry(job *river.Job[T]) time.Time {
	if adapter.descriptor.RetryPolicy != "bounded_exponential_jitter" || job == nil || job.JobRow == nil {
		return time.Time{}
	}
	attempt := job.Attempt
	if attempt < 1 {
		attempt = 1
	}
	exponent := attempt - 1
	if exponent > 6 {
		exponent = 6
	}
	delay := 5 * time.Second * time.Duration(1<<exponent)
	if delay > 5*time.Minute {
		delay = 5 * time.Minute
	}
	// Stable +/-10% jitter avoids correlated retries without process-global RNG.
	seed := uint64(job.ID)*11400714819323198485 + uint64(attempt)*14029467366897019727
	offsetPermille := int64(seed%201) - 100
	delay += time.Duration(int64(delay) * offsetPermille / 1000)
	base := job.ScheduledAt
	if job.AttemptedAt != nil {
		base = *job.AttemptedAt
	}
	if base.IsZero() {
		base = job.CreatedAt
	}
	return base.Add(delay)
}

func (adapter *Adapter[T]) Work(parent context.Context, job *river.Job[T]) error {
	started := time.Now()
	labels := JobLabels{
		Profile: adapter.descriptor.Profile,
		Queue:   adapter.descriptor.Queue,
		Kind:    adapter.descriptor.Kind,
	}
	observe(func() { adapter.observer.JobStarted(parent, labels) })
	choice := decision{result: ResultCancel, category: CategoryValidation, cancel: true}
	var envelope jobcontract.Envelope

	defer func() {
		observe(func() {
			adapter.observer.JobFinished(parent, labels, choice.result, choice.category, time.Since(started))
		})
	}()

	choice, envelope, err := adapter.execute(parent, job, labels)
	if choice.result == ResultCancel {
		observe(func() { adapter.observer.JobCancelled(parent, labels, choice.category) })
	}
	if err == nil {
		adapter.logFinish(parent, job, envelope, choice, started)
		return nil
	}
	adapter.logFinish(parent, job, envelope, choice, started)
	return transportError(choice)
}

func (adapter *Adapter[T]) execute(parent context.Context, job *river.Job[T], labels JobLabels) (choice decision, envelope jobcontract.Envelope, returned error) {
	choice = decision{result: ResultCancel, category: CategoryValidation, cancel: true}
	var claim IdempotencyClaim
	defer func() {
		if recovered := recover(); recovered != nil {
			attempt := 1
			if job != nil && job.JobRow != nil && job.Attempt > 0 {
				attempt = job.Attempt
			}
			choice = retryDecision(CategoryPanic, attempt, adapter.descriptor.MaxAttempts)
			returned = &safeError{category: CategoryPanic}
			observe(func() { adapter.observer.JobPanicked(parent, labels) })
			if claim != nil && claim.State() == ClaimProceed {
				if err := finishClaim(parent, claim, Completion{Result: choice.result, Category: choice.category}); err != nil {
					choice = retryDecision(CategoryIdempotency, attempt, adapter.descriptor.MaxAttempts)
					returned = &safeError{category: CategoryIdempotency}
				}
			}
		}
	}()

	if job == nil || job.JobRow == nil {
		return choice, envelope, errors.New("missing River job")
	}
	if err := adapter.validateRow(job); err != nil {
		return choice, envelope, err
	}
	decoded, err := jobcontract.Decode(adapter.descriptor.Kind, job.EncodedArgs)
	if err != nil {
		return choice, envelope, err
	}
	typed := job.Args.ContractEnvelope()
	if !reflect.DeepEqual(decoded, typed) {
		return choice, envelope, errors.New("typed River arguments drift from validated contract")
	}
	envelope = decoded
	adapter.logStart(parent, job, envelope)

	ctx, cancel := context.WithTimeout(parent, adapter.descriptor.Timeout)
	defer cancel()
	ctx = context.WithValue(ctx, correlationContextKey{}, envelope.CorrelationID)
	if envelope.OrganizationID != nil {
		ctx = context.WithValue(ctx, organizationContextKey{}, *envelope.OrganizationID)
	}

	scoped, err := adapter.tenantScope.Resolve(ctx, ScopeRequest{
		Kind:              adapter.descriptor.Kind,
		OrganizationID:    envelope.OrganizationID,
		Domain:            envelope.Domain,
		OrganizationScope: adapter.descriptor.OrganizationScope,
	})
	if err != nil {
		var marked *markedError
		if errors.As(err, &marked) {
			choice = classify(ctx, err, job.Attempt, adapter.descriptor.MaxAttempts)
		} else {
			choice = classify(ctx, mark(CategoryTenant, err, false), job.Attempt, adapter.descriptor.MaxAttempts)
		}
		if choice.category == CategoryTenant && choice.cancel {
			observe(func() { adapter.observer.DomainMismatch(ctx, envelope.Domain.Type) })
		}
		return choice, envelope, err
	}
	if scoped == nil {
		return choice, envelope, errors.New("tenant scope returned nil context")
	}
	ctx = scoped

	waitStarted := time.Now()
	lease, err := adapter.budget.Acquire(ctx, BudgetRequest{
		Kind:             adapter.descriptor.Kind,
		OrganizationID:   envelope.OrganizationID,
		ConcurrencyScope: adapter.descriptor.ConcurrencyScope,
		ConcurrencyLimit: adapter.descriptor.ConcurrencyLimit,
	})
	waitResult := "acquired"
	if err != nil {
		waitResult = waitResultForContext(ctx)
	}
	observe(func() { adapter.observer.BudgetWait(ctx, labels, time.Since(waitStarted), waitResult) })
	if err != nil {
		choice = classify(ctx, mark(CategoryBudget, err, false), job.Attempt, adapter.descriptor.MaxAttempts)
		return choice, envelope, err
	}
	if lease == nil {
		return choice, envelope, errors.New("budget returned nil lease")
	}
	defer lease.Release()

	claim, err = adapter.idempotency.Begin(ctx, ClaimRequest{
		Kind:           adapter.descriptor.Kind,
		OrganizationID: envelope.OrganizationID,
		IdempotencyKey: envelope.IdempotencyKey,
		Domain:         envelope.Domain,
		Policy:         adapter.descriptor.Idempotency,
		JobID:          job.ID,
		Attempt:        job.Attempt,
	})
	if err != nil {
		choice = classify(ctx, mark(CategoryIdempotency, err, false), job.Attempt, adapter.descriptor.MaxAttempts)
		return choice, envelope, err
	}
	if claim == nil {
		return choice, envelope, errors.New("idempotency returned nil claim")
	}
	switch claim.State() {
	case ClaimAlreadyComplete:
		return decision{result: ResultDuplicate, category: CategoryNone}, envelope, nil
	case ClaimTerminal:
		choice = decision{result: ResultCancel, category: CategoryTerminalDomain, cancel: true}
		observe(func() { adapter.observer.DomainMismatch(ctx, envelope.Domain.Type) })
		return choice, envelope, &safeError{category: CategoryTerminalDomain}
	case ClaimProceed:
	default:
		return choice, envelope, errors.New("idempotency returned invalid claim state")
	}

	deadline, _ := ctx.Deadline()
	execution := &Execution[T]{
		JobID:          job.ID,
		Attempt:        job.Attempt,
		Args:           job.Args,
		Envelope:       envelope,
		CorrelationID:  envelope.CorrelationID,
		OrganizationID: envelope.OrganizationID,
		Deadline:       deadline,
		Definition:     adapter.Spec(),
		Logger: adapter.logger.With(
			"job_id", job.ID,
			"kind", adapter.descriptor.Kind,
			"correlation_id", envelope.CorrelationID,
			"domain_type", envelope.Domain.Type,
			"domain_id", envelope.Domain.ID,
		),
	}
	handlerErr := adapter.handler.Work(ctx, execution)
	if handlerErr == nil && ctx.Err() != nil {
		handlerErr = ctx.Err()
	}
	choice = classify(ctx, handlerErr, job.Attempt, adapter.descriptor.MaxAttempts)
	if choice.category == CategoryTerminalDomain {
		observe(func() { adapter.observer.DomainMismatch(ctx, envelope.Domain.Type) })
	}
	if err := finishClaim(ctx, claim, Completion{Result: choice.result, Category: choice.category}); err != nil {
		choice = classify(ctx, mark(CategoryIdempotency, err, false), job.Attempt, adapter.descriptor.MaxAttempts)
		return choice, envelope, err
	}
	return choice, envelope, handlerErr
}

func finishClaim(ctx context.Context, claim IdempotencyClaim, completion Completion) error {
	finishContext, cancel := context.WithTimeout(context.WithoutCancel(ctx), claimFinalizeTimeout)
	defer cancel()
	return claim.Finish(finishContext, completion)
}

func (adapter *Adapter[T]) validateRow(job *river.Job[T]) error {
	if !adapter.descriptor.Executable() {
		return fmt.Errorf("handler %s is disabled by migration route", adapter.descriptor.Kind)
	}
	if job.Kind != adapter.descriptor.Kind || job.Queue != adapter.descriptor.Queue ||
		job.Priority != adapter.descriptor.Priority || job.MaxAttempts < adapter.descriptor.MaxAttempts {
		return errors.New("River job execution policy drifts from registry")
	}
	if job.Attempt < 1 || job.Attempt > job.MaxAttempts {
		return errors.New("River job attempt is outside registry bounds")
	}
	return nil
}

func (adapter *Adapter[T]) logStart(ctx context.Context, job *river.Job[T], envelope jobcontract.Envelope) {
	adapter.logger.InfoContext(ctx, "job started",
		"job_id", job.ID,
		"kind", adapter.descriptor.Kind,
		"contract_version", envelope.ContractVersion,
		"profile", adapter.descriptor.Profile,
		"queue", adapter.descriptor.Queue,
		"attempt", job.Attempt,
		"correlation_id", envelope.CorrelationID,
		"domain_type", envelope.Domain.Type,
		"domain_id", envelope.Domain.ID,
	)
}

func (adapter *Adapter[T]) logFinish(ctx context.Context, job *river.Job[T], envelope jobcontract.Envelope, choice decision, started time.Time) {
	attributes := []any{
		"kind", adapter.descriptor.Kind,
		"profile", adapter.descriptor.Profile,
		"queue", adapter.descriptor.Queue,
		"result", choice.result,
		"error_category", choice.category,
		"duration_ms", time.Since(started).Milliseconds(),
	}
	if job != nil && job.JobRow != nil {
		attributes = append(attributes, "job_id", job.ID, "attempt", job.Attempt)
	}
	if envelope.CorrelationID != "" {
		attributes = append(attributes,
			"contract_version", envelope.ContractVersion,
			"correlation_id", envelope.CorrelationID,
			"domain_type", envelope.Domain.Type,
			"domain_id", envelope.Domain.ID,
		)
	}
	adapter.logger.InfoContext(ctx, "job finished", attributes...)
}

func waitResultForContext(ctx context.Context) string {
	if errors.Is(ctx.Err(), context.DeadlineExceeded) {
		return "timeout"
	}
	if errors.Is(ctx.Err(), context.Canceled) {
		return "cancelled"
	}
	return "error"
}

func observe(callback func()) {
	defer func() { _ = recover() }()
	callback()
}

type correlationContextKey struct{}
type organizationContextKey struct{}

func CorrelationID(ctx context.Context) (string, bool) {
	value, ok := ctx.Value(correlationContextKey{}).(string)
	return value, ok
}

func OrganizationID(ctx context.Context) (string, bool) {
	value, ok := ctx.Value(organizationContextKey{}).(string)
	return value, ok
}
