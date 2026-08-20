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
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	oteltrace "go.opentelemetry.io/otel/trace"
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

type budgetLeaseLoss interface {
	Lost() <-chan struct{}
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
	// Terminal separates the two outcomes that both arrive as ResultCancel:
	// an explicit domain-terminal decision (validation failure, permanent
	// error, ClaimTerminal readback), which must never run again, from a
	// process drain or budget-lease loss, which must stay retryable. Without
	// it the idempotency store stamped every cancellation "terminal" and the
	// River retry was auto-cancelled forever (CHAOS-3865).
	Terminal bool
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
	if job == nil {
		return time.Time{}
	}
	return NextRetryAt(adapter.descriptor, job.JobRow)
}

// NextRetryAt applies the checked-in retry policy without requiring an
// executable handler. River maintenance clients use it when they carry
// type-only workers for kinds executed by another queue.
func NextRetryAt(descriptor Descriptor, job *rivertype.JobRow) time.Time {
	if descriptor.RetryPolicy != "bounded_exponential_jitter" || job == nil {
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
		Queue: adapter.descriptor.Queue,
		Kind:  adapter.descriptor.Kind,
	}
	// ScheduledAt is River's own "available to be worked" timestamp, so the gap
	// to this Work() entry is exactly the availability-to-execution-start wait
	// the TRD requires. A missing or clock-skewed ScheduledAt yields a negative
	// gap, which is dropped rather than observed.
	if job != nil && job.JobRow != nil && !job.JobRow.ScheduledAt.IsZero() {
		if wait := started.Sub(job.JobRow.ScheduledAt); wait >= 0 {
			observe(func() { adapter.observer.JobWait(parent, labels, wait) })
		}
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
	// span is created once the envelope decodes (startJobSpan below); a job
	// that fails validation/decode before that point stays nil here and relies
	// on otelriver's own river.work span for baseline visibility.
	var span oteltrace.Span
	defer func() {
		if recovered := recover(); recovered != nil {
			attempt := 1
			if job != nil && job.JobRow != nil && job.Attempt > 0 {
				attempt = job.Attempt
			}
			choice = retryDecision(CategoryPanic, ReasonHandlerPanic, attempt, adapter.descriptor.MaxAttempts)
			returned = &safeError{category: CategoryPanic, reason: ReasonHandlerPanic}
			observe(func() { adapter.observer.JobPanicked(parent, labels) })
			if claim != nil && claim.State() == ClaimProceed {
				if err := finishClaim(parent, claim, Completion{
					Result: choice.result, Category: choice.category, Terminal: choice.cancel,
				}); err != nil {
					choice = retryDecision(CategoryIdempotency, Reason{}, attempt, adapter.descriptor.MaxAttempts)
					returned = &safeError{category: CategoryIdempotency}
				}
			}
		}
		// Runs on every path -- panic, early return, or normal completion --
		// because named returns are already final by the time this runs last.
		finishJobSpan(span, choice, returned)
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
	parent, span = startJobSpan(parent, adapter.descriptor, job.ID, job.Attempt, envelope)

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
	ctx, cancelLeaseLoss := withBudgetLeaseLoss(ctx, lease)
	defer cancelLeaseLoss()
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
	// A handler that returned success completed its work; a drain or lease
	// loss that lands between that return and this line must not rewrite the
	// outcome. Promoting ctx.Err() here used to turn a succeeded run into a
	// cancellation -- and, before the runStatus fix below, into a permanently
	// terminal one (CHAOS-3865).
	choice = classify(ctx, handlerErr, job.Attempt, adapter.descriptor.MaxAttempts)
	if choice.category == CategoryTerminalDomain {
		observe(func() { adapter.observer.DomainMismatch(ctx, envelope.Domain.Type) })
	}
	if err := finishClaim(ctx, claim, Completion{
		Result: choice.result, Category: choice.category, Terminal: choice.cancel,
	}); err != nil {
		choice = classify(ctx, mark(CategoryIdempotency, err, false), job.Attempt, adapter.descriptor.MaxAttempts)
		return choice, envelope, err
	}
	return choice, envelope, handlerErr
}

func withBudgetLeaseLoss(ctx context.Context, lease BudgetLease) (context.Context, context.CancelFunc) {
	lostLease, ok := lease.(budgetLeaseLoss)
	if !ok || lostLease.Lost() == nil {
		return ctx, func() {}
	}
	leaseContext, cancel := context.WithCancel(ctx)
	go func() {
		select {
		case <-lostLease.Lost():
			cancel()
		case <-leaseContext.Done():
		}
	}()
	return leaseContext, cancel
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

// jobTracerName scopes the span by the package that creates it, the
// OpenTelemetry convention, rather than by any one job kind -- this single
// scope covers every kind Adapter executes. otel.Tracer is looked up fresh on
// every call rather than cached in a package var: otel's global delegation
// only rebinds an already-vended Tracer handle to the first TracerProvider it
// ever sees (go.opentelemetry.io/otel/internal/global's setDelegate runs
// behind a sync.Once), so a package-level Tracer var would stay silently
// bound to whichever provider tracing.Init installed first and ignore any
// later one -- invisible in production (Init runs once and stays), but the
// exact failure mode a naive test of this function hits.
const jobTracerName = "github.com/full-chaos/dev-health-ops/internal/jobruntime"

// startJobSpan opens the one span every Adapter-executed job kind gets,
// parented from envelope.TraceParent when the Python producer captured one at
// enqueue time (CHAOS-3993) so a sync run's outbox-relayed work lands in the
// same trace it was dispatched from. otelriver's own river.work span (wired
// in cmd/dev-health-worker/river_process.go and
// internal/joboutbox/inserter.go) is the parent in context when this runs;
// this span nests under it and carries the attributes otelriver's baseline
// span does not: job id, correlation id, and domain identity.
func startJobSpan(
	ctx context.Context,
	descriptor Descriptor,
	jobID int64,
	attempt int,
	envelope jobcontract.Envelope,
) (context.Context, oteltrace.Span) {
	if envelope.TraceParent != "" {
		ctx = otel.GetTextMapPropagator().Extract(ctx, propagation.MapCarrier{"traceparent": envelope.TraceParent})
	}
	attributes := []attribute.KeyValue{
		attribute.String("dev_health.job.kind", descriptor.Kind),
		attribute.String("dev_health.job.queue", descriptor.Queue),
		attribute.Int64("dev_health.job.id", jobID),
		attribute.Int("dev_health.job.attempt", attempt),
		attribute.String("dev_health.correlation_id", envelope.CorrelationID),
		attribute.String("dev_health.domain.type", envelope.Domain.Type),
		attribute.String("dev_health.domain.id", envelope.Domain.ID),
	}
	if envelope.OrganizationID != nil {
		attributes = append(attributes, attribute.String("dev_health.organization_id", *envelope.OrganizationID))
	}
	return otel.Tracer(jobTracerName).Start(ctx, "dev_health.job.execute", oteltrace.WithAttributes(attributes...))
}

// finishJobSpan is safe to call with a nil span: a job that fails validation
// or decode before startJobSpan runs never gets one.
func finishJobSpan(span oteltrace.Span, choice decision, err error) {
	if span == nil {
		return
	}
	span.SetAttributes(
		attribute.String("dev_health.job.result", string(choice.result)),
		attribute.String("dev_health.job.error_category", string(choice.category)),
	)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
	} else {
		span.SetStatus(codes.Ok, "")
	}
	span.End()
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
