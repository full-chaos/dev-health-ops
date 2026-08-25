package jobruntime

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/riverqueue/river"
)

// ErrorCategory is a bounded value safe for logs, metrics, River error rows,
// and operator responses. Original error text never crosses this boundary.
type ErrorCategory string

const (
	CategoryNone           ErrorCategory = "none"
	CategoryValidation     ErrorCategory = "validation"
	CategoryPanic          ErrorCategory = "panic"
	CategoryTimeout        ErrorCategory = "timeout"
	CategoryCancelled      ErrorCategory = "cancelled"
	CategoryRetryable      ErrorCategory = "retryable"
	CategoryPermanent      ErrorCategory = "permanent"
	CategoryTerminalDomain ErrorCategory = "terminal_domain"
	CategoryTenant         ErrorCategory = "tenant_scope"
	CategoryBudget         ErrorCategory = "budget"
	CategoryRateLimited    ErrorCategory = "rate_limit"
	CategoryIdempotency    ErrorCategory = "idempotency"
)

// Reason is a bounded, compile-time-fixed elaboration of an ErrorCategory.
// It crosses the same boundary ErrorCategory does -- logs, metrics, River
// error rows, operator responses -- and is safe for the same reason: its
// entire value space is the finite set of package-level constants declared
// below, never a runtime string.
//
// The field is unexported and this package exports no function that builds
// a Reason from a string. A call site outside this package cannot construct
// one at all (only the zero value, via an unkeyed Reason{} literal, which
// isZero treats as "no reason supplied"); a call site inside this package
// cannot smuggle a runtime value past a Reason-typed parameter, because
// doing so requires literally writing Reason{value: someExpr} in this file
// next to -- and as conspicuous as -- the constants it would be duplicating.
// That is the enforcement: passing err.Error(), a tenant ID, or a formatted
// message anywhere a Reason is expected is a compile error, not a review
// nit. See TestReasonConstructorRejectsRuntimeValue for the compiled proof.
type Reason struct{ value string }

func (reason Reason) isZero() bool { return reason == Reason{} }

// String renders the bounded reason text. It is exported so operator-facing
// formatting outside this package (if any) can read the value without
// reaching into the safeError string; it can never return anything other
// than one of the constants below.
func (reason Reason) String() string { return reason.value }

// reason is the package's only constructor. It is unexported, so nothing
// outside jobruntime can call it, and every call site inside the package is
// one of the "var Reason... = reason(...)" declarations immediately below --
// there is no call to reason() anywhere else in the package.
func reason(value string) Reason { return Reason{value: value} }

// The fixed catalog. Adding a new bounded reason means adding a line here,
// in code review, next to this comment -- the same bar Category constants
// already clear.
var (
	// ReasonHandlerPanic marks the safeError built by Adapter.execute's
	// recover path: the sole site in this package that turns a panic into a
	// CategoryPanic result. It never carries the recovered value or a stack
	// trace -- only the fact that this is where the panic was caught.
	ReasonHandlerPanic = reason("handler_panic_recovered")
	// ReasonInvalidState marks a Permanent failure caused by a deterministic
	// durable-state precondition -- a malformed/missing scope, a domain
	// mismatch, a required field a caller never supplied. It never varies
	// with the attempt count: the same input produces the same failure
	// every time, which is exactly why it is Permanent rather than
	// Retryable (CHAOS-4242 -- a native-executor precondition failure
	// misclassified as Retryable spent a job's whole attempt budget on
	// three identical failures before discarding).
	ReasonInvalidState = reason("invalid_state")
)

// Result is the runtime decision. A discard is represented by a normal safe
// error on the final River attempt; River performs the durable state change.
type Result string

const (
	ResultSuccess   Result = "success"
	ResultDuplicate Result = "duplicate"
	ResultRetry     Result = "retry"
	ResultDiscard   Result = "discard"
	ResultCancel    Result = "cancel"
)

type markedError struct {
	category ErrorCategory
	cause    error
	cancel   bool
	snooze   time.Duration
	reason   Reason
}

func (err *markedError) Error() string { return "job error category: " + string(err.category) }
func (err *markedError) Unwrap() error { return err.cause }

// safeCauseProvider is implemented by an error that itself declares its
// message is safe to appear in Adapter's WARN "job failed" log line
// (CHAOS-4242) -- never in ErrorCategory/Reason/River rows, which stay
// governed exactly as before.
type safeCauseProvider interface{ SafeLogCause() string }

// WithSafeCause marks err's message as safe for that WARN log line. It is
// opt-in and narrow on purpose: a handler error can legitimately carry
// upstream response bodies, tokens, or other caller-supplied content the
// runtime has no way to vet, so nothing is logged unless a call site says
// so explicitly (see logCause in adapter.go, and
// TestAdapterMiddlewareOutcomesAreSafeAndDeterministic, which plants fake
// secrets in ordinary handler errors and asserts they never reach a log).
//
// Use this only for errors built from static format strings plus
// caller-controlled-but-non-secret identifiers -- partition/run/job IDs,
// scope field names, day strings. Never wrap anything that might embed
// upstream response content, a connection string, or a credential.
func WithSafeCause(err error) error {
	if err == nil {
		return nil
	}
	return &safeCauseError{err}
}

// safeCauseError wraps err, promoting its Error() method unchanged (via the
// embedded interface) while adding SafeLogCause() so Adapter's logCause can
// find it by walking the error's Unwrap() chain.
type safeCauseError struct{ error }

func (wrapped *safeCauseError) SafeLogCause() string { return wrapped.error.Error() }
func (wrapped *safeCauseError) Unwrap() error        { return wrapped.error }

// Retryable marks an expected transient handler failure.
func Retryable(err error) error { return mark(CategoryRetryable, err, false) }

// RetryableAfter marks transient contention that must be retried after a
// known delay without consuming the job's bounded failure attempts.
func RetryableAfter(err error, delay time.Duration) error {
	if delay <= 0 {
		delay = time.Second
	}
	return &markedError{category: CategoryRetryable, cause: nonNilError(err), snooze: delay}
}

// BudgetContention marks a short-lived shared-request reservation collision.
// River snoozes do not count as attempts, so this is distinct from Retryable:
// healthy sibling work cannot consume a job's bounded failure budget.
func BudgetContention(err error, delay time.Duration) error {
	if delay <= 0 {
		delay = time.Second
	}
	return &markedError{category: CategoryBudget, cause: nonNilError(err), snooze: delay}
}

// RateLimited marks provider-signalled rate limiting. Like BudgetContention it
// snoozes rather than failing, because a 429 is the provider scheduling us, not
// the job going wrong: burning one of the unit's bounded attempts on a 30-60
// minute GitHub reset window exhausted all of them in minutes and terminalized
// the unit (CHAOS-3868). It carries its own category so rate-limited work is
// distinguishable from shared-bucket contention in metrics and logs.
func RateLimited(err error, delay time.Duration) error {
	if delay <= 0 {
		delay = time.Second
	}
	return &markedError{category: CategoryRateLimited, cause: nonNilError(err), snooze: delay}
}

// SnoozeDelay exposes the typed decision to domain handlers and tests without
// exposing the internal runtime error representation.
func SnoozeDelay(err error) (time.Duration, bool) {
	var marked *markedError
	if !errors.As(err, &marked) || marked.snooze <= 0 {
		return 0, false
	}
	return marked.snooze, true
}

// Permanent marks an invalid request or deterministic handler failure that
// must not be retried.
func Permanent(err error) error { return mark(CategoryPermanent, err, true) }

// TerminalDomain marks a domain-state precondition that makes work obsolete.
func TerminalDomain(err error) error { return mark(CategoryTerminalDomain, err, true) }

// Cancel marks an explicit domain-requested cancellation.
func Cancel(err error) error { return mark(CategoryCancelled, err, true) }

// DomainMismatch marks a tenant/domain link mismatch and is terminal.
func DomainMismatch(err error) error { return mark(CategoryTenant, err, true) }

func mark(category ErrorCategory, err error, cancel bool) error {
	return &markedError{category: category, cause: nonNilError(err), cancel: cancel}
}

// WithReason attaches a bounded Reason to an error already produced by one of
// this package's marking functions (Retryable, RetryableAfter, Permanent,
// TerminalDomain, Cancel, DomainMismatch, BudgetContention, RateLimited).
// Applied to anything else -- including a nil or unmarked error -- it is a
// no-op, so an unclassified handler error still fails closed exactly as
// before: a reason is opt-in, never a substitute for classification.
//
// Because its second parameter is the unexported-field Reason type, a caller
// can only pass one of this package's exported Reason constants; there is no
// way to pass err.Error(), a tenant ID, or any other runtime string.
func WithReason(err error, why Reason) error {
	var marked *markedError
	if !errors.As(err, &marked) {
		return err
	}
	updated := *marked
	updated.reason = why
	return &updated
}

func nonNilError(err error) error {
	if err == nil {
		return errors.New("unspecified")
	}
	return err
}

type decision struct {
	result   Result
	category ErrorCategory
	cancel   bool
	snooze   time.Duration
	reason   Reason
}

func classify(ctx context.Context, err error, attempt, maxAttempts int) decision {
	if err == nil {
		return decision{result: ResultSuccess, category: CategoryNone}
	}
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(ctx.Err(), context.DeadlineExceeded) {
		return retryDecision(CategoryTimeout, Reason{}, attempt, maxAttempts)
	}
	if errors.Is(err, context.Canceled) || errors.Is(ctx.Err(), context.Canceled) {
		// Do not wrap this in river.JobCancel. A remote River cancellation already
		// marks the row, while a process drain must leave it retryable.
		return decision{result: ResultCancel, category: CategoryCancelled}
	}
	var marked *markedError
	if errors.As(err, &marked) {
		if marked.snooze > 0 {
			return decision{result: ResultRetry, category: marked.category, snooze: marked.snooze, reason: marked.reason}
		}
		if marked.cancel {
			return decision{result: ResultCancel, category: marked.category, cancel: true, reason: marked.reason}
		}
		return retryDecision(marked.category, marked.reason, attempt, maxAttempts)
	}
	// Unclassified handler errors fail closed. Retrying requires an explicit
	// Retryable wrapper so deterministic failures cannot create retry storms.
	// No marker means no reason: this path never sets one.
	return decision{result: ResultCancel, category: CategoryPermanent, cancel: true}
}

// classifyBudgetWait classifies a budget.Acquire failure -- a wait for fleet
// or organization capacity that failed before the handler ever ran.
//
// Unlike classify, "is this the final attempt" is answered by River's own
// job.MaxAttempts (the ceiling that actually governs whether River retries),
// never adapter.descriptor.MaxAttempts (the contract's own domain-attempt
// ceiling): no domain attempt has been made yet, so there is nothing to
// charge against it. validateRow only requires job.MaxAttempts to be AT
// LEAST descriptor.MaxAttempts, so a caller may legitimately give River more
// retry headroom against infrastructure backpressure than the contract's own
// ceiling allows (CHAOS-4235: TestMultiReplicaFleetSurvivesDatabaseOutage
// does this deliberately for system.heartbeat's insert). Using the contract
// ceiling here mislabeled that still-retryable-at-River wait as
// ResultDiscard; passing River's own ceiling keeps the label accurate in
// BOTH directions -- it also correctly reports ResultDiscard for the common
// production case where the two ceilings match (system.heartbeat's real
// insert path sets MaxAttempts=1 for the same reason its contract does: a
// retried heartbeat post would report the same day twice; see
// internal/scheduler/fixed/inventory.go), where the wait genuinely IS
// exhausting River's last attempt.
//
// Neither branch ever sets cancel=true, matching classify's own behavior for
// this category: River's executor, not this decision, is what actually
// enforces job.MaxAttempts. `jobRow.Attempt >= jobRow.MaxAttempts` runs
// unconditionally on any returned non-cancel error
// (internal/jobexecutor/job_executor.go's reportError), so this function's
// Result has never controlled -- and still does not control -- whether a
// job whose budget wait keeps failing is bounded. It only controls what
// gets reported while that River-owned bound is reached.
func classifyBudgetWait(ctx context.Context, err error, attempt, riverMaxAttempts int) decision {
	if errors.Is(err, context.Canceled) || errors.Is(ctx.Err(), context.Canceled) {
		// Mirrors classify: a remote River cancellation already marks the
		// row, while a process drain must leave the job retryable.
		return decision{result: ResultCancel, category: CategoryCancelled}
	}
	category := CategoryBudget
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(ctx.Err(), context.DeadlineExceeded) {
		category = CategoryTimeout
	}
	if attempt >= riverMaxAttempts {
		return decision{result: ResultDiscard, category: category}
	}
	return decision{result: ResultRetry, category: category}
}

func retryDecision(category ErrorCategory, why Reason, attempt, maxAttempts int) decision {
	if attempt >= maxAttempts {
		return decision{result: ResultDiscard, category: category, reason: why}
	}
	return decision{result: ResultRetry, category: category, reason: why}
}

type safeError struct {
	category ErrorCategory
	reason   Reason
}

func (err *safeError) Error() string {
	if err.reason.isZero() {
		return fmt.Sprintf("dev-health job failed [%s]", err.category)
	}
	return fmt.Sprintf("dev-health job failed [%s: %s]", err.category, err.reason)
}

func transportError(choice decision) error {
	if choice.snooze > 0 {
		return river.JobSnooze(choice.snooze)
	}
	safe := &safeError{category: choice.category, reason: choice.reason}
	if choice.cancel {
		return river.JobCancel(safe)
	}
	return safe
}
