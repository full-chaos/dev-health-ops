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
	CategoryIdempotency    ErrorCategory = "idempotency"
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
}

func (err *markedError) Error() string { return "job error category: " + string(err.category) }
func (err *markedError) Unwrap() error { return err.cause }

// Retryable marks an expected transient handler failure.
func Retryable(err error) error { return mark(CategoryRetryable, err, false) }

// BudgetContention marks a short-lived shared-request reservation collision.
// River snoozes do not count as attempts, so this is distinct from Retryable:
// healthy sibling work cannot consume a job's bounded failure budget.
func BudgetContention(err error, delay time.Duration) error {
	if delay <= 0 {
		delay = time.Second
	}
	return &markedError{category: CategoryBudget, cause: nonNilError(err), snooze: delay}
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
}

func classify(ctx context.Context, err error, attempt, maxAttempts int) decision {
	if err == nil {
		return decision{result: ResultSuccess, category: CategoryNone}
	}
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(ctx.Err(), context.DeadlineExceeded) {
		return retryDecision(CategoryTimeout, attempt, maxAttempts)
	}
	if errors.Is(err, context.Canceled) || errors.Is(ctx.Err(), context.Canceled) {
		// Do not wrap this in river.JobCancel. A remote River cancellation already
		// marks the row, while a process drain must leave it retryable.
		return decision{result: ResultCancel, category: CategoryCancelled}
	}
	var marked *markedError
	if errors.As(err, &marked) {
		if marked.snooze > 0 {
			return decision{result: ResultRetry, category: marked.category, snooze: marked.snooze}
		}
		if marked.cancel {
			return decision{result: ResultCancel, category: marked.category, cancel: true}
		}
		return retryDecision(marked.category, attempt, maxAttempts)
	}
	// Unclassified handler errors fail closed. Retrying requires an explicit
	// Retryable wrapper so deterministic failures cannot create retry storms.
	return decision{result: ResultCancel, category: CategoryPermanent, cancel: true}
}

func retryDecision(category ErrorCategory, attempt, maxAttempts int) decision {
	if attempt >= maxAttempts {
		return decision{result: ResultDiscard, category: category}
	}
	return decision{result: ResultRetry, category: category}
}

type safeError struct{ category ErrorCategory }

func (err *safeError) Error() string {
	return fmt.Sprintf("dev-health job failed [%s]", err.category)
}

func transportError(choice decision) error {
	if choice.snooze > 0 {
		return river.JobSnooze(choice.snooze)
	}
	safe := &safeError{category: choice.category}
	if choice.cancel {
		return river.JobCancel(safe)
	}
	return safe
}
